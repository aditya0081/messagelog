package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"messagelog/internal/record"
)

const maxFrame = 1 << 20

// bodies has one filled-in value of every body type, plus a fresh value of
// the same type to decode into.
func bodies() []struct {
	name     string
	in, into Body
} {
	return []struct {
		name     string
		in, into Body
	}{
		{"CreateTopicRequest", &CreateTopicRequest{Name: "orders", Partitions: 3}, &CreateTopicRequest{}},
		{"MetadataRequest", &MetadataRequest{Topics: []string{"orders", "payments"}}, &MetadataRequest{}},
		{"MetadataRequest all", &MetadataRequest{}, &MetadataRequest{}},
		{"MetadataResponse", &MetadataResponse{
			Brokers: []BrokerInfo{{ID: 1, Addr: "localhost:9092"}},
			Topics: []TopicInfo{
				{Name: "orders", Partitions: []PartitionInfo{{Number: 0, LeaderID: 1}, {Number: 1, LeaderID: 1}}},
				{Name: "empty"},
			},
		}, &MetadataResponse{}},
		{"InitProducerResponse", &InitProducerResponse{ProducerID: 42}, &InitProducerResponse{}},
		{"PublishRequest", &PublishRequest{Topic: "orders", Partition: 0, ProducerID: 7, Sequence: 9, Value: []byte("hello")}, &PublishRequest{}},
		{"PublishResponse", &PublishResponse{Offset: 123, Duplicate: true}, &PublishResponse{}},
		{"ConsumeRequest", &ConsumeRequest{Topic: "orders", Partition: 0, Offset: 5, Limit: 100, MaxWaitMs: 500}, &ConsumeRequest{}},
		{"ConsumeResponse", &ConsumeResponse{HighWatermark: 10, Records: []byte{1, 2, 3}}, &ConsumeResponse{}},
		{"CommitRequest", &CommitRequest{Group: "billing", Topic: "orders", Partition: 0, Offset: 77}, &CommitRequest{}},
		{"GetCommittedOffsetRequest", &GetCommittedOffsetRequest{Group: "billing", Topic: "orders", Partition: 2}, &GetCommittedOffsetRequest{}},
		{"GetCommittedOffsetResponse", &GetCommittedOffsetResponse{Offset: 77, Found: true}, &GetCommittedOffsetResponse{}},
	}
}

func TestBodyRoundTrip(t *testing.T) {
	for _, tc := range bodies() {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.into.DecodeFrom(tc.in.AppendTo(nil)); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tc.in, tc.into) {
				t.Fatalf("got %+v, want %+v", tc.into, tc.in)
			}
		})
	}
}

func TestBodyRejectsShortAndTrailing(t *testing.T) {
	for _, tc := range bodies() {
		t.Run(tc.name, func(t *testing.T) {
			enc := tc.in.AppendTo(nil)
			if err := tc.into.DecodeFrom(enc[:len(enc)-1]); !errors.Is(err, ErrMalformed) {
				t.Errorf("short: err = %v, want ErrMalformed", err)
			}
			if err := tc.into.DecodeFrom(append(enc, 0)); !errors.Is(err, ErrMalformed) {
				t.Errorf("trailing: err = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestDecodeRejectsBadLengths(t *testing.T) {
	tests := []struct {
		name string
		body Body
		buf  []byte
	}{
		{"string length past end", &CreateTopicRequest{}, []byte{0xff, 0xff, 'a'}},
		{"bytes length past end", &ConsumeResponse{}, append(make([]byte, 8), 0xff, 0xff, 0xff, 0xff)},
		{"huge count", &MetadataRequest{}, []byte{0x7f, 0xff, 0xff, 0xff}},
		{"negative count", &MetadataRequest{}, []byte{0xff, 0xff, 0xff, 0xff}},
		{"bool not 0 or 1", &PublishResponse{}, append(make([]byte, 8), 2)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.body.DecodeFrom(tc.buf); !errors.Is(err, ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestRequestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	pub := &PublishRequest{Topic: "orders", ProducerID: 1, Sequence: 2, Value: []byte("v")}
	if err := WriteRequest(&buf, APIPublish, 99, pub); err != nil {
		t.Fatal(err)
	}
	if err := WriteRequest(&buf, APIInitProducer, 100, nil); err != nil {
		t.Fatal(err)
	}

	req := readRequest(t, &buf)
	if req.APIKey != APIPublish || req.CorrelationID != 99 {
		t.Fatalf("header = %+v", req)
	}
	var got PublishRequest
	if err := got.DecodeFrom(req.Body); err != nil || !reflect.DeepEqual(&got, pub) {
		t.Fatalf("body = %+v, %v; want %+v", got, err, pub)
	}

	req = readRequest(t, &buf)
	if req.APIKey != APIInitProducer || req.CorrelationID != 100 || req.Body != nil {
		t.Fatalf("empty-body request = %+v", req)
	}
	if _, err := ReadFrame(&buf, maxFrame); err != io.EOF {
		t.Fatalf("after last frame err = %v, want io.EOF", err)
	}
}

func readRequest(t *testing.T, r io.Reader) Request {
	t.Helper()
	frame, err := ReadFrame(r, maxFrame)
	if err != nil {
		t.Fatal(err)
	}
	req, err := ParseRequest(frame)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestResponseFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	ack := &PublishResponse{Offset: 5}
	if err := WriteResponse(&buf, 1, ErrNone, "", ack); err != nil {
		t.Fatal(err)
	}
	// The body is dropped for an error response.
	if err := WriteResponse(&buf, 2, ErrUnknownTopicOrPartition, "no such topic", ack); err != nil {
		t.Fatal(err)
	}

	resp := readResponse(t, &buf)
	var got PublishResponse
	if resp.CorrelationID != 1 || resp.Error != ErrNone || got.DecodeFrom(resp.Body) != nil || got != *ack {
		t.Fatalf("ok response = %+v, body %+v", resp, got)
	}

	resp = readResponse(t, &buf)
	want := Response{CorrelationID: 2, Error: ErrUnknownTopicOrPartition, Message: "no such topic"}
	if !reflect.DeepEqual(resp, want) {
		t.Fatalf("error response = %+v, want %+v", resp, want)
	}
}

func readResponse(t *testing.T, r io.Reader) Response {
	t.Helper()
	frame, err := ReadFrame(r, maxFrame)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ParseResponse(frame)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestConsumeResponseHeaderMatchesWriteResponse(t *testing.T) {
	var records []byte
	for i := range 3 {
		records = record.Append(records, record.Record{Offset: int64(i), Body: []byte("msg")})
	}

	// What the broker does: header, then records streamed from the segment.
	var streamed bytes.Buffer
	streamed.Write(ConsumeResponseHeader(7, 3, uint32(len(records))))
	streamed.Write(records)

	var written bytes.Buffer
	want := &ConsumeResponse{HighWatermark: 3, Records: records}
	if err := WriteResponse(&written, 7, ErrNone, "", want); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(streamed.Bytes(), written.Bytes()) {
		t.Fatalf("streamed frame differs from WriteResponse:\n%x\n%x", streamed.Bytes(), written.Bytes())
	}

	resp := readResponse(t, &streamed)
	var got ConsumeResponse
	if err := got.DecodeFrom(resp.Body); err != nil {
		t.Fatal(err)
	}
	if resp.CorrelationID != 7 || !reflect.DeepEqual(&got, want) {
		t.Fatalf("got %+v (corr %d), want %+v", got, resp.CorrelationID, want)
	}
	if decoded, err := record.DecodeAll(got.Records); err != nil || len(decoded) != 3 {
		t.Fatalf("records: %d, %v", len(decoded), err)
	}
}

func TestReadFrameRejects(t *testing.T) {
	sizePrefix := func(n uint32) []byte { return binary.BigEndian.AppendUint32(nil, n) }

	if _, err := ReadFrame(bytes.NewReader(sizePrefix(maxFrame+1)), maxFrame); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("oversized: err = %v, want ErrFrameTooLarge", err)
	}
	if _, err := ReadFrame(bytes.NewReader([]byte{0, 0}), maxFrame); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("partial size: err = %v, want io.ErrUnexpectedEOF", err)
	}
	truncated := append(sizePrefix(10), 1, 2, 3)
	if _, err := ReadFrame(bytes.NewReader(truncated), maxFrame); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("partial payload: err = %v, want io.ErrUnexpectedEOF", err)
	}
	sizeOnly := sizePrefix(10)
	if _, err := ReadFrame(bytes.NewReader(sizeOnly), maxFrame); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("missing payload: err = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestParseRejects(t *testing.T) {
	if _, err := ParseRequest([]byte{0, 4, 0, 0}); !errors.Is(err, ErrMalformed) {
		t.Errorf("short request header: err = %v", err)
	}
	if _, err := ParseResponse([]byte{0, 0, 0, 1, 0}); !errors.Is(err, ErrMalformed) {
		t.Errorf("short response header: err = %v", err)
	}
	errWithBody := []byte{0, 0, 0, 1, 0, byte(ErrUnknown), 0, 0, 0xaa}
	if _, err := ParseResponse(errWithBody); !errors.Is(err, ErrMalformed) {
		t.Errorf("error response with body: err = %v", err)
	}
}

func TestLongErrorMessageIsCut(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteResponse(&buf, 1, ErrUnknown, strings.Repeat("x", MaxStringLen+10), nil); err != nil {
		t.Fatal(err)
	}
	if resp := readResponse(t, &buf); len(resp.Message) != MaxStringLen {
		t.Fatalf("message length = %d, want %d", len(resp.Message), MaxStringLen)
	}
}

func TestErrorCodeString(t *testing.T) {
	for c := ErrNone; c <= ErrInvalidTopic; c++ {
		if s := c.String(); s == "" || strings.HasPrefix(s, "ErrorCode(") {
			t.Errorf("code %d has no name", c)
		}
	}
	if got := ErrorCode(999).String(); got != "ErrorCode(999)" {
		t.Errorf("unknown code = %q", got)
	}
}
