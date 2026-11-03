package record

import (
	"bytes"
	"errors"
	"testing"
)

func sample() Record {
	return Record{Offset: 42, Timestamp: 1_700_000_000_000, ProducerID: 7, Sequence: 3, Body: []byte("hello")}
}

func TestRoundTrip(t *testing.T) {
	want := sample()
	buf := Append(nil, want)
	if len(buf) != want.Size() {
		t.Fatalf("size = %d, want %d", len(buf), want.Size())
	}

	got, n, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Fatalf("consumed %d, want %d", n, len(buf))
	}
	if got.Offset != want.Offset || got.Timestamp != want.Timestamp ||
		got.ProducerID != want.ProducerID || got.Sequence != want.Sequence ||
		!bytes.Equal(got.Body, want.Body) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestDecodeDetectsDamage(t *testing.T) {
	good := Append(nil, sample())

	tests := []struct {
		name string
		buf  []byte
		want error
	}{
		{"header cut short", good[:HeaderSize-1], ErrShort},
		{"body cut short", good[:len(good)-1], ErrShort},
		{"flipped body byte", flip(good, len(good)-1), ErrCorrupt},
		{"flipped offset byte", flip(good, 3), ErrCorrupt},
		{"huge length", withLength(good, MaxBodySize+1), ErrTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := Decode(tc.buf); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecodeAll(t *testing.T) {
	var buf []byte
	for i := range 3 {
		r := sample()
		r.Offset = int64(i)
		buf = Append(buf, r)
	}
	recs, err := DecodeAll(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 || recs[2].Offset != 2 {
		t.Fatalf("got %d records, last offset %d", len(recs), recs[len(recs)-1].Offset)
	}
}

func flip(b []byte, i int) []byte {
	c := bytes.Clone(b)
	c[i] ^= 0xFF
	return c
}

func withLength(b []byte, n uint32) []byte {
	c := bytes.Clone(b)
	c[16], c[17], c[18], c[19] = byte(n>>24), byte(n>>16), byte(n>>8), byte(n)
	return c
}
