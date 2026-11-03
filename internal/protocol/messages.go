package protocol

// Request and response bodies, one struct per API. Field order on the wire
// is the field order in the struct. APIs with an empty body (CreateTopic and
// Commit responses, the InitProducer request) have no type: pass a nil Body.

// CreateTopicRequest creates a topic with the given number of partitions.
type CreateTopicRequest struct {
	Name       string
	Partitions int32
}

func (m *CreateTopicRequest) AppendTo(b []byte) []byte {
	b = appendString(b, m.Name)
	return appendInt32(b, m.Partitions)
}

func (m *CreateTopicRequest) DecodeFrom(b []byte) error {
	r := reader{buf: b}
	m.Name = r.string()
	m.Partitions = r.int32()
	return r.finish()
}

// MetadataRequest asks for brokers and topics. An empty Topics means all topics.
type MetadataRequest struct {
	Topics []string
}

func (m *MetadataRequest) AppendTo(b []byte) []byte {
	return appendSlice(b, m.Topics, appendString)
}

func (m *MetadataRequest) DecodeFrom(b []byte) error {
	r := reader{buf: b}
	m.Topics = readSlice(&r, 2, (*reader).string)
	return r.finish()
}

// MetadataResponse lists the cluster's brokers and the requested topics.
type MetadataResponse struct {
	Brokers []BrokerInfo
	Topics  []TopicInfo
}

// BrokerInfo is where a client can reach a broker.
type BrokerInfo struct {
	ID   int32
	Addr string
}

// TopicInfo describes one topic's partitions.
type TopicInfo struct {
	Name       string
	Partitions []PartitionInfo
}

// PartitionInfo says which broker leads a partition.
type PartitionInfo struct {
	Number   int32
	LeaderID int32
}

// Smallest encodings, used to bound counts by the bytes left.
const (
	minBrokerInfoSize    = 4 + 2
	minTopicInfoSize     = 2 + 4
	minPartitionInfoSize = 4 + 4
)

func (m *MetadataResponse) AppendTo(b []byte) []byte {
	b = appendSlice(b, m.Brokers, appendBrokerInfo)
	return appendSlice(b, m.Topics, appendTopicInfo)
}

func (m *MetadataResponse) DecodeFrom(b []byte) error {
	r := reader{buf: b}
	m.Brokers = readSlice(&r, minBrokerInfoSize, readBrokerInfo)
	m.Topics = readSlice(&r, minTopicInfoSize, readTopicInfo)
	return r.finish()
}

func appendBrokerInfo(b []byte, bi BrokerInfo) []byte {
	b = appendInt32(b, bi.ID)
	return appendString(b, bi.Addr)
}

func readBrokerInfo(r *reader) BrokerInfo {
	return BrokerInfo{ID: r.int32(), Addr: r.string()}
}

func appendTopicInfo(b []byte, t TopicInfo) []byte {
	b = appendString(b, t.Name)
	return appendSlice(b, t.Partitions, appendPartitionInfo)
}

func readTopicInfo(r *reader) TopicInfo {
	return TopicInfo{Name: r.string(), Partitions: readSlice(r, minPartitionInfoSize, readPartitionInfo)}
}

func appendPartitionInfo(b []byte, p PartitionInfo) []byte {
	b = appendInt32(b, p.Number)
	return appendInt32(b, p.LeaderID)
}

func readPartitionInfo(r *reader) PartitionInfo {
	return PartitionInfo{Number: r.int32(), LeaderID: r.int32()}
}

// InitProducerResponse carries a new idempotent producer id.
type InitProducerResponse struct {
	ProducerID int64
}

func (m *InitProducerResponse) AppendTo(b []byte) []byte {
	return appendInt64(b, m.ProducerID)
}

func (m *InitProducerResponse) DecodeFrom(b []byte) error {
	r := reader{buf: b}
	m.ProducerID = r.int64()
	return r.finish()
}

// PublishRequest appends one message to a partition. The client picks the
// partition, because the sequence number is per (producer, partition).
type PublishRequest struct {
	Topic      string
	Partition  int32
	ProducerID int64
	Sequence   int64
	Value      []byte
}

func (m *PublishRequest) AppendTo(b []byte) []byte {
	b = appendString(b, m.Topic)
	b = appendInt32(b, m.Partition)
	b = appendInt64(b, m.ProducerID)
	b = appendInt64(b, m.Sequence)
	return appendBytes(b, m.Value)
}

func (m *PublishRequest) DecodeFrom(b []byte) error {
	r := reader{buf: b}
	m.Topic = r.string()
	m.Partition = r.int32()
	m.ProducerID = r.int64()
	m.Sequence = r.int64()
	m.Value = r.bytes()
	return r.finish()
}

// PublishResponse is the ack. Duplicate means the broker had already written
// this sequence and is re-acking the original offset (-1 if no longer known).
type PublishResponse struct {
	Offset    int64
	Duplicate bool
}

func (m *PublishResponse) AppendTo(b []byte) []byte {
	b = appendInt64(b, m.Offset)
	return appendBool(b, m.Duplicate)
}

func (m *PublishResponse) DecodeFrom(b []byte) error {
	r := reader{buf: b}
	m.Offset = r.int64()
	m.Duplicate = r.bool()
	return r.finish()
}

// ConsumeRequest reads up to Limit records starting at Offset, waiting up to
// MaxWaitMs for data if none is available yet (long-poll).
type ConsumeRequest struct {
	Topic     string
	Partition int32
	Offset    int64
	Limit     int32
	MaxWaitMs int32
}

func (m *ConsumeRequest) AppendTo(b []byte) []byte {
	b = appendString(b, m.Topic)
	b = appendInt32(b, m.Partition)
	b = appendInt64(b, m.Offset)
	b = appendInt32(b, m.Limit)
	return appendInt32(b, m.MaxWaitMs)
}

func (m *ConsumeRequest) DecodeFrom(b []byte) error {
	r := reader{buf: b}
	m.Topic = r.string()
	m.Partition = r.int32()
	m.Offset = r.int64()
	m.Limit = r.int32()
	m.MaxWaitMs = r.int32()
	return r.finish()
}

// ConsumeResponse carries raw back-to-back records (see package record).
// The broker builds it with ConsumeResponseHeader and streams Records from
// the segment file; this type is for clients and tests.
type ConsumeResponse struct {
	HighWatermark int64
	Records       []byte
}

// AppendTo must stay in step with ConsumeResponseHeader.
func (m *ConsumeResponse) AppendTo(b []byte) []byte {
	b = appendInt64(b, m.HighWatermark)
	return appendBytes(b, m.Records)
}

func (m *ConsumeResponse) DecodeFrom(b []byte) error {
	r := reader{buf: b}
	m.HighWatermark = r.int64()
	m.Records = r.bytes()
	return r.finish()
}

// CommitRequest stores a consumer group's progress on a partition.
type CommitRequest struct {
	Group     string
	Topic     string
	Partition int32
	Offset    int64
}

func (m *CommitRequest) AppendTo(b []byte) []byte {
	b = appendString(b, m.Group)
	b = appendString(b, m.Topic)
	b = appendInt32(b, m.Partition)
	return appendInt64(b, m.Offset)
}

func (m *CommitRequest) DecodeFrom(b []byte) error {
	r := reader{buf: b}
	m.Group = r.string()
	m.Topic = r.string()
	m.Partition = r.int32()
	m.Offset = r.int64()
	return r.finish()
}

// GetCommittedOffsetRequest asks for a group's last committed offset.
type GetCommittedOffsetRequest struct {
	Group     string
	Topic     string
	Partition int32
}

func (m *GetCommittedOffsetRequest) AppendTo(b []byte) []byte {
	b = appendString(b, m.Group)
	b = appendString(b, m.Topic)
	return appendInt32(b, m.Partition)
}

func (m *GetCommittedOffsetRequest) DecodeFrom(b []byte) error {
	r := reader{buf: b}
	m.Group = r.string()
	m.Topic = r.string()
	m.Partition = r.int32()
	return r.finish()
}

// GetCommittedOffsetResponse returns the offset; Found is false if the group
// never committed on this partition.
type GetCommittedOffsetResponse struct {
	Offset int64
	Found  bool
}

func (m *GetCommittedOffsetResponse) AppendTo(b []byte) []byte {
	b = appendInt64(b, m.Offset)
	return appendBool(b, m.Found)
}

func (m *GetCommittedOffsetResponse) DecodeFrom(b []byte) error {
	r := reader{buf: b}
	m.Offset = r.int64()
	m.Found = r.bool()
	return r.finish()
}
