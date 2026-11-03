package client

import (
	"context"
	"fmt"
	"math"
	"time"

	"messagelog/internal/protocol"
	"messagelog/internal/record"
)

// Consumer reads partitions and manages consumer-group offsets. It is safe
// for concurrent use. A failed call returns its error; the next call
// reconnects if the connection broke.
type Consumer struct {
	conn *redialer
}

// NewConsumer connects to the broker at addr.
func NewConsumer(ctx context.Context, addr string) (*Consumer, error) {
	c := &Consumer{conn: &redialer{addr: addr}}
	if _, err := c.conn.get(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Consume reads up to limit records from offset. If offset is the high
// watermark (nothing new yet), the broker waits up to maxWait for data and
// otherwise returns no records. It also returns the high watermark, the
// offset the next new record will get.
//
// ctx must outlive maxWait, or the call gives up before the broker answers.
func (c *Consumer) Consume(ctx context.Context, topic string, partition int32, offset int64, limit int, maxWait time.Duration) ([]record.Record, int64, error) {
	if err := checkWireLen("topic", topic); err != nil {
		return nil, 0, err
	}
	req := &protocol.ConsumeRequest{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Limit:     int32(min(limit, math.MaxInt32)),
		MaxWaitMs: int32(min(maxWait.Milliseconds(), math.MaxInt32)),
	}
	var resp protocol.ConsumeResponse
	if err := c.conn.do(ctx, protocol.APIConsume, req, &resp); err != nil {
		return nil, 0, err
	}
	records, err := record.DecodeAll(resp.Records)
	if err != nil {
		return nil, 0, fmt.Errorf("client: decode records: %w", err)
	}
	return records, resp.HighWatermark, nil
}

// Commit stores offset as group's progress on the partition. By convention
// it is the next offset to read, not the last one processed.
func (c *Consumer) Commit(ctx context.Context, group, topic string, partition int32, offset int64) error {
	if err := checkGroupAndTopic(group, topic); err != nil {
		return err
	}
	req := &protocol.CommitRequest{Group: group, Topic: topic, Partition: partition, Offset: offset}
	return c.conn.do(ctx, protocol.APICommit, req, nil)
}

// CommittedOffset returns group's last committed offset; found is false if
// it never committed on this partition.
func (c *Consumer) CommittedOffset(ctx context.Context, group, topic string, partition int32) (offset int64, found bool, err error) {
	if err := checkGroupAndTopic(group, topic); err != nil {
		return 0, false, err
	}
	req := &protocol.GetCommittedOffsetRequest{Group: group, Topic: topic, Partition: partition}
	var resp protocol.GetCommittedOffsetResponse
	if err := c.conn.do(ctx, protocol.APIGetCommittedOffset, req, &resp); err != nil {
		return 0, false, err
	}
	return resp.Offset, resp.Found, nil
}

// Close closes the connection.
func (c *Consumer) Close() error { return c.conn.close() }

func checkGroupAndTopic(group, topic string) error {
	if err := checkWireLen("group", group); err != nil {
		return err
	}
	return checkWireLen("topic", topic)
}

// GroupConsumer reads one partition on behalf of a consumer group. It
// starts where the group last committed (offset 0 if it never did), so a
// restarted consumer resumes instead of starting over.
type GroupConsumer struct {
	consumer  *Consumer
	group     string
	topic     string
	partition int32
	position  int64 // next offset to read

	Limit   int           // max records per Poll; default 100
	MaxWait time.Duration // how long Poll waits for new data; default 1s
}

// Group returns a GroupConsumer positioned at group's committed offset.
func (c *Consumer) Group(ctx context.Context, group, topic string, partition int32) (*GroupConsumer, error) {
	offset, _, err := c.CommittedOffset(ctx, group, topic, partition)
	if err != nil {
		return nil, err
	}
	return &GroupConsumer{
		consumer:  c,
		group:     group,
		topic:     topic,
		partition: partition,
		position:  offset, // 0 when nothing was committed
		Limit:     100,
		MaxWait:   time.Second,
	}, nil
}

// Poll returns the next records and moves past them. It returns no records
// if none arrive within MaxWait.
//
// If retention has deleted the records at Position, Poll fails with
// ErrOffsetOutOfRange (the message names the offsets still on disk) and
// keeps failing until the caller picks a new position with SetPosition.
func (g *GroupConsumer) Poll(ctx context.Context) ([]record.Record, error) {
	records, _, err := g.consumer.Consume(ctx, g.topic, g.partition, g.position, g.Limit, g.MaxWait)
	if err != nil {
		return nil, err
	}
	if len(records) > 0 {
		g.position = records[len(records)-1].Offset + 1
	}
	return records, nil
}

// Commit stores the current position as the group's committed offset.
func (g *GroupConsumer) Commit(ctx context.Context) error {
	return g.consumer.Commit(ctx, g.group, g.topic, g.partition, g.position)
}

// Position is the next offset Poll will read.
func (g *GroupConsumer) Position() int64 { return g.position }

// SetPosition moves the position to offset, for example past records that
// retention deleted. It is not committed until Commit.
func (g *GroupConsumer) SetPosition(offset int64) { g.position = offset }
