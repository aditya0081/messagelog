package client

import (
	"context"
	"math"

	"messagelog/internal/protocol"
)

// CreateTopic creates a topic with the given number of partitions.
func (c *Conn) CreateTopic(ctx context.Context, name string, partitions int) error {
	if err := checkWireLen("topic", name); err != nil {
		return err
	}
	req := &protocol.CreateTopicRequest{Name: name, Partitions: int32(min(partitions, math.MaxInt32))}
	return c.Do(ctx, protocol.APICreateTopic, req, nil)
}

// Metadata lists the brokers and the given topics (all topics if none are
// given). Unknown topics are left out of the response.
func (c *Conn) Metadata(ctx context.Context, topics ...string) (*protocol.MetadataResponse, error) {
	for _, t := range topics {
		if err := checkWireLen("topic", t); err != nil {
			return nil, err
		}
	}
	var resp protocol.MetadataResponse
	if err := c.Do(ctx, protocol.APIMetadata, &protocol.MetadataRequest{Topics: topics}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
