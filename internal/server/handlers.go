package server

import (
	"context"
	"errors"
	"fmt"

	"messagelog/internal/broker"
	"messagelog/internal/metadata"
	"messagelog/internal/offsets"
	"messagelog/internal/partition"
	"messagelog/internal/protocol"
)

// errInvalidRequest marks requests that decode fine but make no sense.
var errInvalidRequest = errors.New("invalid request")

// dispatch runs a request with a plain response body: everything except
// Publish and Consume, which have their own paths in conn.
func (s *Server) dispatch(req protocol.Request) (protocol.Body, error) {
	switch req.APIKey {
	case protocol.APICreateTopic:
		return s.createTopic(req.Body)
	case protocol.APIMetadata:
		return s.metadata(req.Body)
	case protocol.APIInitProducer:
		return s.initProducer()
	case protocol.APICommit:
		return s.commit(req.Body)
	case protocol.APIGetCommittedOffset:
		return s.committedOffset(req.Body)
	default:
		return nil, fmt.Errorf("%w: unknown api key %d", errInvalidRequest, req.APIKey)
	}
}

func (s *Server) createTopic(raw []byte) (protocol.Body, error) {
	var req protocol.CreateTopicRequest
	if err := req.DecodeFrom(raw); err != nil {
		return nil, err
	}
	_, err := s.broker.CreateTopic(req.Name, int(req.Partitions))
	return nil, err
}

func (s *Server) metadata(raw []byte) (protocol.Body, error) {
	var req protocol.MetadataRequest
	if err := req.DecodeFrom(raw); err != nil {
		return nil, err
	}
	brokers, topics := s.broker.Metadata(req.Topics)
	resp := &protocol.MetadataResponse{}
	for _, b := range brokers {
		resp.Brokers = append(resp.Brokers, protocol.BrokerInfo{ID: b.ID, Addr: b.Addr})
	}
	for _, t := range topics {
		info := protocol.TopicInfo{Name: t.Name}
		for _, p := range t.Partitions {
			info.Partitions = append(info.Partitions, protocol.PartitionInfo{Number: p.Number, LeaderID: p.LeaderID})
		}
		resp.Topics = append(resp.Topics, info)
	}
	return resp, nil
}

func (s *Server) initProducer() (protocol.Body, error) {
	id, err := s.broker.InitProducer()
	if err != nil {
		return nil, err
	}
	return &protocol.InitProducerResponse{ProducerID: id}, nil
}

func (s *Server) commit(raw []byte) (protocol.Body, error) {
	var req protocol.CommitRequest
	if err := req.DecodeFrom(raw); err != nil {
		return nil, err
	}
	return nil, s.broker.Commit(req.Group, req.Topic, req.Partition, req.Offset)
}

func (s *Server) committedOffset(raw []byte) (protocol.Body, error) {
	var req protocol.GetCommittedOffsetRequest
	if err := req.DecodeFrom(raw); err != nil {
		return nil, err
	}
	offset, found, err := s.broker.CommittedOffset(req.Group, req.Topic, req.Partition)
	if err != nil {
		return nil, err
	}
	return &protocol.GetCommittedOffsetResponse{Offset: offset, Found: found}, nil
}

// errorCode maps an error from any layer to its wire code. Anything not
// listed is ErrUnknown, which the conn logs.
func errorCode(err error) protocol.ErrorCode {
	switch {
	case err == nil:
		return protocol.ErrNone
	case errors.Is(err, broker.ErrUnknownTopicOrPartition):
		return protocol.ErrUnknownTopicOrPartition
	case errors.Is(err, partition.ErrOffsetOutOfRange):
		return protocol.ErrOffsetOutOfRange
	case errors.Is(err, partition.ErrOutOfOrderSeq):
		return protocol.ErrOutOfOrderSequence
	case errors.Is(err, partition.ErrMessageTooLarge):
		return protocol.ErrMessageTooLarge
	case errors.Is(err, metadata.ErrTopicExists):
		return protocol.ErrTopicExists
	case errors.Is(err, metadata.ErrInvalidTopic):
		return protocol.ErrInvalidTopic
	case errors.Is(err, protocol.ErrMalformed),
		errors.Is(err, errInvalidRequest),
		errors.Is(err, metadata.ErrInvalidPartitionCount),
		errors.Is(err, offsets.ErrNameTooLong):
		return protocol.ErrInvalidRequest
	case errors.Is(err, broker.ErrClosed),
		errors.Is(err, partition.ErrClosed),
		errors.Is(err, offsets.ErrClosed),
		errors.Is(err, context.Canceled): // the connection is closing
		return protocol.ErrShuttingDown
	default:
		return protocol.ErrUnknown
	}
}
