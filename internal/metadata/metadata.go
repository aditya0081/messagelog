// Package metadata stores the cluster's topics, partitions and producer ids.
//
// The data is small and changes rarely, so the whole state lives in memory and
// is rewritten as one JSON file on every change. Store is an interface because
// a multi-broker cluster will need the same data agreed through consensus;
// FileStore is the single-broker implementation.
package metadata

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrTopicExists means CreateTopic was called with a name already in
	// use, ignoring case: "Orders" and "orders" would share a partition
	// directory on a case-insensitive filesystem.
	ErrTopicExists = errors.New("metadata: topic already exists")
	// ErrInvalidTopic means a name breaks the rules in ValidateTopicName:
	// [a-zA-Z0-9._-], 1..200 chars, not starting with "__".
	ErrInvalidTopic = errors.New("metadata: invalid topic name")
	// ErrInvalidPartitionCount means CreateTopic was asked for a partition
	// count outside 1..MaxPartitions.
	ErrInvalidPartitionCount = errors.New("metadata: invalid partition count")
)

// MaxPartitions bounds the partitions of one topic. The count comes from the
// wire, and each partition costs open files and goroutines.
const MaxPartitions = 1024

// MaxTopicNameLen bounds topic names. Names become directory names
// (<topic>-<partition>), so they must stay well under filesystem limits.
const MaxTopicNameLen = 200

// Broker identifies one broker in the cluster.
type Broker struct {
	ID   int32
	Addr string
}

// Partition is one ordered log of a topic.
type Partition struct {
	ID       int64 // globally unique, never reused
	TopicID  int64
	Number   int32 // position within its topic, 0..n-1
	LeaderID int32 // broker id; always this broker for now
}

// Topic is a named stream split into partitions.
type Topic struct {
	ID         int64
	Name       string
	Partitions []Partition
}

// Store is the metadata interface. Today a file; later backed by consensus.
type Store interface {
	CreateTopic(name string, numPartitions int) (Topic, error)
	Topic(name string) (Topic, bool)
	Topics() []Topic
	NextProducerID() (int64, error) // persisted before returning
	Close() error
}

// ValidateTopicName reports whether name is a legal topic name.
// Names starting with "__" are reserved for internal topics.
func ValidateTopicName(name string) error {
	if len(name) == 0 || len(name) > MaxTopicNameLen {
		return fmt.Errorf("%w: length %d not in 1..%d", ErrInvalidTopic, len(name), MaxTopicNameLen)
	}
	if strings.HasPrefix(name, "__") {
		return fmt.Errorf("%w: %q uses the reserved \"__\" prefix", ErrInvalidTopic, name)
	}
	for _, c := range name {
		if !isTopicChar(c) {
			return fmt.Errorf("%w: %q contains %q", ErrInvalidTopic, name, c)
		}
	}
	return nil
}

func isTopicChar(c rune) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
		c == '.' || c == '_' || c == '-'
}
