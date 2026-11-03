package metadata

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"messagelog/internal/fsutil"
)

var _ Store = (*FileStore)(nil)

// FileStore keeps metadata in memory and persists it to one JSON file.
//
// Every mutation builds the next state, writes it durably, and only then
// swaps it in. A failed write therefore leaves both memory and disk at the
// previous state, and an id handed out is never handed out again after a
// crash.
type FileStore struct {
	path     string
	brokerID int32

	mu    sync.Mutex
	state state
}

// state is the persisted document.
type state struct {
	Topics          map[string]Topic `json:"topics"`
	NextTopicID     int64            `json:"nextTopicId"`
	NextPartitionID int64            `json:"nextPartitionId"`
	NextProducerID  int64            `json:"nextProducerId"`
}

func emptyState() state {
	return state{
		Topics:          map[string]Topic{},
		NextTopicID:     1,
		NextPartitionID: 1,
		NextProducerID:  1,
	}
}

// OpenFileStore loads (or creates) a JSON file; every change is written
// tmp + fsync + rename. brokerID becomes LeaderID of new partitions.
func OpenFileStore(path string, brokerID int32) (*FileStore, error) {
	if err := fsutil.MkdirAll(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}
	st, err := load(path)
	if err != nil {
		return nil, err
	}
	return &FileStore{path: path, brokerID: brokerID, state: st}, nil
}

// load reads the state file, or returns an empty state if there is none yet.
func load(path string) (state, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return emptyState(), nil
	}
	if err != nil {
		return state{}, fmt.Errorf("metadata: read %s: %w", path, err)
	}
	st := emptyState()
	if err := json.Unmarshal(data, &st); err != nil {
		return state{}, fmt.Errorf("metadata: parse %s: %w", path, err)
	}
	if st.Topics == nil {
		st.Topics = map[string]Topic{}
	}
	return st, nil
}

// CreateTopic registers a topic with numPartitions partitions, all led by
// this broker. Partition ids are allocated from a global counter. Names are
// unique ignoring case (see ErrTopicExists).
func (s *FileStore) CreateTopic(name string, numPartitions int) (Topic, error) {
	if err := ValidateTopicName(name); err != nil {
		return Topic{}, err
	}
	if numPartitions < 1 || numPartitions > MaxPartitions {
		return Topic{}, fmt.Errorf("%w: got %d, want 1..%d", ErrInvalidPartitionCount, numPartitions, MaxPartitions)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for existing := range s.state.Topics {
		if strings.EqualFold(existing, name) {
			return Topic{}, fmt.Errorf("%w: %q (as %q)", ErrTopicExists, name, existing)
		}
	}

	next := s.state
	next.Topics = maps.Clone(s.state.Topics)
	topic := Topic{ID: next.NextTopicID, Name: name}
	next.NextTopicID++
	for n := range numPartitions {
		topic.Partitions = append(topic.Partitions, Partition{
			ID:       next.NextPartitionID,
			TopicID:  topic.ID,
			Number:   int32(n),
			LeaderID: s.brokerID,
		})
		next.NextPartitionID++
	}
	next.Topics[name] = topic

	if err := s.save(next); err != nil {
		return Topic{}, err
	}
	s.state = next
	return cloneTopic(topic), nil
}

// Topic returns a copy of the named topic.
func (s *FileStore) Topic(name string) (Topic, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.state.Topics[name]
	return cloneTopic(t), ok
}

// Topics returns copies of all topics in creation order.
func (s *FileStore) Topics() []Topic {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Topic, 0, len(s.state.Topics))
	for _, t := range s.state.Topics {
		out = append(out, cloneTopic(t))
	}
	slices.SortFunc(out, func(a, b Topic) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// NextProducerID allocates a producer id. It is persisted before it is
// returned, so ids keep increasing across restarts and are never reused.
func (s *FileStore) NextProducerID() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.state // Topics is shared but not modified.
	id := next.NextProducerID
	next.NextProducerID++
	if err := s.save(next); err != nil {
		return 0, err
	}
	s.state = next
	return id, nil
}

// Close releases the store. Every change is already on disk, so there is
// nothing to flush.
func (s *FileStore) Close() error { return nil }

// save writes st durably and atomically (fsutil.WriteFileAtomic).
func (s *FileStore) save(st state) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("metadata: encode: %w", err)
	}
	if err := fsutil.WriteFileAtomic(s.path, data); err != nil {
		return fmt.Errorf("metadata: save: %w", err)
	}
	return nil
}

func cloneTopic(t Topic) Topic {
	t.Partitions = slices.Clone(t.Partitions)
	return t
}
