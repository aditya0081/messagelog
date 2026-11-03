package metadata

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

const testBroker int32 = 1

func openStore(t *testing.T, path string) *FileStore {
	t.Helper()
	s, err := OpenFileStore(path, testBroker)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustCreate(t *testing.T, s Store, name string, n int) Topic {
	t.Helper()
	topic, err := s.CreateTopic(name, n)
	if err != nil {
		t.Fatalf("CreateTopic(%q, %d): %v", name, n, err)
	}
	return topic
}

func TestCreateGetList(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "meta.json"))

	orders := mustCreate(t, s, "orders", 3)
	if orders.Name != "orders" || len(orders.Partitions) != 3 {
		t.Fatalf("created %+v", orders)
	}
	for i, p := range orders.Partitions {
		if p.Number != int32(i) || p.TopicID != orders.ID || p.LeaderID != testBroker {
			t.Fatalf("partition %d = %+v", i, p)
		}
	}
	mustCreate(t, s, "payments", 1)

	got, ok := s.Topic("orders")
	if !ok || got.ID != orders.ID || len(got.Partitions) != 3 {
		t.Fatalf("Topic(orders) = %+v, %v", got, ok)
	}
	if _, ok := s.Topic("missing"); ok {
		t.Fatal("Topic(missing) found")
	}

	all := s.Topics()
	if len(all) != 2 || all[0].Name != "orders" || all[1].Name != "payments" {
		t.Fatalf("Topics() = %+v", all)
	}
}

func TestReturnedTopicsAreCopies(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "meta.json"))
	mustCreate(t, s, "orders", 1)

	got, _ := s.Topic("orders")
	got.Partitions[0].LeaderID = 99
	s.Topics()[0].Partitions[0].LeaderID = 99

	again, _ := s.Topic("orders")
	if again.Partitions[0].LeaderID != testBroker {
		t.Fatal("caller mutated store state")
	}
}

func TestCreateRejects(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "meta.json"))
	mustCreate(t, s, "orders", 1)

	tests := []struct {
		name  string
		topic string
		parts int
		want  error
	}{
		{"duplicate", "orders", 1, ErrTopicExists},
		// Would share the directory orders-0 on a case-insensitive filesystem.
		{"duplicate ignoring case", "Orders", 1, ErrTopicExists},
		{"empty", "", 1, ErrInvalidTopic},
		{"too long", strings.Repeat("a", MaxTopicNameLen+1), 1, ErrInvalidTopic},
		{"reserved prefix", "__offsets", 1, ErrInvalidTopic},
		{"slash", "a/b", 1, ErrInvalidTopic},
		{"space", "a b", 1, ErrInvalidTopic},
		{"zero partitions", "fresh", 0, ErrInvalidPartitionCount},
		{"too many partitions", "fresh", MaxPartitions + 1, ErrInvalidPartitionCount},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.CreateTopic(tc.topic, tc.parts); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
	if n := len(s.Topics()); n != 1 {
		t.Fatalf("rejected creates left %d topics, want 1", n)
	}
}

func TestValidNames(t *testing.T) {
	for _, name := range []string{"a", "Orders.v2", "user_events-1", "_single", strings.Repeat("x", MaxTopicNameLen)} {
		if err := ValidateTopicName(name); err != nil {
			t.Errorf("ValidateTopicName(%q) = %v", name, err)
		}
	}
}

func TestPartitionIDsGloballyUnique(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "meta.json"))
	seen := map[int64]bool{}
	for _, name := range []string{"a", "b", "c"} {
		for _, p := range mustCreate(t, s, name, 4).Partitions {
			if seen[p.ID] {
				t.Fatalf("partition id %d reused", p.ID)
			}
			seen[p.ID] = true
		}
	}
}

func TestReopenPersistsState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.json")

	first, err := OpenFileStore(path, testBroker)
	if err != nil {
		t.Fatal(err)
	}
	orders := mustCreate(t, first, "orders", 2)
	lastProducer := nextProducer(t, first)
	if lastProducer != 1 {
		t.Fatalf("first producer id = %d, want 1", lastProducer)
	}
	lastProducer = nextProducer(t, first)
	first.Close()

	second := openStore(t, path)
	got, ok := second.Topic("orders")
	if !ok || got.ID != orders.ID || len(got.Partitions) != 2 || got.Partitions[1] != orders.Partitions[1] {
		t.Fatalf("after reopen Topic(orders) = %+v, %v; want %+v", got, ok, orders)
	}
	if id := nextProducer(t, second); id <= lastProducer {
		t.Fatalf("producer id %d after reopen, want > %d", id, lastProducer)
	}

	payments := mustCreate(t, second, "payments", 1)
	if payments.ID <= orders.ID || payments.Partitions[0].ID <= orders.Partitions[1].ID {
		t.Fatalf("ids reused after reopen: %+v vs %+v", payments, orders)
	}
}

func nextProducer(t *testing.T, s Store) int64 {
	t.Helper()
	id, err := s.NextProducerID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
