package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"messagelog/internal/partition"
	"messagelog/internal/record"
)

func openBroker(t *testing.T, autoCreate bool) *Broker {
	t.Helper()
	b, err := Open(Config{DataDir: t.TempDir(), BrokerID: 1, AutoCreateTopics: autoCreate})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func publishSync(b *Broker, topic string, partitionNum int32, body string) partition.AppendResult {
	result := make(chan partition.AppendResult, 1)
	req := partition.AppendRequest{ProducerID: record.NoProducerID, Body: []byte(body)}
	b.Publish(topic, partitionNum, req, func(r partition.AppendResult) { result <- r })
	return <-result
}

func TestUnknownTopicOrPartition(t *testing.T) {
	b := openBroker(t, false)
	if _, err := b.CreateTopic("orders", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	cases := []struct {
		topic     string
		partition int32
	}{
		{"missing", 0},
		{"orders", 1},
		{"orders", -1},
	}
	for _, c := range cases {
		if r := publishSync(b, c.topic, c.partition, "x"); !errors.Is(r.Err, ErrUnknownTopicOrPartition) {
			t.Errorf("Publish(%s-%d) = %v, want ErrUnknownTopicOrPartition", c.topic, c.partition, r.Err)
		}
		if err := b.Commit("g", c.topic, c.partition, 0); !errors.Is(err, ErrUnknownTopicOrPartition) {
			t.Errorf("Commit(%s-%d) = %v, want ErrUnknownTopicOrPartition", c.topic, c.partition, err)
		}
	}
}

func TestAutoCreateTopic(t *testing.T) {
	b := openBroker(t, true)

	if r := publishSync(b, "events", 0, "hello"); r.Err != nil || r.Offset != 0 {
		t.Fatalf("Publish to new topic = %+v, want offset 0", r)
	}
	_, topics := b.Metadata([]string{"events"})
	if len(topics) != 1 || len(topics[0].Partitions) != 1 {
		t.Fatalf("Metadata = %+v, want events with 1 partition", topics)
	}
	// Auto-create makes the topic, not arbitrary partition numbers.
	if r := publishSync(b, "events", 3, "x"); !errors.Is(r.Err, ErrUnknownTopicOrPartition) {
		t.Fatalf("Publish to events-3 = %v, want ErrUnknownTopicOrPartition", r.Err)
	}
	// Consume does not auto-create: only publishing does.
	if _, _, err := b.Consume(context.Background(), "fresh", 0, 0, 10, 0); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Fatalf("Consume unknown topic = %v, want ErrUnknownTopicOrPartition", err)
	}
	if _, topics := b.Metadata([]string{"fresh"}); len(topics) != 0 {
		t.Fatalf("Consume created topic fresh: %+v", topics)
	}
}

func TestConsumeLongPollTimesOutEmpty(t *testing.T) {
	b := openBroker(t, false)
	if _, err := b.CreateTopic("orders", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	start := time.Now()
	chunk, _, err := b.Consume(context.Background(), "orders", 0, 0, 10, 50*time.Millisecond)
	defer chunk.Close()
	if err != nil || chunk.Size != 0 {
		t.Fatalf("Consume = (%+v, %v), want empty chunk and nil error", chunk, err)
	}
	if waited := time.Since(start); waited < 50*time.Millisecond {
		t.Fatalf("returned after %v, want it to wait 50ms", waited)
	}
}
