package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"messagelog/client"
	"messagelog/internal/record"
)

// bench publishes n messages from c goroutines that share one Producer, so
// up to c requests are pipelined on one connection, and reports throughput
// and publish latency. The first error stops every publisher.
func bench(ctx context.Context, addr string, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	topic := fs.String("topic", "bench", "topic (created if missing)")
	n := fs.Int("n", 10000, "messages to publish")
	concurrency := fs.Int("c", 8, "concurrent publishers")
	size := fs.Int("size", 1024, "message size in bytes")
	if _, err := parseArgs(fs, args, 0); err != nil {
		return err
	}
	if *n <= 0 || *concurrency <= 0 {
		return errors.New("bench: -n and -c must be positive")
	}
	if *size < 0 || *size > record.MaxBodySize {
		return fmt.Errorf("bench: -size must be 0..%d", record.MaxBodySize)
	}
	if err := ensureTopic(ctx, addr, *topic); err != nil {
		return err
	}
	p, err := client.NewProducer(ctx, addr, client.ProducerConfig{})
	if err != nil {
		return err
	}
	defer p.Close()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	value := make([]byte, *size)
	latencies := make([]time.Duration, *n)
	var next atomic.Int64 // index of the next message to send

	start := time.Now()
	var wg sync.WaitGroup
	for range *concurrency {
		wg.Go(func() {
			for i := next.Add(1) - 1; i < int64(*n) && ctx.Err() == nil; i = next.Add(1) - 1 {
				sent := time.Now()
				if _, err := p.Publish(ctx, *topic, value); err != nil {
					cancel(err) // keeps the first cause; stops the other publishers
					return
				}
				latencies[i] = time.Since(sent)
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	if err := context.Cause(ctx); err != nil {
		return err
	}

	slices.Sort(latencies)
	percentile := func(p float64) time.Duration { return latencies[int(p*float64(len(latencies)-1))] }
	secs := elapsed.Seconds()
	fmt.Printf("published %d x %d B with %d publishers in %v\n", *n, *size, *concurrency, elapsed.Round(time.Millisecond))
	fmt.Printf("throughput: %.0f msgs/s, %.2f MB/s\n", float64(*n)/secs, float64(*n**size)/secs/1e6)
	fmt.Printf("latency:    p50 %v, p99 %v, max %v\n",
		percentile(0.50).Round(time.Microsecond), percentile(0.99).Round(time.Microsecond), latencies[len(latencies)-1].Round(time.Microsecond))
	return nil
}

func ensureTopic(ctx context.Context, addr, topic string) error {
	conn, err := client.Dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.CreateTopic(ctx, topic, 1); err != nil && !errors.Is(err, client.ErrTopicExists) {
		return err
	}
	return nil
}
