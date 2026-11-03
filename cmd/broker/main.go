// Command broker runs a single messagelog broker.
//
//	broker -addr :9092 -data ./data
//
// SIGINT or SIGTERM shuts it down gracefully: stop accepting, answer or
// drop in-flight requests, then flush and checkpoint every partition.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"messagelog/internal/broker"
	"messagelog/internal/partition"
	"messagelog/internal/server"
)

type options struct {
	addr          string
	advertise     string
	dataDir       string
	brokerID      int
	ack           string
	flushInterval time.Duration
	segmentBytes  int64
	retention     time.Duration
	autoCreate    bool
	maxConns      int
	idleTimeout   time.Duration
}

func parseFlags() options {
	var o options
	flag.StringVar(&o.addr, "addr", ":9092", "listen address")
	flag.StringVar(&o.advertise, "advertise", "", "address clients should use, returned in metadata (default: -addr)")
	flag.StringVar(&o.dataDir, "data", "./data", "data directory")
	flag.IntVar(&o.brokerID, "id", 1, "broker id")
	flag.StringVar(&o.ack, "ack", "fsync", "when to ack a publish: fsync (durable) or async (before fsync)")
	flag.DurationVar(&o.flushInterval, "flush-interval", 2*time.Millisecond, "group commit interval")
	flag.Int64Var(&o.segmentBytes, "segment-bytes", 1<<30, "segment roll size in bytes")
	flag.DurationVar(&o.retention, "retention", 7*24*time.Hour, "delete segments older than this")
	flag.BoolVar(&o.autoCreate, "auto-create", false, "create unknown topics on publish")
	flag.IntVar(&o.maxConns, "max-conns", 1024, "maximum open client connections")
	flag.DurationVar(&o.idleTimeout, "idle-timeout", 5*time.Minute, "close connections that send nothing for this long")
	flag.Parse()
	if o.advertise == "" {
		o.advertise = o.addr
	}
	return o
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(parseFlags(), log); err != nil {
		log.Error("broker failed", "err", err)
		os.Exit(1)
	}
}

func run(o options, log *slog.Logger) error {
	cfg, err := brokerConfig(o)
	if err != nil {
		return err
	}
	b, err := broker.Open(cfg)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", o.addr)
	if err != nil {
		b.Close()
		return fmt.Errorf("listen: %w", err)
	}

	srv := server.New(b, log, server.Options{MaxConns: o.maxConns, IdleTimeout: o.idleTimeout})
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err = <-served:
		log.Error("server stopped", "err", err)
	}

	// Server first, so no request reaches a closed broker.
	srv.Close()
	if cerr := b.Close(); cerr != nil {
		return errors.Join(err, cerr)
	}
	log.Info("shutdown complete")
	return err
}

func brokerConfig(o options) (broker.Config, error) {
	ackModes := map[string]partition.AckMode{"fsync": partition.AckAfterFsync, "async": partition.AckBeforeFsync}
	ackMode, ok := ackModes[o.ack]
	if !ok {
		return broker.Config{}, fmt.Errorf("-ack must be fsync or async, got %q", o.ack)
	}
	return broker.Config{
		DataDir:          o.dataDir,
		BrokerID:         int32(o.brokerID),
		AdvertisedAddr:   o.advertise,
		AutoCreateTopics: o.autoCreate,
		Partition: func(dir string) partition.Config {
			c := partition.DefaultConfig(dir)
			c.AckMode = ackMode
			c.FlushInterval = o.flushInterval
			c.SegmentBytes = o.segmentBytes
			c.RetentionAge = o.retention
			return c
		},
	}, nil
}
