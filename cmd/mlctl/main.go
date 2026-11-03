// Command mlctl is a command-line client for a messagelog broker.
//
//	mlctl [-addr host:port] <command> [args]
//
// Run mlctl without arguments for the list of commands.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	"messagelog/client"
)

const usage = `usage: mlctl [-addr host:port] <command> [args]

commands:
  create-topic TOPIC [-partitions N]
  topics
  publish TOPIC MSG...
  consume TOPIC [-partition P] [-offset O] [-limit N] [-follow]
  commit GROUP TOPIC OFFSET [-partition P]
  committed GROUP TOPIC [-partition P]
  bench [-topic T] [-n N] [-c C] [-size BYTES]
`

type command func(ctx context.Context, addr string, args []string) error

var commands = map[string]command{
	"create-topic": createTopic,
	"topics":       listTopics,
	"publish":      publish,
	"consume":      consume,
	"commit":       commit,
	"committed":    committed,
	"bench":        bench,
}

func main() {
	addr := flag.String("addr", "localhost:9092", "broker address")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	cmd, ok := commands[flag.Arg(0)]
	if !ok {
		fmt.Fprintf(os.Stderr, "mlctl: unknown command %q\n\n%s", flag.Arg(0), usage)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := cmd(ctx, *addr, flag.Args()[1:]); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "mlctl:", err)
		os.Exit(1)
	}
}

// parseArgs parses flags that may come before, between or after the
// positional arguments (the flag package stops at the first positional
// one), and checks the positional count.
func parseArgs(fs *flag.FlagSet, args []string, wantPositional int) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			break
		}
		positional = append(positional, fs.Arg(0))
		args = fs.Args()[1:]
	}
	if wantPositional >= 0 && len(positional) != wantPositional {
		return nil, fmt.Errorf("%s: want %d arguments, got %d (see mlctl -h)", fs.Name(), wantPositional, len(positional))
	}
	return positional, nil
}

func createTopic(ctx context.Context, addr string, args []string) error {
	fs := flag.NewFlagSet("create-topic", flag.ContinueOnError)
	partitions := fs.Int("partitions", 1, "number of partitions")
	pos, err := parseArgs(fs, args, 1)
	if err != nil {
		return err
	}
	conn, err := client.Dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.CreateTopic(ctx, pos[0], *partitions); err != nil {
		return err
	}
	fmt.Printf("created %s with %d partition(s)\n", pos[0], *partitions)
	return nil
}

func listTopics(ctx context.Context, addr string, args []string) error {
	if _, err := parseArgs(flag.NewFlagSet("topics", flag.ContinueOnError), args, 0); err != nil {
		return err
	}
	conn, err := client.Dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	md, err := conn.Metadata(ctx)
	if err != nil {
		return err
	}
	for _, t := range md.Topics {
		for _, p := range t.Partitions {
			fmt.Printf("%s\tpartition=%d\tleader=%d\n", t.Name, p.Number, p.LeaderID)
		}
	}
	return nil
}

func publish(ctx context.Context, addr string, args []string) error {
	pos, err := parseArgs(flag.NewFlagSet("publish", flag.ContinueOnError), args, -1)
	if err != nil {
		return err
	}
	if len(pos) < 2 {
		return errors.New("publish: want TOPIC MSG...")
	}
	p, err := client.NewProducer(ctx, addr, client.ProducerConfig{})
	if err != nil {
		return err
	}
	defer p.Close()
	for _, msg := range pos[1:] {
		res, err := p.Publish(ctx, pos[0], []byte(msg))
		if err != nil {
			return err
		}
		fmt.Printf("partition=%d offset=%d\n", res.Partition, res.Offset)
	}
	return nil
}

func consume(ctx context.Context, addr string, args []string) error {
	fs := flag.NewFlagSet("consume", flag.ContinueOnError)
	partition := fs.Int("partition", 0, "partition")
	offset := fs.Int64("offset", 0, "first offset to read")
	limit := fs.Int("limit", 0, "stop after this many records (0 = no limit)")
	follow := fs.Bool("follow", false, "keep waiting for new records")
	pos, err := parseArgs(fs, args, 1)
	if err != nil {
		return err
	}
	c, err := client.NewConsumer(ctx, addr)
	if err != nil {
		return err
	}
	defer c.Close()

	maxWait := time.Duration(0)
	if *follow {
		maxWait = time.Second
	}
	next, printed := *offset, 0
	for *limit == 0 || printed < *limit {
		batch := 500
		if *limit > 0 {
			batch = min(batch, *limit-printed)
		}
		records, hw, err := c.Consume(ctx, pos[0], int32(*partition), next, batch, maxWait)
		if err != nil {
			return err
		}
		for _, r := range records {
			ts := time.UnixMilli(r.Timestamp).UTC().Format(time.RFC3339Nano)
			fmt.Printf("offset=%d time=%s %s\n", r.Offset, ts, r.Body)
			next = r.Offset + 1
		}
		printed += len(records)
		if !*follow && next >= hw {
			break
		}
	}
	return nil
}

func commit(ctx context.Context, addr string, args []string) error {
	fs := flag.NewFlagSet("commit", flag.ContinueOnError)
	partition := fs.Int("partition", 0, "partition")
	pos, err := parseArgs(fs, args, 3)
	if err != nil {
		return err
	}
	offset, err := strconv.ParseInt(pos[2], 10, 64)
	if err != nil {
		return fmt.Errorf("commit: bad offset %q", pos[2])
	}
	c, err := client.NewConsumer(ctx, addr)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Commit(ctx, pos[0], pos[1], int32(*partition), offset); err != nil {
		return err
	}
	fmt.Printf("committed %s %s-%d at %d\n", pos[0], pos[1], *partition, offset)
	return nil
}

func committed(ctx context.Context, addr string, args []string) error {
	fs := flag.NewFlagSet("committed", flag.ContinueOnError)
	partition := fs.Int("partition", 0, "partition")
	pos, err := parseArgs(fs, args, 2)
	if err != nil {
		return err
	}
	c, err := client.NewConsumer(ctx, addr)
	if err != nil {
		return err
	}
	defer c.Close()
	offset, found, err := c.CommittedOffset(ctx, pos[0], pos[1], int32(*partition))
	if err != nil {
		return err
	}
	if !found {
		fmt.Println("no committed offset")
		return nil
	}
	fmt.Println(offset)
	return nil
}
