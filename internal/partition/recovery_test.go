package partition

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"messagelog/internal/record"
	"messagelog/internal/segment"
)

// writeCrashFixture appends 4 records and returns the open partition. The
// first flush writes the checkpoint (after record 0), so records 1-3 sit in
// the tail that recovery must scan.
func writeCrashFixture(t *testing.T) *Partition {
	p := mustOpen(t, testConfig(t))
	appendBodies(t, p, 0, 4)
	return p
}

func TestCrashRecoveryTruncatesBadTail(t *testing.T) {
	cases := []struct {
		name   string
		tamper func(t *testing.T, log string)
		want   int // records left
	}{
		{"clean crash", func(*testing.T, string) {}, 4},
		{"partial header", func(t *testing.T, log string) {
			appendToFile(t, log, make([]byte, 10))
		}, 4},
		{"partial body", func(t *testing.T, log string) {
			torn := record.Append(nil, record.Record{Offset: 4, Body: body(4)})
			appendToFile(t, log, torn[:record.HeaderSize+3])
		}, 4},
		{"flipped body byte", func(t *testing.T, log string) {
			data, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			data[len(data)-1] ^= 0xFF // last byte of record 3's body
			if err := os.WriteFile(log, data, 0o644); err != nil {
				t.Fatal(err)
			}
		}, 3},
		{"garbage appended", func(t *testing.T, log string) {
			appendToFile(t, log, bytes.Repeat([]byte{0xAB}, 100))
		}, 4},
		{"valid record with wrong offset", func(t *testing.T, log string) {
			appendToFile(t, log, record.Append(nil, record.Record{Offset: 9, Body: body(9)}))
		}, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := writeCrashFixture(t)
			log := segmentPath(p.cfg.Dir, 0)
			p = crashAndReopen(t, p, func(string) { tc.tamper(t, log) })

			assertLog(t, p, tc.want)
			if got := fileSize(t, log); got != int64(tc.want*recordSize) {
				t.Fatalf("log file is %d bytes, want %d", got, tc.want*recordSize)
			}
			appendBodies(t, p, tc.want, tc.want+2)
			p = reopen(t, p)
			assertLog(t, p, tc.want+2)
		})
	}
}

func TestCrashRecoveryWithoutCheckpointScansEverything(t *testing.T) {
	p := writeCrashFixture(t)
	p = crashAndReopen(t, p, func(dir string) {
		os.Remove(checkpointPath(dir))
		appendToFile(t, segmentPath(dir, 0), []byte("torn"))
	})
	assertLog(t, p, 4)
}

// TestRecoveryTrustsCheckpointedPrefix shows recovery only scans after the
// checkpoint: damage before it goes unnoticed, which is what makes
// restarts fast (DESIGN: "scan only the last segment from the checkpoint").
func TestRecoveryTrustsCheckpointedPrefix(t *testing.T) {
	p := writeCrashFixture(t)
	log := segmentPath(p.cfg.Dir, 0)
	p.Close() // checkpoint now covers all 4 records
	data, _ := os.ReadFile(log)
	data[record.HeaderSize] ^= 0xFF // record 0's body
	os.WriteFile(log, data, 0o644)

	p = mustOpen(t, p.cfg)
	if hw := p.HighWatermark(); hw != 4 {
		t.Fatalf("high watermark = %d, want 4 (prefix must not be rescanned)", hw)
	}
}

// writeSegmentsFixture writes 6 records into segments 0, 2 and 4, crashes,
// and lets tamper damage the files. Every test here drops the checkpoint
// one way or another, forcing a full scan.
func writeSegmentsFixture(t *testing.T, tamper func(dir string)) Config {
	cfg := testConfig(t)
	cfg.SegmentBytes = int64(2 * recordSize)
	p := mustOpen(t, cfg)
	appendBodies(t, p, 0, 6)
	p.crashForTest()
	tamper(cfg.Dir)
	return cfg
}

func TestFullScanTruncatesTornLastSegment(t *testing.T) {
	cfg := writeSegmentsFixture(t, func(dir string) {
		os.Remove(checkpointPath(dir))
		os.Truncate(segmentPath(dir, 4), int64(2*recordSize-1)) // tear record 5
	})
	p := mustOpen(t, cfg)
	assertLog(t, p, 5)
	appendBodies(t, p, 5, 7)
	assertLog(t, p, 7)
}

// TestFullScanRefusesCorruptClosedSegment: a closed segment was fsynced
// before the next one was created, so a bad record in it is corruption, not
// a torn write. Recovery must fail rather than drop later, acked segments.
func TestFullScanRefusesCorruptClosedSegment(t *testing.T) {
	losingCheckpoint := map[string]func(dir string){
		"missing checkpoint": func(dir string) { os.Remove(checkpointPath(dir)) },
		"unparseable checkpoint": func(dir string) {
			os.WriteFile(checkpointPath(dir), []byte("{not json"), 0o644)
		},
	}
	for name, loseCheckpoint := range losingCheckpoint {
		t.Run(name, func(t *testing.T) {
			cfg := writeSegmentsFixture(t, func(dir string) {
				loseCheckpoint(dir)
				log := segmentPath(dir, 2)
				data, _ := os.ReadFile(log)
				data[len(data)-1] ^= 0xFF // last byte of record 3's body
				os.WriteFile(log, data, 0o644)
			})
			sizes := map[int64]int64{}
			for _, base := range []int64{0, 2, 4} {
				sizes[base] = fileSize(t, segmentPath(cfg.Dir, base))
			}

			p, err := Open(cfg)
			if err == nil {
				p.Close()
				t.Fatal("Open succeeded on a corrupt closed segment")
			}
			if !errors.Is(err, ErrCorruptLog) {
				t.Fatalf("Open: %v, want ErrCorruptLog", err)
			}
			for base, size := range sizes {
				if got := fileSize(t, segmentPath(cfg.Dir, base)); got != size {
					t.Fatalf("segment %d is %d bytes after failed Open, want %d untouched", base, got, size)
				}
			}
		})
	}
}

func TestFullScanRefusesMissingMiddleSegment(t *testing.T) {
	cfg := writeSegmentsFixture(t, func(dir string) {
		os.Remove(checkpointPath(dir))
		os.Remove(segmentPath(dir, 2))
	})
	if _, err := Open(cfg); !errors.Is(err, ErrCorruptLog) {
		t.Fatalf("Open with a gap: %v, want ErrCorruptLog", err)
	}
	if bases, _ := segment.List(cfg.Dir); len(bases) != 2 {
		t.Fatalf("segments after failed Open = %v, want [0 4] untouched", bases)
	}
}

func TestProducerStateSurvivesRestart(t *testing.T) {
	restarts := []struct {
		name    string
		restart func(t *testing.T, p *Partition) *Partition
	}{
		{"close", reopen},
		{"crash with checkpoint", func(t *testing.T, p *Partition) *Partition {
			return crashAndReopen(t, p, nil)
		}},
		{"crash without checkpoint", func(t *testing.T, p *Partition) *Partition {
			return crashAndReopen(t, p, func(dir string) { os.Remove(checkpointPath(dir)) })
		}},
	}
	for _, tc := range restarts {
		t.Run(tc.name, func(t *testing.T) {
			p := mustOpen(t, testConfig(t))
			const pid = 7
			for seq := int64(0); seq < 3; seq++ {
				if res := appendSeq(p, pid, seq); res.Err != nil {
					t.Fatal(res.Err)
				}
			}
			p = tc.restart(t, p)

			// seq 0 comes from the checkpoint snapshot (if any), 1-2 from
			// replaying record headers.
			for seq := int64(0); seq < 3; seq++ {
				if res := appendSeq(p, pid, seq); res.Offset != seq || !res.Duplicate {
					t.Fatalf("retry seq %d after restart: %+v", seq, res)
				}
			}
			if res := appendSeq(p, pid, 4); !errors.Is(res.Err, ErrOutOfOrderSeq) {
				t.Fatalf("gap after restart: %+v", res)
			}
			if res := appendSeq(p, pid, 3); res.Err != nil || res.Offset != 3 || res.Duplicate {
				t.Fatalf("next seq after restart: %+v", res)
			}
		})
	}
}
