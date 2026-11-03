package partition

import (
	"fmt"

	"messagelog/internal/record"
	"messagelog/internal/segment"
)

// recovered is the state Open needs from recovery.
type recovered struct {
	segments  []*segment.Segment // sorted by base; the last is active
	producers producerState
}

// scanStart is the durable point recovery scans forward from.
type scanStart struct {
	segment    int // index into the segment list
	position   int64
	nextOffset int64
	producers  producerState
}

// recoverLog opens the partition's segments and repairs the log tail
// (DESIGN "Crash recovery flow"). Segments before the checkpoint are trusted;
// from the checkpoint on, every record is validated, a torn tail is cut off,
// and producer sequences are replayed from the headers. Without a usable
// checkpoint the whole log is scanned.
func recoverLog(cfg Config) (recovered, error) {
	bases, err := segment.List(cfg.Dir)
	if err != nil {
		return recovered{}, err
	}
	if len(bases) == 0 {
		s, err := segment.Create(cfg.Dir, 0, cfg.IndexIntervalBytes)
		if err != nil {
			return recovered{}, err
		}
		return recovered{segments: []*segment.Segment{s}, producers: producerState{}}, nil
	}

	segs, err := openSegments(cfg, bases)
	if err != nil {
		return recovered{}, err
	}
	cp, haveCheckpoint, err := readCheckpoint(cfg.Dir)
	if err != nil {
		closeSegments(segs)
		return recovered{}, err
	}
	start := findScanStart(segs, cp, haveCheckpoint)
	if err := repairTail(segs, start); err != nil {
		closeSegments(segs)
		return recovered{}, err
	}
	return recovered{segments: segs, producers: start.producers}, nil
}

func openSegments(cfg Config, bases []int64) ([]*segment.Segment, error) {
	segs := make([]*segment.Segment, 0, len(bases))
	for _, base := range bases {
		s, err := segment.Open(cfg.Dir, base, cfg.IndexIntervalBytes)
		if err != nil {
			closeSegments(segs)
			return nil, err
		}
		segs = append(segs, s)
	}
	return segs, nil
}

// findScanStart uses the checkpoint if it points inside an existing
// segment; otherwise recovery starts from the very beginning.
func findScanStart(segs []*segment.Segment, cp checkpoint, haveCheckpoint bool) scanStart {
	if haveCheckpoint {
		for i, s := range segs {
			if s.Base() == cp.SegmentBase && cp.Position <= s.Size() {
				return scanStart{segment: i, position: cp.Position, nextOffset: cp.NextOffset, producers: cp.Producers}
			}
		}
	}
	return scanStart{segment: 0, position: 0, nextOffset: segs[0].Base(), producers: producerState{}}
}

// repairTail validates every segment from start onwards and replays producer
// sequences from their headers.
//
// Only the last segment can legitimately be torn by a crash: every earlier
// one was fsynced in full before its successor was created (writer.roll).
// So a bad record or an offset gap anywhere else is corruption, and Open
// fails with ErrCorruptLog rather than truncating, which would throw away
// every acked record in the later segments.
func repairTail(segs []*segment.Segment, start scanStart) error {
	replay := func(h record.Header) {
		if h.ProducerID != record.NoProducerID {
			start.producers.record(h.ProducerID, h.Sequence, h.Offset)
		}
	}
	position, next := start.position, start.nextOffset
	for i := start.segment; i < len(segs); i++ {
		s := segs[i]
		if i > start.segment && s.Base() != next {
			return fmt.Errorf("%w: segment %d does not continue at offset %d", ErrCorruptLog, s.Base(), next)
		}
		torn, err := s.Recover(position, next, replay)
		if err != nil {
			return fmt.Errorf("recover segment %d: %w", s.Base(), err)
		}
		if torn {
			if i < len(segs)-1 {
				return fmt.Errorf("%w: bad record in closed segment %d at byte %d (offset %d)",
					ErrCorruptLog, s.Base(), s.Size(), s.NextOffset())
			}
			if err := s.TruncateTail(); err != nil {
				return err
			}
		}
		position, next = 0, s.NextOffset()
	}
	return nil
}

func closeSegments(segs []*segment.Segment) {
	for _, s := range segs {
		s.Close()
	}
}
