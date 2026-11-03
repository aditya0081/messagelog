package partition

import (
	"fmt"
	"slices"
)

// dedupWindow is how many recent (sequence → offset) pairs are remembered
// per producer. A retry of any of them re-acks with the original offset.
const dedupWindow = 5

// seqOffset remembers where one producer sequence landed in the log.
// Exported fields so the checkpoint can serialize it.
type seqOffset struct {
	Seq    int64
	Offset int64
}

// producerState is the idempotent-producer table (DESIGN "Exactly-once"):
// for each producer, its last few sequences, oldest first. It covers every
// record written to the log, flushed or not. Only the writer goroutine
// touches it (recovery builds it before the writer starts).
type producerState map[int64][]seqOffset

// check classifies seq from producer pid against what is already in the log:
//   - seq < 0: invalid, ErrOutOfOrderSeq.
//   - seq == last+1 (or 0 for a new producer): new, write it.
//   - seq <= last: duplicate; offset is the original one, or -1 if it has
//     fallen out of the window.
//   - seq > last+1: a gap, ErrOutOfOrderSeq.
func (s producerState) check(pid, seq int64) (duplicate bool, offset int64, err error) {
	window := s[pid]
	last := int64(-1) // so a new producer must start at 0
	if len(window) > 0 {
		last = window[len(window)-1].Seq
	}
	switch {
	case seq < 0:
		return false, 0, fmt.Errorf("%w: negative sequence %d", ErrOutOfOrderSeq, seq)
	case seq == last+1:
		return false, 0, nil
	case seq > last+1:
		return false, 0, ErrOutOfOrderSeq
	}
	i := slices.IndexFunc(window, func(e seqOffset) bool { return e.Seq == seq })
	if i < 0 {
		return true, -1, nil
	}
	return true, window[i].Offset, nil
}

// record notes that seq from pid was written at offset. Recovery also calls
// it while replaying record headers.
func (s producerState) record(pid, seq, offset int64) {
	window := append(s[pid], seqOffset{Seq: seq, Offset: offset})
	if len(window) > dedupWindow {
		window = slices.Delete(window, 0, len(window)-dedupWindow)
	}
	s[pid] = window
}

// expire forgets every producer whose newest record is below logStart, i.e.
// retention has deleted all its records (Kafka's rule). Such a producer is
// new again: its next request must use sequence 0.
func (s producerState) expire(logStart int64) {
	for pid, window := range s {
		if window[len(window)-1].Offset < logStart {
			delete(s, pid)
		}
	}
}
