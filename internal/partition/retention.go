package partition

import (
	"errors"
	"fmt"
	"time"

	"messagelog/internal/fsutil"
	"messagelog/internal/segment"
)

// runRetention calls EnforceRetention every RetentionCheck until Close
// (DESIGN "Retention flow"). Errors are not fatal: the next tick retries,
// and callers that care can call EnforceRetention themselves.
func (p *Partition) runRetention() {
	defer close(p.retentionDone)
	ticker := time.NewTicker(p.cfg.RetentionCheck)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = p.EnforceRetention()
		case <-p.stopRetention:
			return
		}
	}
}

// EnforceRetention deletes expired closed segments now. The background job calls it too.
//
// Segments are deleted oldest first, whole, while the oldest one's newest
// record is older than RetentionAge or the partition is over
// RetentionBytes. The active segment is never deleted. Deleting only from
// the front keeps the log gap-free.
func (p *Partition) EnforceRetention() error {
	p.retentionMu.Lock()
	defer p.retentionMu.Unlock()
	if p.closed.Load() {
		return ErrClosed
	}
	expired := p.detachExpired()
	if len(expired) == 0 {
		return nil
	}
	var errs []error
	for _, s := range expired {
		if err := s.Remove(); err != nil {
			errs = append(errs, fmt.Errorf("retention: %w", err))
		}
	}
	if err := fsutil.SyncDir(p.cfg.Dir); err != nil {
		errs = append(errs, fmt.Errorf("retention: %w", err))
	}
	return errors.Join(errs...)
}

// detachExpired removes expired segments from the list and advances the log
// start offset, so new readers stop finding them before the files go away.
func (p *Partition) detachExpired() []*segment.Segment {
	cutoff := p.cfg.Now().Add(-p.cfg.RetentionAge).UnixMilli()

	p.segMu.Lock()
	defer p.segMu.Unlock()
	var total int64
	for _, s := range p.segments {
		total += s.Size()
	}
	n := 0
	for ; n < len(p.segments)-1; n++ { // the last one is active
		s := p.segments[n]
		if s.LastTimestamp() >= cutoff && total <= p.cfg.RetentionBytes {
			break
		}
		total -= s.Size()
	}
	expired := p.segments[:n]
	p.segments = p.segments[n:]
	p.logStartOffset.Store(p.segments[0].Base())
	return expired
}
