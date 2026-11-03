package partition

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"messagelog/internal/fsutil"
)

const checkpointFile = "recovery.checkpoint"

// checkpoint is the recovery point: everything before (SegmentBase,
// Position) is known to be fsynced, so recovery only scans what comes after.
// Producers is the producer table as of that point; recovery replays the
// scanned tail on top of it.
type checkpoint struct {
	SegmentBase int64
	Position    int64
	NextOffset  int64
	Producers   producerState
}

// writeCheckpoint replaces the checkpoint atomically: a crash leaves either
// the old or the new checkpoint, never a torn one.
func writeCheckpoint(dir string, cp checkpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	if err := fsutil.WriteFileAtomic(filepath.Join(dir, checkpointFile), data); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	return nil
}

// readCheckpoint loads the checkpoint; ok is false if there is none. An
// unparseable checkpoint is logged and also treated as none: atomic writes
// mean it should never happen, and a full scan recovers correctly anyway.
func readCheckpoint(dir string) (cp checkpoint, ok bool, err error) {
	path := filepath.Join(dir, checkpointFile)
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return checkpoint{}, false, nil
	}
	if err != nil {
		return checkpoint{}, false, fmt.Errorf("read checkpoint: %w", err)
	}
	if err := json.Unmarshal(data, &cp); err != nil {
		slog.Warn("partition: ignoring unparseable recovery checkpoint, scanning the whole log",
			"path", path, "err", err)
		return checkpoint{}, false, nil
	}
	if cp.Producers == nil {
		cp.Producers = producerState{}
	}
	return cp, true, nil
}
