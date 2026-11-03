// Package offsets stores the last committed offset of each consumer group per
// topic partition.
//
// Commits are appended to one log file ("offsets.log") and mirrored in an
// in-memory map, so reads never touch disk. The log only ever grows, so it is
// compacted (rewritten with just the latest entry per key) once it is mostly
// overwritten entries. On open the log is replayed into the map; a torn or
// corrupt tail from a crash is truncated, exactly like partition recovery.
package offsets

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"messagelog/internal/fsutil"
)

const (
	logName = "offsets.log"

	// DefaultCompactAfterBytes is used when Options.CompactAfterBytes is 0.
	DefaultCompactAfterBytes = 64 << 20 // 64 MiB
)

var (
	// ErrClosed is returned by Commit and Compact after Close.
	ErrClosed = errors.New("offsets: closed")
	// ErrNameTooLong means a group or topic name does not fit the log format.
	ErrNameTooLong = errors.New("offsets: group or topic name too long")
)

// Key identifies whose progress an offset records.
type Key struct {
	Group     string
	Topic     string
	Partition int32
}

// Options tunes a Store. The zero value uses the defaults.
type Options struct {
	CompactAfterBytes int64 // compact when the log exceeds this and is >2x live size; default 64 MiB
}

// Store is the committed-offset store. It is safe for concurrent use.
//
// One mutex serializes everything, including the fsync in Commit. That is
// deliberate: commits are far rarer than publishes (a consumer commits once
// per batch, not per message), so a simple lock beats a group-commit pipeline.
type Store struct {
	dir  string
	opts Options

	mu        sync.Mutex
	file      *os.File // offsets.log opened for append; nil after Close
	size      int64    // bytes in the log
	liveBytes int64    // bytes the log would have after compaction
	latest    map[Key]int64

	// failed is set when the log file may no longer match what was
	// acknowledged (see append and compactLocked). Commit and Compact then
	// return it until the store is reopened, which runs recovery.
	failed error
	// retryCompactionAt is the log size at which automatic compaction is
	// tried again after it failed, so a persistent failure (e.g. a full
	// disk) is not retried on every commit.
	retryCompactionAt int64
}

// Open replays the commit log in dir (file "offsets.log") into memory.
// A torn/corrupt tail is truncated, like partition recovery.
func Open(dir string, opts Options) (*Store, error) {
	if opts.CompactAfterBytes <= 0 {
		opts.CompactAfterBytes = DefaultCompactAfterBytes
	}
	if err := fsutil.MkdirAll(dir); err != nil {
		return nil, fmt.Errorf("offsets: %w", err)
	}
	path := filepath.Join(dir, logName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("offsets: open %s: %w", path, err)
	}
	// The file may have just been created; make its name durable before
	// any commit is acknowledged.
	if err := fsutil.SyncDir(dir); err != nil {
		f.Close()
		return nil, fmt.Errorf("offsets: %w", err)
	}
	s := &Store{dir: dir, opts: opts, file: f, latest: map[Key]int64{}}
	if err := s.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// recover replays the log into the map and truncates anything after the
// last valid entry.
func (s *Store) recover() error {
	valid, err := s.replay()
	if err != nil {
		return err
	}
	info, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("offsets: stat log: %w", err)
	}
	if info.Size() > valid {
		if err := s.file.Truncate(valid); err != nil {
			return fmt.Errorf("offsets: truncate torn tail: %w", err)
		}
		if err := s.file.Sync(); err != nil {
			return fmt.Errorf("offsets: sync after truncate: %w", err)
		}
	}
	s.size = valid
	return nil
}

// replay applies every valid entry to the map and returns the byte length
// of the valid prefix. The file was just opened, so reads start at byte 0.
func (s *Store) replay() (int64, error) {
	r := bufio.NewReader(s.file)
	var valid int64
	for {
		k, offset, n, err := readEntry(r)
		if err == io.EOF || errors.Is(err, errBadEntry) {
			return valid, nil
		}
		if err != nil {
			return 0, fmt.Errorf("offsets: replay: %w", err)
		}
		s.apply(k, offset)
		valid += n
	}
}

// apply records offset as the latest for k and keeps liveBytes in step.
func (s *Store) apply(k Key, offset int64) {
	if _, seen := s.latest[k]; !seen {
		s.liveBytes += entrySize(k)
	}
	s.latest[k] = offset
}

// Commit durably records offset as k's progress. It returns after fsync.
//
// If the log has grown mostly stale, it is compacted first. The commit is
// already durable by then, so a compaction failure does not fail it: it is
// logged and retried once the log has doubled in size.
func (s *Store) Commit(k Key, offset int64) error {
	if len(k.Group) > maxNameLen || len(k.Topic) > maxNameLen {
		return fmt.Errorf("%w: group %d bytes, topic %d bytes, max %d",
			ErrNameTooLong, len(k.Group), len(k.Topic), maxNameLen)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writable(); err != nil {
		return err
	}
	if err := s.append(appendEntry(nil, k, offset)); err != nil {
		return err
	}
	s.apply(k, offset)

	if s.needsCompaction() {
		if err := s.compactLocked(); err != nil {
			s.retryCompactionAt = 2 * s.size
			slog.Warn("offsets: automatic compaction failed; commits continue",
				"err", err, "retryAtBytes", s.retryCompactionAt)
		}
	}
	return nil
}

// writable returns why the store cannot take writes, or nil.
func (s *Store) writable() error {
	if s.file == nil {
		return ErrClosed
	}
	return s.failed
}

// append writes and fsyncs one entry. On failure it cuts the file back to
// its previous size, so a half-written entry cannot sit in front of later
// commits and get them truncated away on the next recovery. If even that
// fails, the store stops taking writes.
func (s *Store) append(entry []byte) error {
	_, err := s.file.Write(entry)
	if err == nil {
		err = s.file.Sync()
	}
	if err != nil {
		if terr := s.file.Truncate(s.size); terr != nil {
			s.failed = fmt.Errorf("offsets: failed append left a partial entry: %w", errors.Join(err, terr))
			return s.failed
		}
		return fmt.Errorf("offsets: append: %w", err)
	}
	s.size += int64(len(entry))
	return nil
}

func (s *Store) needsCompaction() bool {
	return s.size > s.opts.CompactAfterBytes && s.size > 2*s.liveBytes &&
		s.size >= s.retryCompactionAt
}

// Get returns the last committed offset for k.
func (s *Store) Get(k Key) (offset int64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	offset, ok = s.latest[k]
	return offset, ok
}

// Compact rewrites the log with only the latest entry per key: tmp + fsync + rename.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writable(); err != nil {
		return err
	}
	return s.compactLocked()
}

// compactLocked writes the live entries to a tmp file opened for append,
// fsyncs it and renames it over the log. The tmp handle then becomes the
// append handle (it follows the file through the rename), so there is no
// reopen step that could fail after the swap.
//
// A failure before the rename leaves the old log in use, untouched. A
// failure to fsync the directory after it means a crash could bring back
// the old log without the commits appended from now on, so the store stops
// taking writes.
func (s *Store) compactLocked() error {
	path := filepath.Join(s.dir, logName)
	tmpPath := path + ".tmp"

	tmp, err := s.writeLiveEntries(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("offsets: compact: rename: %w", err)
	}
	s.file.Close() // the old inode is unlinked; nothing more to save in it
	s.file = tmp
	s.size = s.liveBytes
	s.retryCompactionAt = 0

	if err := fsutil.SyncDir(s.dir); err != nil {
		s.failed = fmt.Errorf("offsets: compacted log may not survive a crash: %w", err)
		return s.failed
	}
	return nil
}

// writeLiveEntries creates path holding one entry per key and returns it
// fsynced and open for append.
func (s *Store) writeLiveEntries(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("offsets: compact: create tmp: %w", err)
	}
	buf := make([]byte, 0, s.liveBytes)
	for k, offset := range s.latest {
		buf = appendEntry(buf, k, offset)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return nil, fmt.Errorf("offsets: compact: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("offsets: compact: sync tmp: %w", err)
	}
	return f, nil
}

// Close closes the log. Every commit is already durable.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	if err != nil {
		return fmt.Errorf("offsets: close: %w", err)
	}
	return nil
}
