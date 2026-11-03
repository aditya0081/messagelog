package offsets

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

var orders0 = Key{Group: "billing", Topic: "orders", Partition: 0}

func openStore(t *testing.T, dir string, opts Options) *Store {
	t.Helper()
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustCommit(t *testing.T, s *Store, k Key, offset int64) {
	t.Helper()
	if err := s.Commit(k, offset); err != nil {
		t.Fatalf("Commit(%+v, %d): %v", k, offset, err)
	}
}

func wantOffset(t *testing.T, s *Store, k Key, want int64) {
	t.Helper()
	got, ok := s.Get(k)
	if !ok || got != want {
		t.Fatalf("Get(%+v) = %d, %v; want %d, true", k, got, ok, want)
	}
}

func logSize(t *testing.T, dir string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, logName))
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func TestCommitGetOverwrite(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})

	if _, ok := s.Get(orders0); ok {
		t.Fatal("Get on empty store found an offset")
	}
	mustCommit(t, s, orders0, 10)
	wantOffset(t, s, orders0, 10)

	mustCommit(t, s, orders0, 25)
	wantOffset(t, s, orders0, 25)

	other := Key{Group: "audit", Topic: "orders", Partition: 0}
	if _, ok := s.Get(other); ok {
		t.Fatal("other group sees billing's offset")
	}
}

func TestReopenRestoresLatest(t *testing.T) {
	dir := t.TempDir()
	first := openStore(t, dir, Options{})
	other := Key{Group: "audit", Topic: "orders", Partition: 1}
	mustCommit(t, first, orders0, 1)
	mustCommit(t, first, other, 7)
	mustCommit(t, first, orders0, 2)
	first.Close()

	second := openStore(t, dir, Options{})
	wantOffset(t, second, orders0, 2)
	wantOffset(t, second, other, 7)
}

func TestTornTailIsTruncated(t *testing.T) {
	entry := entrySize(orders0)
	tests := []struct {
		name   string
		damage func(t *testing.T, path string)
	}{
		{"partial header", func(t *testing.T, path string) { appendBytes(t, path, []byte{0, 0, 0}) }},
		{"partial payload", func(t *testing.T, path string) {
			appendBytes(t, path, appendEntry(nil, orders0, 99)[:entry-1])
		}},
		{"absurd length", func(t *testing.T, path string) {
			appendBytes(t, path, []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 1, 2, 3})
		}},
		{"crc mismatch", func(t *testing.T, path string) {
			bad := appendEntry(nil, orders0, 99)
			bad[len(bad)-1] ^= 0xff
			appendBytes(t, path, bad)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := openStore(t, dir, Options{})
			mustCommit(t, s, orders0, 5)
			s.Close()

			tc.damage(t, filepath.Join(dir, logName))

			s = openStore(t, dir, Options{})
			wantOffset(t, s, orders0, 5)
			if got := logSize(t, dir); got != entry {
				t.Fatalf("log size after recovery = %d, want %d", got, entry)
			}

			// Later commits land after the good prefix and survive a reopen.
			mustCommit(t, s, orders0, 6)
			s.Close()
			wantOffset(t, openStore(t, dir, Options{}), orders0, 6)
		})
	}
}

func appendBytes(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
}

func TestCompactKeepsLatestAndShrinks(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{})
	keys := []Key{orders0, {Group: "audit", Topic: "orders", Partition: 0}}
	for i := range 50 {
		for _, k := range keys {
			mustCommit(t, s, k, int64(i))
		}
	}
	before := logSize(t, dir)

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	after := logSize(t, dir)
	if want := entrySize(keys[0]) + entrySize(keys[1]); after != want {
		t.Fatalf("compacted size = %d, want %d (was %d)", after, want, before)
	}

	// The store keeps appending to the compacted file.
	mustCommit(t, s, orders0, 100)
	s.Close()
	s = openStore(t, dir, Options{})
	wantOffset(t, s, orders0, 100)
	wantOffset(t, s, keys[1], 49)
}

func TestCommitCompactsAutomatically(t *testing.T) {
	dir := t.TempDir()
	entry := entrySize(orders0)
	s := openStore(t, dir, Options{CompactAfterBytes: 10 * entry})

	for i := range 100 {
		mustCommit(t, s, orders0, int64(i))
	}
	if got := logSize(t, dir); got > 10*entry {
		t.Fatalf("log size %d never compacted below threshold %d", got, 10*entry)
	}
	wantOffset(t, s, orders0, 99)
}

func TestConcurrentCommits(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{CompactAfterBytes: 1024})

	const writers, commits = 8, 50
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			k := Key{Group: fmt.Sprintf("g%d", w), Topic: "orders", Partition: 0}
			for i := range commits {
				if err := s.Commit(k, int64(i)); err != nil {
					t.Error(err)
					return
				}
				s.Get(k)
			}
		})
	}
	wg.Wait()
	s.Close()

	s = openStore(t, dir, Options{})
	for w := range writers {
		wantOffset(t, s, Key{Group: fmt.Sprintf("g%d", w), Topic: "orders", Partition: 0}, commits-1)
	}
}

func TestClosedStoreRejectsCommit(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	s.Close()
	if err := s.Commit(orders0, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Commit after Close = %v, want ErrClosed", err)
	}
}

// TestFailedAutoCompactionDoesNotFailCommit: the commit is durable before
// compaction runs, so a compaction error must not be reported as a failed
// commit. Compaction backs off and works again once the problem is gone.
func TestFailedAutoCompactionDoesNotFailCommit(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, logName+".tmp")
	// A non-empty directory in the way: compaction can neither create its
	// tmp file nor clean it up.
	if err := os.MkdirAll(filepath.Join(tmp, "blocker"), 0o755); err != nil {
		t.Fatal(err)
	}
	entry := entrySize(orders0)
	s := openStore(t, dir, Options{CompactAfterBytes: 4 * entry})

	for i := range 10 {
		mustCommit(t, s, orders0, int64(i))
	}
	if s.retryCompactionAt == 0 {
		t.Fatal("expected a failed compaction to set a retry size")
	}
	sizeAfterFailure := logSize(t, dir)
	if sizeAfterFailure != 10*entry {
		t.Fatalf("log size %d, want %d (nothing compacted)", sizeAfterFailure, 10*entry)
	}

	if err := os.RemoveAll(tmp); err != nil {
		t.Fatal(err)
	}
	for i := 10; i < 30; i++ {
		mustCommit(t, s, orders0, int64(i))
	}
	if got := logSize(t, dir); got >= sizeAfterFailure {
		t.Fatalf("log size %d: compaction never retried after the failure cleared", got)
	}
	s.Close()
	wantOffset(t, openStore(t, dir, Options{}), orders0, 29)
}

// TestUnrepairableAppendStopsWrites: if a failed append cannot be cut back
// off the file, later commits would sit behind a partial entry and be lost
// on recovery, so the store refuses further writes.
func TestUnrepairableAppendStopsWrites(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{})
	mustCommit(t, s, orders0, 1)

	// A read-only handle makes both the write and the truncate fail.
	s.file.Close()
	ro, err := os.Open(filepath.Join(dir, logName))
	if err != nil {
		t.Fatal(err)
	}
	s.file = ro

	first := s.Commit(orders0, 2)
	if first == nil {
		t.Fatal("Commit on a broken file succeeded")
	}
	if err := s.Commit(orders0, 3); err != first {
		t.Fatalf("second Commit = %v, want the sticky error %v", err, first)
	}
	if err := s.Compact(); err != first {
		t.Fatalf("Compact = %v, want the sticky error %v", err, first)
	}
}
