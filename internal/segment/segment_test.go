package segment

import (
	"fmt"
	"os"
	"testing"

	"messagelog/internal/record"
)

const testIndexInterval = 100

// appendN writes n records with offsets base..base+n-1 and returns each
// record's byte position.
func appendN(t *testing.T, s *Segment, n int) []int64 {
	t.Helper()
	var positions []int64
	for i := 0; i < n; i++ {
		positions = append(positions, s.Size())
		r := record.Record{
			Offset:     s.NextOffset(),
			Timestamp:  int64(1000 + i),
			ProducerID: record.NoProducerID,
			Body:       []byte(fmt.Sprintf("message-%03d", i)),
		}
		if err := s.Append(r); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	return positions
}

func assertLocates(t *testing.T, s *Segment, positions []int64) {
	t.Helper()
	for i, want := range positions {
		got, err := s.Locate(s.Base() + int64(i))
		if err != nil || got != want {
			t.Fatalf("Locate(%d) = %d, %v; want %d", s.Base()+int64(i), got, err, want)
		}
	}
}

func TestAppendLocateAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Create(dir, 50, testIndexInterval)
	if err != nil {
		t.Fatal(err)
	}
	positions := appendN(t, s, 40)
	assertLocates(t, s, positions)
	if len(s.index.entries) == 0 {
		t.Fatal("expected sparse index entries")
	}
	size, entries := s.Size(), len(s.index.entries)
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dir, 50, testIndexInterval)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.NextOffset() != 90 || s.Size() != size || s.LastTimestamp() != 1039 {
		t.Fatalf("reopened: next=%d size=%d ts=%d", s.NextOffset(), s.Size(), s.LastTimestamp())
	}
	if len(s.index.entries) != entries {
		t.Fatalf("reopened index has %d entries, want %d", len(s.index.entries), entries)
	}
	assertLocates(t, s, positions)

	// Appending after reopen keeps the index spacing going.
	positions = append(positions, appendN(t, s, 10)...)
	assertLocates(t, s, positions)
}

func TestRecoverFindsTornTailAndRebuildsIndex(t *testing.T) {
	dir := t.TempDir()
	s, err := Create(dir, 0, testIndexInterval)
	if err != nil {
		t.Fatal(err)
	}
	positions := appendN(t, s, 30)
	goodSize := s.Size()
	// A torn write: half of the next record.
	torn := record.Append(nil, record.Record{Offset: 30, Body: []byte("lost")})
	if _, err := s.log.WriteAt(torn[:len(torn)/2], goodSize); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s, err = Open(dir, 0, testIndexInterval)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var visited []int64
	tornTail, err := s.Recover(0, 0, func(h record.Header) { visited = append(visited, h.Offset) })
	if err != nil || !tornTail {
		t.Fatalf("Recover = %v, %v; want torn", tornTail, err)
	}
	if s.Size() != goodSize || s.NextOffset() != 30 || len(visited) != 30 {
		t.Fatalf("after recover: size=%d next=%d visited=%d", s.Size(), s.NextOffset(), len(visited))
	}
	if info, _ := os.Stat(s.Path()); info.Size() == goodSize {
		t.Fatal("Recover must leave the torn bytes for the caller to decide about")
	}
	if err := s.TruncateTail(); err != nil {
		t.Fatal(err)
	}
	if info, _ := os.Stat(s.Path()); info.Size() != goodSize {
		t.Fatalf("file size after TruncateTail %d, want %d", info.Size(), goodSize)
	}
	assertLocates(t, s, positions)
}

func TestRecoverFromMiddleTrustsPrefix(t *testing.T) {
	dir := t.TempDir()
	s, err := Create(dir, 0, testIndexInterval)
	if err != nil {
		t.Fatal(err)
	}
	positions := appendN(t, s, 20)
	s.Close()

	s, err = Open(dir, 0, testIndexInterval)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var visited []int64
	if _, err := s.Recover(positions[12], 12, func(h record.Header) { visited = append(visited, h.Offset) }); err != nil {
		t.Fatal(err)
	}
	if len(visited) != 8 || visited[0] != 12 {
		t.Fatalf("visited %v, want offsets 12..19", visited)
	}
	if s.NextOffset() != 20 || s.LastTimestamp() != 1019 {
		t.Fatalf("next=%d ts=%d", s.NextOffset(), s.LastTimestamp())
	}
	assertLocates(t, s, positions)
}

func TestOpenIgnoresTornIndexEntry(t *testing.T) {
	dir := t.TempDir()
	s, err := Create(dir, 0, testIndexInterval)
	if err != nil {
		t.Fatal(err)
	}
	positions := appendN(t, s, 30)
	entries := len(s.index.entries)
	// Half an entry, as if the process died mid-write.
	if _, err := s.index.file.WriteAt([]byte{0, 0, 1, 0}, int64(entries)*indexEntrySize); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s, err = Open(dir, 0, testIndexInterval)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if len(s.index.entries) != entries {
		t.Fatalf("index has %d entries, want %d", len(s.index.entries), entries)
	}
	assertLocates(t, s, positions)
}

func TestList(t *testing.T) {
	dir := t.TempDir()
	for _, base := range []int64{200, 0, 37} {
		s, err := Create(dir, base, testIndexInterval)
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	os.WriteFile(dir+"/recovery.checkpoint", nil, 0o644)
	bases, err := List(dir)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(bases) != "[0 37 200]" {
		t.Fatalf("List = %v", bases)
	}
}

func TestReadAfterRemoveReturnsErrClosed(t *testing.T) {
	s, err := Create(t.TempDir(), 0, testIndexInterval)
	if err != nil {
		t.Fatal(err)
	}
	appendN(t, s, 3)
	if err := s.Remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadHeader(0); err != ErrClosed {
		t.Fatalf("ReadHeader after Remove = %v, want ErrClosed", err)
	}
}
