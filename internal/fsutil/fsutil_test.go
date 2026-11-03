package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileAtomicReplacesContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	for _, want := range []string{"first", "second"} {
		if err := WriteFileAtomic(path, []byte(want)); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil || string(got) != want {
			t.Fatalf("read back %q, %v; want %q", got, err, want)
		}
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temp file left behind: %v", err)
	}
}

func TestMkdirAllCreatesNestedDirs(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b")
	if err := MkdirAll(dir); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("stat %s: %v", dir, err)
	}
	if err := MkdirAll(dir); err != nil {
		t.Fatalf("MkdirAll on an existing dir: %v", err)
	}
}

func TestSyncDirMissing(t *testing.T) {
	if err := SyncDir(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("SyncDir on a missing dir succeeded")
	}
}
