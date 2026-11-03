// Package fsutil holds the small durability helpers every storage package
// needs. A file's contents survive a crash only after fsync on the file; a
// file's name (create, rename, unlink) survives only after fsync on its
// directory.
package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// SyncDir fsyncs a directory so entries created, renamed or removed in it
// survive a crash.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for fsync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	return nil
}

// MkdirAll creates dir (and any missing parents), then fsyncs dir's parent
// so the new directory entry survives a crash.
func MkdirAll(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create dir: %w", err)
	}
	return SyncDir(filepath.Dir(dir))
}

// WriteFileAtomic replaces path with data so that a crash leaves either the
// old or the new contents, never a mix: write a temp file, fsync it, rename
// it over path, fsync the directory.
func WriteFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := writeFileSync(tmp, data); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename %s: %w", tmp, err)
	}
	return SyncDir(filepath.Dir(path))
}

func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
