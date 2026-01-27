//go:build !windows

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDeviceExistsUnix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "serial")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	if !deviceExists(path) {
		t.Fatalf("expected deviceExists(%q) to be true", path)
	}

	if deviceExists(filepath.Join(dir, "missing")) {
		t.Fatalf("expected deviceExists for missing path to be false")
	}
}
