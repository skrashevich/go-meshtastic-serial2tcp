//go:build !windows

package serial

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

	if !DeviceExists(path) {
		t.Fatalf("expected DeviceExists(%q) to be true", path)
	}

	if DeviceExists(filepath.Join(dir, "missing")) {
		t.Fatalf("expected DeviceExists for missing path to be false")
	}
}
