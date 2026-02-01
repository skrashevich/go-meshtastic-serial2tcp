//go:build windows

package serial

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestNormalizeWindowsDevice(t *testing.T) {
	tests := []struct {
		input    string
		wantPath string
		wantName string
	}{
		{input: "COM3", wantPath: `\\.\COM3`, wantName: "COM3"},
		{input: "com4", wantPath: `\\.\com4`, wantName: "com4"},
		{input: "COM5:", wantPath: `\\.\COM5`, wantName: "COM5"},
		{input: `\\.\COM7`, wantPath: `\\.\COM7`, wantName: "COM7"},
		{input: `C:\temp\serial`, wantPath: `C:\temp\serial`, wantName: `C:\temp\serial`},
	}

	for _, test := range tests {
		path, name := normalizeWindowsDevice(test.input)
		if path != test.wantPath || name != test.wantName {
			t.Fatalf("normalizeWindowsDevice(%q) = (%q, %q), want (%q, %q)", test.input, path, name, test.wantPath, test.wantName)
		}
	}
}

func TestIsCOMDevice(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{input: "COM1", want: true},
		{input: "com12", want: true},
		{input: "COM", want: false},
		{input: "COMA", want: false},
		{input: "LPT1", want: false},
	}

	for _, test := range tests {
		if got := isCOMDevice(test.input); got != test.want {
			t.Fatalf("isCOMDevice(%q) = %v, want %v", test.input, got, test.want)
		}
	}
}

func TestDeviceExistsWindowsFile(t *testing.T) {
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

func TestDeviceExistsWindowsCOMUsesQuery(t *testing.T) {
	origQuery := queryDosDeviceFn
	defer func() {
		queryDosDeviceFn = origQuery
	}()

	callCount := 0
	queryDosDeviceFn = func(_ *uint16, _ *uint16, _ uint32) (uint32, error) {
		callCount++
		return 8, nil
	}

	if !DeviceExists("COM9") {
		t.Fatalf("expected COM device to exist via QueryDosDevice")
	}
	if callCount == 0 {
		t.Fatalf("expected QueryDosDevice to be called")
	}
}

func TestDeviceExistsWindowsCOMRetriesOnBuffer(t *testing.T) {
	origQuery := queryDosDeviceFn
	defer func() {
		queryDosDeviceFn = origQuery
	}()

	callCount := 0
	queryDosDeviceFn = func(_ *uint16, _ *uint16, _ uint32) (uint32, error) {
		callCount++
		if callCount == 1 {
			return 0, windows.ERROR_INSUFFICIENT_BUFFER
		}
		return 12, nil
	}

	if !DeviceExists("COM10") {
		t.Fatalf("expected COM device to exist after buffer retry")
	}
	if callCount < 2 {
		t.Fatalf("expected QueryDosDevice to be retried, got %d calls", callCount)
	}
}
