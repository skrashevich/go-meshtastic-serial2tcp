//go:build windows

package serial

import (
	"errors"
	"testing"

	"golang.org/x/sys/windows"
)

func TestOpenSerialUsesNormalizedPath(t *testing.T) {
	origCreateFile := createFile
	origCloseHandle := closeHandle
	defer func() {
		createFile = origCreateFile
		closeHandle = origCloseHandle
	}()

	sentinel := errors.New("create failed")
	createFile = func(path *uint16, access uint32, mode uint32, sa *windows.SecurityAttributes, creation uint32, attrs uint32, template windows.Handle) (windows.Handle, error) {
		got := windows.UTF16PtrToString(path)
		if got != `\\.\COM3` {
			t.Fatalf("unexpected path: %q", got)
		}
		return 0, sentinel
	}
	closeHandle = func(handle windows.Handle) error {
		return nil
	}

	_, err := Open("COM3", 115200)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected createFile error, got %v", err)
	}
}

func TestDisableHUPCLWindowsNoop(t *testing.T) {
	if err := DisableHUPCL("COM1"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
