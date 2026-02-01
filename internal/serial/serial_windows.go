//go:build windows

package serial

import (
	"errors"
	"os"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/termios"
	"golang.org/x/sys/windows"
)

func DisableHUPCL(device string) error {
	return nil
}

var (
	createFile  = windows.CreateFile
	closeHandle = windows.CloseHandle
)

func Open(device string, baud int) (*os.File, error) {
	path, _ := normalizeWindowsDevice(device)
	handle, err := createFile(
		windows.StringToUTF16Ptr(path),
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, err
	}

	fd := int(handle)
	termiosState, err := termios.GetTermios(fd)
	if err != nil {
		closeHandle(handle)
		return nil, err
	}

	termios.SetRawMode(termiosState)
	if err := termios.SetBaudRate(termiosState, baud); err != nil {
		closeHandle(handle)
		return nil, err
	}

	if err := termios.SetTermios(fd, termiosState); err != nil {
		closeHandle(handle)
		return nil, err
	}

	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		closeHandle(handle)
		return nil, errors.New("failed to open serial device")
	}

	return file, nil
}
