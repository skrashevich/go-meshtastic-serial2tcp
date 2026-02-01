//go:build !windows

package serial

import (
	"errors"
	"log"
	"os"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/termios"
	"golang.org/x/sys/unix"
)

func DisableHUPCL(device string) error {
	fd, err := unix.Open(device, unix.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)

	termiosState, err := termios.GetTermios(fd)
	if err != nil {
		return err
	}

	termiosState.Cflag &^= unix.HUPCL
	if err := termios.SetTermios(fd, termiosState); err != nil {
		return err
	}

	log.Printf("HUPCL disabled")
	return nil
}

func Open(device string, baud int) (*os.File, error) {
	fd, err := unix.Open(device, unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}

	if err := unix.SetNonblock(fd, false); err != nil {
		unix.Close(fd)
		return nil, err
	}

	termiosState, err := termios.GetTermios(fd)
	if err != nil {
		unix.Close(fd)
		return nil, err
	}

	termios.SetRawMode(termiosState)
	termiosState.Cflag |= unix.CLOCAL | unix.CREAD
	termiosState.Cflag &^= unix.HUPCL

	if err := termios.SetBaudRate(termiosState, baud); err != nil {
		unix.Close(fd)
		return nil, err
	}

	if err := termios.SetTermios(fd, termiosState); err != nil {
		unix.Close(fd)
		return nil, err
	}

	file := os.NewFile(uintptr(fd), device)
	if file == nil {
		unix.Close(fd)
		return nil, errors.New("failed to open serial device")
	}

	return file, nil
}
