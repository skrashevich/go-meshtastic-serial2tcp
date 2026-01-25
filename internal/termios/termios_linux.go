//go:build linux

package termios

import (
	"fmt"

	"golang.org/x/sys/unix"
)

var baudrateMap = map[int]uint32{
	9600:   unix.B9600,
	19200:  unix.B19200,
	38400:  unix.B38400,
	57600:  unix.B57600,
	115200: unix.B115200,
	230400: unix.B230400,
	460800: unix.B460800,
	921600: unix.B921600,
}

func GetTermios(fd int) (*unix.Termios, error) {
	return unix.IoctlGetTermios(fd, unix.TCGETS)
}

func SetTermios(fd int, termios *unix.Termios) error {
	return unix.IoctlSetTermios(fd, unix.TCSETS, termios)
}

func SetBaudRate(termios *unix.Termios, baud int) error {
	rate, ok := baudrateMap[baud]
	if !ok {
		return fmt.Errorf("unsupported baud rate: %d", baud)
	}

	for _, v := range baudrateMap {
		termios.Cflag &^= v
	}
	termios.Cflag |= rate
	termios.Ispeed = rate
	termios.Ospeed = rate
	return nil
}

func SetRawMode(termios *unix.Termios) {
	termios.Iflag &^= unix.IGNBRK | unix.BRKINT | unix.PARMRK | unix.ISTRIP | unix.INLCR | unix.IGNCR | unix.ICRNL | unix.IXON | unix.IXOFF | unix.IXANY | unix.INPCK | unix.IGNPAR
	termios.Oflag &^= unix.OPOST
	termios.Lflag &^= unix.ECHO | unix.ECHONL | unix.ICANON | unix.ISIG | unix.IEXTEN
	termios.Cflag &^= unix.CSIZE | unix.PARENB | unix.CSTOPB | unix.CRTSCTS
	termios.Cflag |= unix.CS8 | unix.CLOCAL | unix.CREAD
	termios.Cc[unix.VMIN] = 1
	termios.Cc[unix.VTIME] = 0
}
