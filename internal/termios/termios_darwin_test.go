//go:build darwin

package termios

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestSetBaudRateDarwin(t *testing.T) {
	term := &unix.Termios{
		Cflag: 0xdeadbeef,
	}

	if err := SetBaudRate(term, 115200); err != nil {
		t.Fatalf("SetBaudRate error: %v", err)
	}

	rate := baudrateMap[115200]
	if term.Ispeed != rate || term.Ospeed != rate {
		t.Fatalf("unexpected speeds: ispeed=%v ospeed=%v want=%v", term.Ispeed, term.Ospeed, rate)
	}
	if term.Cflag != 0xdeadbeef {
		t.Fatalf("expected Cflag to be unchanged on darwin")
	}

	if err := SetBaudRate(term, 12345); err == nil {
		t.Fatalf("expected error for unsupported baud rate")
	}
}

func TestSetRawModeDarwin(t *testing.T) {
	term := &unix.Termios{
		Iflag: unix.IGNBRK | unix.BRKINT | unix.IXON | unix.IGNPAR,
		Oflag: unix.OPOST,
		Lflag: unix.ECHO | unix.ICANON | unix.ISIG | unix.IEXTEN,
		Cflag: unix.CSIZE | unix.PARENB | unix.CSTOPB | unix.CRTSCTS,
	}

	SetRawMode(term)

	if term.Iflag&(unix.IGNBRK|unix.BRKINT|unix.IXON|unix.IGNPAR) != 0 {
		t.Fatalf("expected input flags to be cleared")
	}
	if term.Oflag&unix.OPOST != 0 {
		t.Fatalf("expected output flags to be cleared")
	}
	if term.Lflag&(unix.ECHO|unix.ICANON|unix.ISIG|unix.IEXTEN) != 0 {
		t.Fatalf("expected local flags to be cleared")
	}
	if term.Cflag&unix.CSIZE != unix.CS8 {
		t.Fatalf("expected Cflag CS8 set, got %v", term.Cflag&unix.CSIZE)
	}
	if term.Cflag&(unix.PARENB|unix.CSTOPB|unix.CRTSCTS) != 0 {
		t.Fatalf("expected parity/stop/flow flags to be cleared")
	}
	if term.Cflag&(unix.CLOCAL|unix.CREAD) != (unix.CLOCAL | unix.CREAD) {
		t.Fatalf("expected CLOCAL|CREAD to be set")
	}
	if term.Cc[unix.VMIN] != 1 || term.Cc[unix.VTIME] != 0 {
		t.Fatalf("unexpected VMIN/VTIME: %v/%v", term.Cc[unix.VMIN], term.Cc[unix.VTIME])
	}
}
