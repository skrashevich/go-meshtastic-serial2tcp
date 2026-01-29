//go:build windows

package termios

import (
	"testing"

	"golang.org/x/sys/windows"
)

func TestSetBaudRateWindows(t *testing.T) {
	term := &Termios{}
	if err := SetBaudRate(term, 115200); err != nil {
		t.Fatalf("SetBaudRate error: %v", err)
	}
	if term.DCB.BaudRate != windows.CBR_115200 {
		t.Fatalf("unexpected baud rate: %v", term.DCB.BaudRate)
	}

	if err := SetBaudRate(term, 12345); err == nil {
		t.Fatalf("expected error for unsupported baud rate")
	}
}

func TestSetRawModeWindows(t *testing.T) {
	term := &Termios{
		DCB: windows.DCB{
			Flags:    dcbParity | dcbOutX | dcbInX | dcbAbortOnError,
			ByteSize: 7,
			Parity:   1,
			StopBits: 2,
		},
		Timeouts: windows.CommTimeouts{
			ReadIntervalTimeout: 1,
		},
	}

	SetRawMode(term)

	flags := term.DCB.Flags
	if flags&dcbBinary == 0 {
		t.Fatalf("expected dcbBinary to be set")
	}
	if flags&dcbTxContinueOnXoff == 0 {
		t.Fatalf("expected dcbTxContinueOnXoff to be set")
	}
	if flags&(dcbParity|dcbOutX|dcbInX|dcbAbortOnError) != 0 {
		t.Fatalf("expected parity/flow/abort flags to be cleared")
	}
	if flags&dcbDtrControlMask != dcbDtrControlEnable {
		t.Fatalf("expected DTR control enable to be set")
	}
	if flags&dcbRtsControlMask != dcbRtsControlEnable {
		t.Fatalf("expected RTS control enable to be set")
	}
	if term.DCB.ByteSize != 8 {
		t.Fatalf("expected ByteSize 8, got %v", term.DCB.ByteSize)
	}
	if term.DCB.Parity != noParity {
		t.Fatalf("expected Parity %v, got %v", noParity, term.DCB.Parity)
	}
	if term.DCB.StopBits != oneStopBit {
		t.Fatalf("expected StopBits %v, got %v", oneStopBit, term.DCB.StopBits)
	}
	if term.Timeouts != (windows.CommTimeouts{}) {
		t.Fatalf("expected zeroed timeouts, got %+v", term.Timeouts)
	}
}
