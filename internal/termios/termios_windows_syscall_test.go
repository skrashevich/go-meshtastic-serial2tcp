//go:build windows

package termios

import (
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestGetTermiosUsesSyscalls(t *testing.T) {
	origGetCommState := getCommState
	origGetCommTimeouts := getCommTimeouts
	defer func() {
		getCommState = origGetCommState
		getCommTimeouts = origGetCommTimeouts
	}()

	getCommState = func(handle windows.Handle, dcb *windows.DCB) error {
		if handle != windows.Handle(123) {
			t.Fatalf("unexpected handle: %v", handle)
		}
		if dcb.DCBlength != uint32(unsafe.Sizeof(*dcb)) {
			t.Fatalf("unexpected DCBlength: %v", dcb.DCBlength)
		}
		dcb.BaudRate = 115200
		dcb.Flags = 0x55
		return nil
	}

	getCommTimeouts = func(handle windows.Handle, timeouts *windows.CommTimeouts) error {
		if handle != windows.Handle(123) {
			t.Fatalf("unexpected handle: %v", handle)
		}
		timeouts.ReadIntervalTimeout = 7
		return nil
	}

	term, err := GetTermios(123)
	if err != nil {
		t.Fatalf("GetTermios error: %v", err)
	}
	if term.DCB.BaudRate != 115200 || term.DCB.Flags != 0x55 {
		t.Fatalf("unexpected DCB: %+v", term.DCB)
	}
	if term.Timeouts.ReadIntervalTimeout != 7 {
		t.Fatalf("unexpected timeouts: %+v", term.Timeouts)
	}
}

func TestSetTermiosUsesSyscalls(t *testing.T) {
	origSetCommState := setCommState
	origSetCommTimeouts := setCommTimeouts
	defer func() {
		setCommState = origSetCommState
		setCommTimeouts = origSetCommTimeouts
	}()

	var gotState windows.DCB
	var gotTimeouts windows.CommTimeouts
	setCommState = func(handle windows.Handle, dcb *windows.DCB) error {
		if handle != windows.Handle(321) {
			t.Fatalf("unexpected handle: %v", handle)
		}
		if dcb.DCBlength != uint32(unsafe.Sizeof(*dcb)) {
			t.Fatalf("unexpected DCBlength: %v", dcb.DCBlength)
		}
		gotState = *dcb
		return nil
	}
	setCommTimeouts = func(handle windows.Handle, timeouts *windows.CommTimeouts) error {
		if handle != windows.Handle(321) {
			t.Fatalf("unexpected handle: %v", handle)
		}
		gotTimeouts = *timeouts
		return nil
	}

	term := &Termios{
		DCB: windows.DCB{
			BaudRate: 57600,
			Flags:    0xAA,
		},
		Timeouts: windows.CommTimeouts{
			ReadIntervalTimeout: 3,
		},
	}

	if err := SetTermios(321, term); err != nil {
		t.Fatalf("SetTermios error: %v", err)
	}

	if gotState.BaudRate != term.DCB.BaudRate || gotState.Flags != term.DCB.Flags {
		t.Fatalf("unexpected DCB in SetCommState: %+v", gotState)
	}
	if gotTimeouts.ReadIntervalTimeout != term.Timeouts.ReadIntervalTimeout {
		t.Fatalf("unexpected timeouts in SetCommTimeouts: %+v", gotTimeouts)
	}
}
