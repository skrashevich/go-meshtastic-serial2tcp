//go:build windows

package termios

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

type Termios struct {
	DCB      windows.DCB
	Timeouts windows.CommTimeouts
}

var (
	getCommState    = windows.GetCommState
	setCommState    = windows.SetCommState
	getCommTimeouts = windows.GetCommTimeouts
	setCommTimeouts = windows.SetCommTimeouts
)

const (
	dcbBinary           = 1 << 0
	dcbParity           = 1 << 1
	dcbOutxCtsFlow      = 1 << 2
	dcbOutxDsrFlow      = 1 << 3
	dcbDtrControlMask   = 0x3 << 4
	dcbDtrControlEnable = 0x1 << 4
	dcbDsrSensitivity   = 1 << 6
	dcbTxContinueOnXoff = 1 << 7
	dcbOutX             = 1 << 8
	dcbInX              = 1 << 9
	dcbErrorChar        = 1 << 10
	dcbNull             = 1 << 11
	dcbRtsControlMask   = 0x3 << 12
	dcbRtsControlEnable = 0x1 << 12
	dcbAbortOnError     = 1 << 14
)

const (
	noParity   = 0
	oneStopBit = 0
)

var baudrateMap = map[int]uint32{
	9600:   windows.CBR_9600,
	19200:  windows.CBR_19200,
	38400:  windows.CBR_38400,
	57600:  windows.CBR_57600,
	115200: windows.CBR_115200,
	230400: 230400,
	460800: 460800,
	921600: 921600,
}

func GetTermios(fd int) (*Termios, error) {
	handle := windows.Handle(fd)
	var dcb windows.DCB
	dcb.DCBlength = uint32(unsafe.Sizeof(dcb))
	if err := getCommState(handle, &dcb); err != nil {
		return nil, err
	}

	var timeouts windows.CommTimeouts
	if err := getCommTimeouts(handle, &timeouts); err != nil {
		return nil, err
	}

	return &Termios{
		DCB:      dcb,
		Timeouts: timeouts,
	}, nil
}

func SetTermios(fd int, termios *Termios) error {
	handle := windows.Handle(fd)
	termios.DCB.DCBlength = uint32(unsafe.Sizeof(termios.DCB))
	if err := setCommState(handle, &termios.DCB); err != nil {
		return err
	}
	if err := setCommTimeouts(handle, &termios.Timeouts); err != nil {
		return err
	}
	return nil
}

func SetBaudRate(termios *Termios, baud int) error {
	rate, ok := baudrateMap[baud]
	if !ok {
		return fmt.Errorf("unsupported baud rate: %d", baud)
	}
	termios.DCB.BaudRate = rate
	return nil
}

func SetRawMode(termios *Termios) {
	dcb := &termios.DCB
	dcb.Flags |= dcbBinary | dcbTxContinueOnXoff
	dcb.Flags &^= dcbParity | dcbOutxCtsFlow | dcbOutxDsrFlow | dcbDsrSensitivity | dcbOutX | dcbInX | dcbErrorChar | dcbNull | dcbAbortOnError
	dcb.Flags &^= dcbDtrControlMask | dcbRtsControlMask
	dcb.Flags |= dcbDtrControlEnable | dcbRtsControlEnable
	dcb.ByteSize = 8
	dcb.Parity = noParity
	dcb.StopBits = oneStopBit
	termios.Timeouts = windows.CommTimeouts{}
}
