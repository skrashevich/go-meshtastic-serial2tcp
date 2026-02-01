package frame

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	Magic0  = 0x94
	Magic1  = 0xC3
	MaxSize = 64*1024 - 1
)

func ReadFrame(r *bufio.Reader) ([]byte, error) {
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != Magic0 {
			continue
		}

		b, err = r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != Magic1 {
			continue
		}

		var lenBuf [2]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return nil, err
		}

		length := binary.BigEndian.Uint16(lenBuf[:])
		if length == 0 || length > MaxSize {
			lengthLE := binary.LittleEndian.Uint16(lenBuf[:])
			if lengthLE == 0 || lengthLE > MaxSize {
				return nil, fmt.Errorf("invalid frame length: %d", length)
			}
			length = lengthLE
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		return payload, nil
	}
}

func WriteFrame(w io.Writer, payload []byte) error {
	if len(payload) > MaxSize {
		return fmt.Errorf("frame too large: %d", len(payload))
	}

	var header [4]byte
	header[0] = Magic0
	header[1] = Magic1
	binary.BigEndian.PutUint16(header[2:], uint16(len(payload)))

	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, payload)
}

func writeAll(dst io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := dst.Write(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		buf = buf[n:]
	}
	return nil
}
