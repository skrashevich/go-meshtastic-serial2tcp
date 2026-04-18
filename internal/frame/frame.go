package frame

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	Magic0  = 0x94
	Magic1  = 0xC3
	MaxSize = 64*1024 - 1
)

// ErrInvalidFrame signals a recoverable parse error: magic bytes were found
// but the length field is out of range. The reader may skip and resync on
// the next magic byte pair instead of closing the underlying connection.
var ErrInvalidFrame = errors.New("invalid frame")

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
			return nil, fmt.Errorf("%w: length=%d", ErrInvalidFrame, length)
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
