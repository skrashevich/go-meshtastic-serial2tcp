package frame

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

type zeroWriter struct{}

func (zeroWriter) Write(p []byte) (int, error) {
	return 0, nil
}

func TestReadWriteFrameRoundTrip(t *testing.T) {
	payload := []byte("hello world")
	var buf bytes.Buffer
	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatalf("WriteFrame error: %v", err)
	}

	reader := bufio.NewReader(&buf)
	out, err := ReadFrame(reader)
	if err != nil {
		t.Fatalf("ReadFrame error: %v", err)
	}
	if !bytes.Equal(out, payload) {
		t.Fatalf("payload mismatch: got %q want %q", out, payload)
	}
}

func TestReadFrameSkipsJunk(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x01, 0x02})
	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatalf("WriteFrame error: %v", err)
	}

	out, err := ReadFrame(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("ReadFrame error: %v", err)
	}
	if !bytes.Equal(out, payload) {
		t.Fatalf("payload mismatch: got %v want %v", out, payload)
	}
}

func TestReadFrameInvalidLength(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(Magic0)
	buf.WriteByte(Magic1)
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], 0)
	buf.Write(lenBuf[:])

	_, err := ReadFrame(bufio.NewReader(&buf))
	if err == nil {
		t.Fatal("expected error for invalid length")
	}
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("expected ErrInvalidFrame, got %v", err)
	}
}

func TestWriteFrameTooLarge(t *testing.T) {
	payload := make([]byte, MaxSize+1)
	if err := WriteFrame(io.Discard, payload); err == nil {
		t.Fatal("expected error for oversized frame")
	}
}

func TestWriteAll(t *testing.T) {
	var buf bytes.Buffer
	data := []byte("abc123")
	if err := writeAll(&buf, data); err != nil {
		t.Fatalf("writeAll error: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("writeAll mismatch: got %q want %q", buf.Bytes(), data)
	}
}

func TestWriteAllZeroWrite(t *testing.T) {
	if err := writeAll(zeroWriter{}, []byte("x")); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}
