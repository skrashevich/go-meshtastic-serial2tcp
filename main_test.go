package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
)

type zeroWriter struct{}

func (zeroWriter) Write(p []byte) (int, error) {
	return 0, nil
}

func TestReadWriteFrameRoundTrip(t *testing.T) {
	payload := []byte("hello world")
	var buf bytes.Buffer
	if err := writeFrame(&buf, payload); err != nil {
		t.Fatalf("writeFrame error: %v", err)
	}

	reader := bufio.NewReader(&buf)
	out, err := readFrame(reader)
	if err != nil {
		t.Fatalf("readFrame error: %v", err)
	}
	if !bytes.Equal(out, payload) {
		t.Fatalf("payload mismatch: got %q want %q", out, payload)
	}
}

func TestReadFrameSkipsJunk(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x01, 0x02})
	if err := writeFrame(&buf, payload); err != nil {
		t.Fatalf("writeFrame error: %v", err)
	}

	out, err := readFrame(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("readFrame error: %v", err)
	}
	if !bytes.Equal(out, payload) {
		t.Fatalf("payload mismatch: got %v want %v", out, payload)
	}
}

func TestReadFrameInvalidLength(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(frameMagic0)
	buf.WriteByte(frameMagic1)
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], 0)
	buf.Write(lenBuf[:])

	_, err := readFrame(bufio.NewReader(&buf))
	if err == nil {
		t.Fatal("expected error for invalid length")
	}
}

func TestWriteFrameTooLarge(t *testing.T) {
	payload := make([]byte, maxFrameSize+1)
	if err := writeFrame(io.Discard, payload); err == nil {
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

func TestEnvHelpers(t *testing.T) {
	t.Setenv("TEST_INT", "42")
	t.Setenv("TEST_INT_BAD", "nope")
	t.Setenv("TEST_BOOL_TRUE", "true")
	t.Setenv("TEST_BOOL_FALSE", "0")
	t.Setenv("TEST_BOOL_BAD", "maybe")

	if got := getenvInt("TEST_INT", 7); got != 42 {
		t.Fatalf("getenvInt valid: got %d want 42", got)
	}
	if got := getenvInt("TEST_INT_BAD", 7); got != 7 {
		t.Fatalf("getenvInt invalid: got %d want 7", got)
	}
	if got := getenvBool("TEST_BOOL_TRUE", false); got != true {
		t.Fatalf("getenvBool true: got %v want true", got)
	}
	if got := getenvBool("TEST_BOOL_FALSE", true); got != false {
		t.Fatalf("getenvBool false: got %v want false", got)
	}
	if got := getenvBool("TEST_BOOL_BAD", true); got != true {
		t.Fatalf("getenvBool invalid: got %v want true", got)
	}
	if got := getenv("TEST_MISSING", "fallback"); got != "fallback" {
		t.Fatalf("getenv fallback: got %q want fallback", got)
	}
}

func TestNormalizePositiveInt(t *testing.T) {
	if got := normalizePositiveInt("x", 3, 7); got != 3 {
		t.Fatalf("normalizePositiveInt valid: got %d want 3", got)
	}
	if got := normalizePositiveInt("x", 0, 7); got != 7 {
		t.Fatalf("normalizePositiveInt invalid: got %d want 7", got)
	}
}

func TestSanitizeDeviceName(t *testing.T) {
	if got := sanitizeDeviceName("/dev/tty.usb0"); got != "_dev_tty_usb0" {
		t.Fatalf("sanitizeDeviceName: got %q", got)
	}
}

func TestFormatPayloads(t *testing.T) {
	if got := formatHexPayload(nil); got != "0x" {
		t.Fatalf("formatHexPayload empty: got %q", got)
	}
	if got := formatHexPayload([]byte{0x01, 0x02}); got != "0x0102" {
		t.Fatalf("formatHexPayload: got %q", got)
	}
	if got := formatTextPayload([]byte("hello")); got != "hello" {
		t.Fatalf("formatTextPayload: got %q", got)
	}
	if got := formatTextPayload([]byte{0xff, 0xfe}); got != "0xfffe" {
		t.Fatalf("formatTextPayload invalid utf8: got %q", got)
	}
}

func TestInjectDecodedPayloadJSON(t *testing.T) {
	base := []byte(`{"packet":{"decoded":{"foo":1}}}`)
	updated, err := injectDecodedPayloadJSON(base, map[string]any{"bar": true})
	if err != nil {
		t.Fatalf("injectDecodedPayloadJSON error: %v", err)
	}
	var root map[string]any
	if err := json.Unmarshal(updated, &root); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	packet := root["packet"].(map[string]any)
	decoded := packet["decoded"].(map[string]any)
	if decoded["payload_decoded"] == nil {
		t.Fatalf("payload_decoded missing")
	}

	_, err = injectDecodedPayloadJSON([]byte("not-json"), "x")
	if err == nil {
		t.Fatalf("expected error for invalid json")
	}
}

func TestConfigCacheUpdateSnapshot(t *testing.T) {
	cache := newConfigCache()
	if !cache.empty() {
		t.Fatalf("expected empty cache")
	}

	payloadA := []byte("A")
	payloadB := []byte("B")
	payloadC := []byte("C")
	payloadD := []byte("D")
	payloadE := []byte("E")
	payloadF := []byte("F")

	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_Metadata{Metadata: &meshtasticpb.DeviceMetadata{}}}, payloadE)
	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_DeviceuiConfig{DeviceuiConfig: &meshtasticpb.DeviceUIConfig{}}}, payloadF)
	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_MyInfo{MyInfo: &meshtasticpb.MyNodeInfo{}}}, payloadA)
	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_NodeInfo{NodeInfo: &meshtasticpb.NodeInfo{Num: 2}}}, payloadB)
	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_NodeInfo{NodeInfo: &meshtasticpb.NodeInfo{Num: 1}}}, payloadC)
	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_Channel{Channel: &meshtasticpb.Channel{Index: 10}}}, []byte("ch10"))
	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_Channel{Channel: &meshtasticpb.Channel{Index: 2}}}, []byte("ch2"))

	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_Config{Config: &meshtasticpb.Config{PayloadVariant: &meshtasticpb.Config_Device{Device: &meshtasticpb.Config_DeviceConfig{}}}}}, payloadD)
	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_Config{Config: &meshtasticpb.Config{PayloadVariant: &meshtasticpb.Config_Display{Display: &meshtasticpb.Config_DisplayConfig{}}}}}, []byte("CFG2"))

	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_ModuleConfig{ModuleConfig: &meshtasticpb.ModuleConfig{PayloadVariant: &meshtasticpb.ModuleConfig_Serial{Serial: &meshtasticpb.ModuleConfig_SerialConfig{}}}}}, []byte("MC2"))
	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_ModuleConfig{ModuleConfig: &meshtasticpb.ModuleConfig{PayloadVariant: &meshtasticpb.ModuleConfig_Mqtt{Mqtt: &meshtasticpb.ModuleConfig_MQTTConfig{}}}}}, []byte("MC1"))

	snap := cache.snapshot()
	if cache.empty() {
		t.Fatalf("expected non-empty cache")
	}
	if !bytes.Equal(snap.myInfo, payloadA) {
		t.Fatalf("myInfo mismatch")
	}
	if !bytes.Equal(snap.metadata, payloadE) {
		t.Fatalf("metadata mismatch")
	}
	if !bytes.Equal(snap.deviceUI, payloadF) {
		t.Fatalf("deviceUI mismatch")
	}

	if len(snap.nodeInfo) != 2 || !bytes.Equal(snap.nodeInfo[0], payloadC) || !bytes.Equal(snap.nodeInfo[1], payloadB) {
		t.Fatalf("nodeInfo ordering mismatch")
	}
	if len(snap.channels) != 2 || string(snap.channels[0]) != "ch2" || string(snap.channels[1]) != "ch10" {
		t.Fatalf("channels ordering mismatch")
	}
	if len(snap.configs) != 2 || string(snap.configs[0]) != "D" || string(snap.configs[1]) != "CFG2" {
		t.Fatalf("config ordering mismatch")
	}
	if len(snap.moduleConfig) != 2 || string(snap.moduleConfig[0]) != "MC1" || string(snap.moduleConfig[1]) != "MC2" {
		t.Fatalf("module config ordering mismatch")
	}
}

func TestSleepWithContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepWithContext(ctx, 50*time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
