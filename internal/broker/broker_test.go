package broker

import (
	"bytes"
	"encoding/json"
	"testing"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
)

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

func TestConfigCacheReady(t *testing.T) {
	cache := newConfigCache()
	if cache.ready() {
		t.Fatalf("expected empty cache to be not ready")
	}

	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_NodeInfo{NodeInfo: &meshtasticpb.NodeInfo{Num: 1}}}, []byte("node"))
	if cache.empty() {
		t.Fatalf("expected nodeInfo-only cache to be non-empty")
	}
	if cache.ready() {
		t.Fatalf("nodeInfo alone must not mark cache ready")
	}

	cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_MyInfo{MyInfo: &meshtasticpb.MyNodeInfo{}}}, []byte("my"))
	if !cache.ready() {
		t.Fatalf("expected cache with myInfo to be ready")
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
