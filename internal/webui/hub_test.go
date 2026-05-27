package webui

import (
	"testing"
	"time"
)

func TestHubRecordAndSnapshot(t *testing.T) {
	h := NewHub()
	h.UpdateChannel(0, "LongFast", "PRIMARY")
	h.Record(Event{
		Category:  "packet",
		Direction: "serial -> broker",
		Summary:   "TEXT_MESSAGE_APP: hi",
	})

	events, channels, _ := h.Snapshot()
	if len(events) != 1 {
		t.Fatalf("events: got %d want 1", len(events))
	}
	if len(channels) != 1 {
		t.Fatalf("channels: got %d want 1", len(channels))
	}
	if channels[0].Name != "LongFast" {
		t.Fatalf("channel name: got %q", channels[0].Name)
	}
}

func TestHubSubscribe(t *testing.T) {
	h := NewHub()
	ch, unsub := h.Subscribe(4)
	defer unsub()

	h.Record(Event{Category: "debug", Direction: "test"})
	select {
	case ev := <-ch:
		if ev.Direction != "test" {
			t.Fatalf("got direction %q", ev.Direction)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}
