package webui

import (
	"context"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
)

func TestHandleSnapshot(t *testing.T) {
	h := NewHub()
	h.UpdateChannel(1, "test", "SECONDARY")
	h.UpdateNodeFromInfo(&meshtasticpb.NodeInfo{
		Num:  0x1234,
		User: &meshtasticpb.User{LongName: "Node A"},
	})
	s := NewServer(h, "127.0.0.1:0")

	req := httptest.NewRequest(http.MethodGet, "/api/snapshot", nil)
	rec := httptest.NewRecorder()
	s.handleSnapshot(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"channels"`) || !strings.Contains(body, "test") {
		t.Fatalf("unexpected body: %s", body)
	}
	if !strings.Contains(body, `"nodes"`) || !strings.Contains(body, "Node A") {
		t.Fatalf("snapshot missing nodes: %s", body)
	}
}

func TestHandleEventsSnapshotFirst(t *testing.T) {
	h := NewHub()
	h.Record(Event{Category: "debug", Direction: "boot"})
	s := NewServer(h, "127.0.0.1:0")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/events", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	go s.handleEvents(rec, req)

	deadline := time.Now().Add(time.Second)
	for rec.Body.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: snapshot") {
		t.Fatalf("expected snapshot event, got: %q", body)
	}
}

func TestStaticIndex(t *testing.T) {
	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		t.Fatal(err)
	}
	f, err := static.Open("index.html")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "Serial Bridge") {
		t.Fatal("index.html missing title")
	}
	if !strings.Contains(string(data), "panel-nodes") {
		t.Fatal("index.html missing nodes panel")
	}
}
