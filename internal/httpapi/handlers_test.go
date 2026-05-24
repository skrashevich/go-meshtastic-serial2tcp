package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/broker"
)

func TestHandleMessagesWithoutPSK(t *testing.T) {
	srv := NewServer(func() *broker.Broker { return nil }, "", "LongFast")
	srv.store.Add(Message{ID: 1, From: 42, Text: "hello", Portnum: "TEXT_MESSAGE_APP", Timestamp: 100})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/messages", nil)
	rec := httptest.NewRecorder()
	srv.handleMessages(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"hello"`) {
		t.Fatalf("body: %s", rec.Body.String())
	}
}

func TestHandleFromRadioNoBroker(t *testing.T) {
	srv := NewServer(func() *broker.Broker { return nil }, "", "LongFast")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/fromradio", nil)
	rec := httptest.NewRecorder()
	srv.handleFromRadio(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d want 503", rec.Code)
	}
}
