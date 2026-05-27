package webui

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"sort"
	"time"
)

//go:embed static/*
var staticFS embed.FS

// Server serves the embedded UI and JSON/SSE APIs.
type Server struct {
	hub    *Hub
	addr   string
	server *http.Server
}

func NewServer(hub *Hub, addr string) *Server {
	return &Server{hub: hub, addr: addr}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		return err
	}
	fileServer := http.FileServer(http.FS(static))
	mux.Handle("/", fileServer)
	mux.HandleFunc("/api/snapshot", s.handleSnapshot)
	mux.HandleFunc("/api/events", s.handleEvents)
	mux.HandleFunc("/api/chat/send", s.handleChatSend)

	s.server = &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.server.Shutdown(shutdownCtx)
	}()

	log.Printf("Web UI listening on http://%s", s.addr)
	err = s.server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	events, channels, status := s.hub.Snapshot()
	sort.Slice(channels, func(i, j int) bool { return channels[i].Index < channels[j].Index })
	writeJSON(w, map[string]any{
		"events":   events,
		"channels": channels,
		"status":   status,
		"chats":    s.hub.SnapshotChat(),
	})
}

type chatSendRequest struct {
	Channel int32  `json:"channel"`
	To      uint32 `json:"to"`
	Text    string `json:"text"`
}

func (s *Server) handleChatSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req chatSendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.To == 0 {
		req.To = BroadcastNode
	}
	msg, err := s.hub.SendChat(req.Channel, req.To, req.Text)
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true, "message": msg})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	events, channels, status := s.hub.Snapshot()
	sort.Slice(channels, func(i, j int) bool { return channels[i].Index < channels[j].Index })
	if data, err := json.Marshal(map[string]any{
		"type":     "snapshot",
		"events":   events,
		"channels": channels,
		"status":   status,
		"chats":    s.hub.SnapshotChat(),
	}); err == nil {
		fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", data)
		flusher.Flush()
	}

	evCh, evUnsub := s.hub.Subscribe(128)
	defer evUnsub()
	chatCh, chatUnsub := s.hub.SubscribeChat(64)
	defer chatUnsub()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-evCh:
			if !ok {
				return
			}
			data, err := s.hub.MarshalEvent(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
			flusher.Flush()
		case msg, ok := <-chatCh:
			if !ok {
				return
			}
			data, err := s.hub.MarshalChat(msg)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: chat\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}
