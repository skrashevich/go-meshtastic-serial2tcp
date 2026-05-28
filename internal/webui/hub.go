package webui

import (
	"encoding/json"
	"sync"
	"time"
)

const (
	defaultMaxEvents      = 1000
	maxPacketProtoCache   = 2048
)

// Event is a single observability record for the web UI.
type Event struct {
	Time         time.Time `json:"time"`
	Direction    string    `json:"direction"`
	Addr         string    `json:"addr,omitempty"`
	Variant      string    `json:"variant,omitempty"`
	Category     string    `json:"category"`
	ChannelIndex *int32    `json:"channel_index,omitempty"`
	ChannelName  string    `json:"channel_name,omitempty"`
	Summary      string    `json:"summary,omitempty"`
	JSON         string    `json:"json,omitempty"`
}

// ChannelInfo is the latest known channel definition from the radio.
type ChannelInfo struct {
	Index    int32  `json:"index"`
	Name     string `json:"name,omitempty"`
	Role     string `json:"role,omitempty"`
	Updated  time.Time `json:"updated"`
}

// Status is a snapshot of bridge health for the UI.
type Status struct {
	SerialConnected bool   `json:"serial_connected"`
	ClientCount     int    `json:"client_count"`
	PrimaryAddr     string `json:"primary_addr,omitempty"`
	CacheReady      bool   `json:"cache_ready"`
	CacheSummary    string `json:"cache_summary,omitempty"`
	LocalNodeNum    uint32 `json:"local_node_num,omitempty"`
}

// Hub stores live state and broadcasts events to SSE subscribers.
type Hub struct {
	mu            sync.RWMutex
	events        []Event
	maxEvents     int
	chats         []ChatMessage
	maxChat       int
	packetProtos  map[uint32]string
	channels      map[int32]ChannelInfo
	status        Status
	subscribers   map[chan Event]struct{}
	chatSubs      map[chan ChatMessage]struct{}
	radioProvider func() Radio
}

func NewHub() *Hub {
	return &Hub{
		maxEvents:   defaultMaxEvents,
		maxChat:     defaultMaxChat,
		channels:     make(map[int32]ChannelInfo),
		packetProtos: make(map[uint32]string),
		subscribers:  make(map[chan Event]struct{}),
		chatSubs:    make(map[chan ChatMessage]struct{}),
	}
}

func (h *Hub) Record(ev Event) {
	if ev.Time.IsZero() {
		ev.Time = time.Now().UTC()
	}
	if ev.ChannelIndex != nil && ev.ChannelName == "" {
		h.mu.RLock()
		if ch, ok := h.channels[*ev.ChannelIndex]; ok {
			ev.ChannelName = ch.Name
		}
		h.mu.RUnlock()
	}

	h.mu.Lock()
	h.events = append(h.events, ev)
	if len(h.events) > h.maxEvents {
		h.events = h.events[len(h.events)-h.maxEvents:]
	}
	subs := make([]chan Event, 0, len(h.subscribers))
	for ch := range h.subscribers {
		subs = append(subs, ch)
	}
	h.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

func (h *Hub) UpdateChannel(index int32, name, role string) {
	h.mu.Lock()
	h.channels[index] = ChannelInfo{
		Index:   index,
		Name:    name,
		Role:    role,
		Updated: time.Now().UTC(),
	}
	h.mu.Unlock()
}

func (h *Hub) SetStatus(st Status) {
	h.mu.Lock()
	h.status = st
	h.mu.Unlock()
}

func (h *Hub) Snapshot() (events []Event, channels []ChannelInfo, status Status) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	events = make([]Event, len(h.events))
	copy(events, h.events)

	channels = make([]ChannelInfo, 0, len(h.channels))
	for _, ch := range h.channels {
		channels = append(channels, ch)
	}
	status = h.status
	return events, channels, status
}

func (h *Hub) Subscribe(buffer int) (<-chan Event, func()) {
	if buffer <= 0 {
		buffer = 64
	}
	ch := make(chan Event, buffer)
	h.mu.Lock()
	h.subscribers[ch] = struct{}{}
	h.mu.Unlock()

	unsub := func() {
		h.mu.Lock()
		delete(h.subscribers, ch)
		h.mu.Unlock()
		close(ch)
	}
	return ch, unsub
}

func (h *Hub) MarshalEvent(ev Event) ([]byte, error) {
	return json.Marshal(ev)
}
