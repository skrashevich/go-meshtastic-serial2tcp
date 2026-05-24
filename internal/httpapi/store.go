package httpapi

import (
	"sync"
	"time"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
)

const defaultStoreCapacity = 512

type Message struct {
	ID        uint32 `json:"id"`
	From      uint32 `json:"from"`
	FromHex   string `json:"from_hex"`
	To        uint32 `json:"to"`
	Channel   uint32 `json:"channel"`
	Text      string `json:"text,omitempty"`
	Portnum   string `json:"portnum"`
	Timestamp int64  `json:"timestamp"`
	RxSnr     float32 `json:"rx_snr,omitempty"`
}

type Store struct {
	mu       sync.RWMutex
	capacity int
	items    []Message
}

func NewStore(capacity int) *Store {
	if capacity <= 0 {
		capacity = defaultStoreCapacity
	}
	return &Store{capacity: capacity}
}

func (s *Store) Add(msg Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, msg)
	if len(s.items) > s.capacity {
		s.items = append([]Message(nil), s.items[len(s.items)-s.capacity:]...)
	}
}

func (s *Store) List(channelHash *byte, since int64, limit int) []Message {
	if limit <= 0 {
		limit = 50
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Message, 0, limit)
	for i := len(s.items) - 1; i >= 0 && len(out) < limit; i-- {
		item := s.items[i]
		if since > 0 && item.Timestamp < since {
			continue
		}
		if channelHash != nil && byte(item.Channel) != *channelHash {
			continue
		}
		out = append(out, item)
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func messageFromPacket(packet *meshtasticpb.MeshPacket, data *meshtasticpb.Data) (Message, bool) {
	if packet == nil || data == nil {
		return Message{}, false
	}
	if data.GetPortnum() != meshtasticpb.PortNum_TEXT_MESSAGE_APP {
		return Message{}, false
	}
	text := string(data.GetPayload())
	if text == "" {
		return Message{}, false
	}
	ts := int64(packet.GetRxTime())
	if ts == 0 {
		ts = time.Now().Unix()
	}
	return Message{
		ID:        packet.GetId(),
		From:      packet.GetFrom(),
		FromHex:   formatNodeID(packet.GetFrom()),
		To:        packet.GetTo(),
		Channel:   packet.GetChannel(),
		Text:      text,
		Portnum:   data.GetPortnum().String(),
		Timestamp: ts,
		RxSnr:     packet.GetRxSnr(),
	}, true
}

func formatNodeID(id uint32) string {
	return "!" + formatHex(id)
}

func formatHex(id uint32) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		out[i] = hexdigits[id&0xf]
		id >>= 4
	}
	return string(out)
}
