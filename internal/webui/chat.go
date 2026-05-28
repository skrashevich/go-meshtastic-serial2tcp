package webui

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
)

const defaultMaxChat = 500

// ChatMessage is a user-visible mesh text message.
type ChatMessage struct {
	Time        time.Time `json:"time"`
	Text        string    `json:"text"`
	From        uint32    `json:"from"`
	To          uint32    `json:"to"`
	Channel     int32     `json:"channel"`
	ChannelName string    `json:"channel_name,omitempty"`
	Outgoing    bool      `json:"outgoing"`
	PacketID    uint32    `json:"packet_id,omitempty"`
	ProtoJSON   string    `json:"proto_json,omitempty"`
}

// Hub chat storage and radio access.
func (h *Hub) SetRadioProvider(fn func() Radio) {
	h.mu.Lock()
	h.radioProvider = fn
	h.mu.Unlock()
}

func (h *Hub) radio() Radio {
	h.mu.RLock()
	fn := h.radioProvider
	h.mu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn()
}

func (h *Hub) SendChat(channel int32, to uint32, text string) (ChatMessage, error) {
	r := h.radio()
	if r == nil {
		return ChatMessage{}, fmt.Errorf("radio not connected")
	}
	text = strings.TrimSpace(text)
	pktID, err := r.SendTextMessage(channel, to, text)
	if err != nil {
		return ChatMessage{}, err
	}
	from, _ := r.LocalNodeNum()
	msg := ChatMessage{
		Time:      time.Now().UTC(),
		Text:      text,
		From:      from,
		To:        to,
		Channel:   channel,
		Outgoing:  true,
		PacketID:  pktID,
		ProtoJSON: chatMeshPacketJSON(from, to, channel, pktID, text),
	}
	h.mu.RLock()
	if ch, ok := h.channels[channel]; ok {
		msg.ChannelName = ch.Name
	}
	h.mu.RUnlock()
	h.RecordChat(msg)
	return msg, nil
}

func (h *Hub) RecordChat(msg ChatMessage) {
	if msg.Time.IsZero() {
		msg.Time = time.Now().UTC()
	}
	if msg.ChannelName == "" {
		h.mu.RLock()
		if ch, ok := h.channels[msg.Channel]; ok {
			msg.ChannelName = ch.Name
		}
		h.mu.RUnlock()
	}

	if h.isDuplicateChat(msg) {
		return
	}

	h.mu.Lock()
	h.chats = append(h.chats, msg)
	if len(h.chats) > h.maxChat {
		h.chats = h.chats[len(h.chats)-h.maxChat:]
	}
	subs := make([]chan ChatMessage, 0, len(h.chatSubs))
	for ch := range h.chatSubs {
		subs = append(subs, ch)
	}
	h.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- msg:
		default:
		}
	}
}

func (h *Hub) SnapshotChat() []ChatMessage {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]ChatMessage, len(h.chats))
	copy(out, h.chats)
	return out
}

func (h *Hub) SubscribeChat(buffer int) (<-chan ChatMessage, func()) {
	if buffer <= 0 {
		buffer = 64
	}
	ch := make(chan ChatMessage, buffer)
	h.mu.Lock()
	h.chatSubs[ch] = struct{}{}
	h.mu.Unlock()
	unsub := func() {
		h.mu.Lock()
		delete(h.chatSubs, ch)
		h.mu.Unlock()
		close(ch)
	}
	return ch, unsub
}

func (h *Hub) TryRecordChatFromPacket(pkt *meshtasticpb.MeshPacket, localNode uint32) {
	if pkt == nil {
		return
	}
	data := pkt.GetDecoded()
	if data == nil || !isChatPort(data.GetPortnum()) {
		return
	}
	text := chatTextFromData(data)
	if text == "" {
		return
	}
	outgoing := localNode != 0 && pkt.GetFrom() == localNode
	msg := ChatMessage{
		Time:      time.Now().UTC(),
		Text:      text,
		From:      pkt.GetFrom(),
		To:        pkt.GetTo(),
		Channel:   int32(pkt.GetChannel()),
		Outgoing:  outgoing,
		PacketID:  pkt.GetId(),
		ProtoJSON: marshalProtoJSONFull(pkt),
	}
	h.RecordChat(msg)
}

func chatMeshPacketJSON(from, to uint32, channel int32, id uint32, text string) string {
	pkt := &meshtasticpb.MeshPacket{
		From:    from,
		To:      to,
		Channel: uint32(channel),
		Id:      id,
		PayloadVariant: &meshtasticpb.MeshPacket_Decoded{
			Decoded: &meshtasticpb.Data{
				Portnum: meshtasticpb.PortNum_TEXT_MESSAGE_APP,
				Payload: []byte(text),
			},
		},
	}
	return marshalProtoJSONFull(pkt)
}

func chatTextFromData(data *meshtasticpb.Data) string {
	payload := data.GetPayload()
	if len(payload) == 0 {
		return ""
	}
	switch data.GetPortnum() {
	case meshtasticpb.PortNum_TEXT_MESSAGE_APP,
		meshtasticpb.PortNum_REPLY_APP,
		meshtasticpb.PortNum_DETECTION_SENSOR_APP,
		meshtasticpb.PortNum_ALERT_APP:
		if isUTF8(payload) {
			return string(payload)
		}
	}
	return ""
}

func isChatPort(p meshtasticpb.PortNum) bool {
	switch p {
	case meshtasticpb.PortNum_TEXT_MESSAGE_APP,
		meshtasticpb.PortNum_REPLY_APP,
		meshtasticpb.PortNum_DETECTION_SENSOR_APP,
		meshtasticpb.PortNum_ALERT_APP:
		return true
	default:
		return false
	}
}

func (h *Hub) MarshalChat(msg ChatMessage) ([]byte, error) {
	return json.Marshal(msg)
}

func (h *Hub) isDuplicateChat(msg ChatMessage) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for i := len(h.chats) - 1; i >= 0 && i >= len(h.chats)-8; i-- {
		prev := h.chats[i]
		if msg.PacketID != 0 && prev.PacketID == msg.PacketID {
			return true
		}
		if prev.Outgoing && msg.Outgoing && prev.Text == msg.Text && prev.Channel == msg.Channel &&
			msg.Time.Sub(prev.Time) < 2*time.Second {
			return true
		}
	}
	return false
}

func formatNodeID(n uint32) string {
	if n == BroadcastNode {
		return "broadcast"
	}
	return fmt.Sprintf("!%08x", n)
}

// BroadcastNode matches meshtastic broadcast destination.
const BroadcastNode = 0xFFFFFFFF
