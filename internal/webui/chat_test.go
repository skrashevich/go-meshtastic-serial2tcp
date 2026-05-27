package webui

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
)

type mockRadio struct {
	sentChannel int32
	sentTo      uint32
	sentText    string
	nodeNum     uint32
}

func (m *mockRadio) SendTextMessage(channelIndex int32, to uint32, text string) (uint32, error) {
	m.sentChannel = channelIndex
	m.sentTo = to
	m.sentText = text
	return 0xBEEF, nil
}

func (m *mockRadio) LocalNodeNum() (uint32, bool) {
	return m.nodeNum, m.nodeNum != 0
}

func TestSendChat(t *testing.T) {
	h := NewHub()
	radio := &mockRadio{nodeNum: 0x11}
	h.SetRadioProvider(func() Radio { return radio })

	msg, err := h.SendChat(0, BroadcastNode, "  hi there  ")
	if err != nil {
		t.Fatal(err)
	}
	if msg.Text != "hi there" {
		t.Fatalf("trimmed text: %q", msg.Text)
	}
	if radio.sentText != "hi there" {
		t.Fatalf("radio got %q", radio.sentText)
	}
	if msg.PacketID != 0xBEEF {
		t.Fatalf("packet id: 0x%x", msg.PacketID)
	}
	chats := h.SnapshotChat()
	if len(chats) != 1 {
		t.Fatalf("chats: %d", len(chats))
	}
}

func TestTryRecordChatFromPacket(t *testing.T) {
	h := NewHub()
	pkt := &meshtasticpb.MeshPacket{
		From:    0x22,
		To:      BroadcastNode,
		Channel: 0,
		Id:      9,
		PayloadVariant: &meshtasticpb.MeshPacket_Decoded{
			Decoded: &meshtasticpb.Data{
				Portnum: meshtasticpb.PortNum_TEXT_MESSAGE_APP,
				Payload: []byte("ping"),
			},
		},
	}
	h.TryRecordChatFromPacket(pkt, 0x11)
	if len(h.SnapshotChat()) != 1 {
		t.Fatal("expected one chat message")
	}
	h.TryRecordChatFromPacket(pkt, 0x11)
	if len(h.SnapshotChat()) != 1 {
		t.Fatal("duplicate packet id should be ignored")
	}
}

func TestHandleChatSend(t *testing.T) {
	h := NewHub()
	radio := &mockRadio{nodeNum: 1}
	h.SetRadioProvider(func() Radio { return radio })
	s := NewServer(h, "127.0.0.1:0")

	body, _ := json.Marshal(map[string]any{"channel": 0, "text": "test"})
	req := httptest.NewRequest(http.MethodPost, "/api/chat/send", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleChatSend(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if radio.sentText != "test" {
		t.Fatalf("radio text %q", radio.sentText)
	}
}
