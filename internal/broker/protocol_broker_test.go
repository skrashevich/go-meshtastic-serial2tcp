package broker

import (
	"bufio"
	"net"
	"os"
	"testing"
	"time"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/frame"
	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/proto"
)

func newTestClient() (*client, net.Conn) {
	c1, c2 := net.Pipe()
	cl := &client{
		conn: c1,
		send: make(chan []byte, clientSendBuffer),
		addr: "test-client",
	}
	return cl, c2
}

func recvPayload(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case payload := <-ch:
		return payload
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for payload")
		return nil
	}
}

func TestProtocolBrokerWantConfigPrimaryForwardsAndTracks(t *testing.T) {
	serialR, serialW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer serialR.Close()
	defer serialW.Close()

	broker := New(serialW, false, false)
	client, peer := newTestClient()
	defer peer.Close()
	defer client.conn.Close()

	wantID := uint32(42)
	toRadio := &meshtasticpb.ToRadio{PayloadVariant: &meshtasticpb.ToRadio_WantConfigId{WantConfigId: wantID}}
	payload, err := proto.Marshal(toRadio)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	broker.handleClientPayload(client, payload)

	out, err := frame.ReadFrame(bufio.NewReader(serialR))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	outMsg := &meshtasticpb.ToRadio{}
	if err := proto.Unmarshal(out, outMsg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	newID := outMsg.GetWantConfigId()
	if newID == 0 || newID == wantID {
		t.Fatalf("want_config_id not rewritten: got %d", newID)
	}

	broker.pendingMu.Lock()
	req, ok := broker.pendingConfig[newID]
	broker.pendingMu.Unlock()
	if !ok {
		t.Fatalf("pending config not recorded")
	}
	if req.originalID != wantID || req.client != client {
		t.Fatalf("pending config mismatch")
	}
}

func TestProtocolBrokerWantConfigReadOnlyUsesCache(t *testing.T) {
	serialR, serialW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer serialR.Close()
	defer serialW.Close()

	broker := New(serialW, true, false)
	primary, primaryPeer := newTestClient()
	defer primaryPeer.Close()
	defer primary.conn.Close()
	readonly, readonlyPeer := newTestClient()
	defer readonlyPeer.Close()
	defer readonly.conn.Close()

	broker.clientsMu.Lock()
	broker.clients[primary] = struct{}{}
	broker.clients[readonly] = struct{}{}
	broker.primary = primary
	broker.clientsMu.Unlock()

	cachePayload := []byte("CACHE")
	broker.cache.update(&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_MyInfo{MyInfo: &meshtasticpb.MyNodeInfo{}}}, cachePayload)

	wantID := uint32(99)
	toRadio := &meshtasticpb.ToRadio{PayloadVariant: &meshtasticpb.ToRadio_WantConfigId{WantConfigId: wantID}}
	payload, err := proto.Marshal(toRadio)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	broker.handleClientPayload(readonly, payload)

	first := recvPayload(t, readonly.send)
	if string(first) != string(cachePayload) {
		t.Fatalf("unexpected cache payload: got %q", first)
	}

	last := recvPayload(t, readonly.send)
	resp := &meshtasticpb.FromRadio{}
	if err := proto.Unmarshal(last, resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.GetConfigCompleteId() != wantID {
		t.Fatalf("config_complete_id mismatch: got %d want %d", resp.GetConfigCompleteId(), wantID)
	}
}

func TestProtocolBrokerHandleConfigCompleteRewrites(t *testing.T) {
	serialR, serialW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer serialR.Close()
	defer serialW.Close()

	broker := New(serialW, false, false)
	client, peer := newTestClient()
	defer peer.Close()
	defer client.conn.Close()

	pendingID := uint32(123)
	originalID := uint32(999)
	broker.pendingMu.Lock()
	broker.pendingConfig[pendingID] = configRequest{client: client, originalID: originalID}
	broker.pendingMu.Unlock()

	broker.handleConfigComplete(pendingID, []byte("ignored"))

	payload := recvPayload(t, client.send)
	resp := &meshtasticpb.FromRadio{}
	if err := proto.Unmarshal(payload, resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.GetConfigCompleteId() != originalID {
		t.Fatalf("config_complete_id mismatch: got %d want %d", resp.GetConfigCompleteId(), originalID)
	}
}

func TestProtocolBrokerRemoveClientPromotesPrimary(t *testing.T) {
	serialR, serialW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer serialR.Close()
	defer serialW.Close()

	broker := New(serialW, true, false)
	primary, primaryPeer := newTestClient()
	defer primaryPeer.Close()
	secondary, secondaryPeer := newTestClient()
	defer secondaryPeer.Close()

	broker.clientsMu.Lock()
	broker.clients[primary] = struct{}{}
	broker.clients[secondary] = struct{}{}
	broker.primary = primary
	broker.clientsMu.Unlock()

	broker.removeClient(primary)

	broker.clientsMu.RLock()
	newPrimary := broker.primary
	broker.clientsMu.RUnlock()
	if newPrimary != secondary {
		t.Fatalf("expected secondary to become primary")
	}
}

func TestProtocolBrokerPacketBroadcastToAllClients(t *testing.T) {
	serialR, serialW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer serialR.Close()
	defer serialW.Close()

	broker := New(serialW, false, false)
	client1, peer1 := newTestClient()
	defer peer1.Close()
	defer client1.conn.Close()
	client2, peer2 := newTestClient()
	defer peer2.Close()
	defer client2.conn.Close()

	broker.clientsMu.Lock()
	broker.clients[client1] = struct{}{}
	broker.clients[client2] = struct{}{}
	broker.clientsMu.Unlock()

	// Создаем тестовый пакет
	testPacket := &meshtasticpb.MeshPacket{
		From: 123456,
		To:   789012,
		Id:   42,
	}
	toRadio := &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_Packet{
			Packet: testPacket,
		},
	}
	payload, err := proto.Marshal(toRadio)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Клиент 1 отправляет пакет
	broker.handleClientPayload(client1, payload)

	// Проверяем, что пакет был отправлен в serial
	out, err := frame.ReadFrame(bufio.NewReader(serialR))
	if err != nil {
		t.Fatalf("ReadFrame from serial: %v", err)
	}
	outMsg := &meshtasticpb.ToRadio{}
	if err := proto.Unmarshal(out, outMsg); err != nil {
		t.Fatalf("unmarshal serial output: %v", err)
	}
	if outMsg.GetPacket().GetId() != 42 {
		t.Fatalf("packet not forwarded to serial correctly")
	}

	// Проверяем, что оба клиента получили broadcast
	for i, client := range []*client{client1, client2} {
		broadcastPayload := recvPayload(t, client.send)
		fromRadio := &meshtasticpb.FromRadio{}
		if err := proto.Unmarshal(broadcastPayload, fromRadio); err != nil {
			t.Fatalf("client %d: unmarshal broadcast: %v", i+1, err)
		}
		pkt := fromRadio.GetPacket()
		if pkt == nil {
			t.Fatalf("client %d: expected packet in broadcast", i+1)
		}
		if pkt.GetId() != 42 || pkt.GetFrom() != 123456 {
			t.Fatalf("client %d: broadcast packet mismatch: got id=%d from=%d", i+1, pkt.GetId(), pkt.GetFrom())
		}
	}
}
