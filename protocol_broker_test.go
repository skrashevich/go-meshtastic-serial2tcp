package main

import (
	"bufio"
	"net"
	"os"
	"testing"
	"time"

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

	broker := newProtocolBroker(serialW, false, false)
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

	out, err := readFrame(bufio.NewReader(serialR))
	if err != nil {
		t.Fatalf("readFrame: %v", err)
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

	broker := newProtocolBroker(serialW, true, false)
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

	broker := newProtocolBroker(serialW, false, false)
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

	broker := newProtocolBroker(serialW, true, false)
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
