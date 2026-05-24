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
	cl := newClient(c1)
	cl.addr = "test-client"
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

	broker.clientsMu.Lock()
	broker.clients[client] = struct{}{}
	broker.primary = client
	broker.clientsMu.Unlock()

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

func TestProtocolBrokerPacketBroadcastExcludesSender(t *testing.T) {
	serialR, serialW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer serialR.Close()
	defer serialW.Close()

	broker := New(serialW, false, false)
	sender, peer1 := newTestClient()
	defer peer1.Close()
	defer sender.conn.Close()
	peerClient, peer2 := newTestClient()
	defer peer2.Close()
	defer peerClient.conn.Close()

	broker.clientsMu.Lock()
	broker.clients[sender] = struct{}{}
	broker.clients[peerClient] = struct{}{}
	broker.primary = sender
	broker.clientsMu.Unlock()

	testPacket := &meshtasticpb.MeshPacket{
		From: 123456,
		To:   789012,
		Id:   42,
	}
	toRadio := &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_Packet{Packet: testPacket},
	}
	payload, err := proto.Marshal(toRadio)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	broker.handleClientPayload(sender, payload)

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

	broadcastPayload := recvPayload(t, peerClient.send)
	fromRadio := &meshtasticpb.FromRadio{}
	if err := proto.Unmarshal(broadcastPayload, fromRadio); err != nil {
		t.Fatalf("unmarshal broadcast: %v", err)
	}
	pkt := fromRadio.GetPacket()
	if pkt == nil {
		t.Fatalf("expected packet in peer broadcast")
	}
	if pkt.GetId() != 42 || pkt.GetFrom() != 123456 {
		t.Fatalf("peer broadcast mismatch: got id=%d from=%d", pkt.GetId(), pkt.GetFrom())
	}

	select {
	case payload := <-sender.send:
		t.Fatalf("sender must not receive its own echo, got %d bytes", len(payload))
	case <-time.After(100 * time.Millisecond):
	}
}

func TestProtocolBrokerRadioEchoExcludesSender(t *testing.T) {
	radioSide, brokerSide := net.Pipe()
	defer radioSide.Close()
	defer brokerSide.Close()

	broker := New(brokerSide, false, false)
	go func() {
		reader := bufio.NewReader(radioSide)
		for {
			if _, err := frame.ReadFrame(reader); err != nil {
				return
			}
		}
	}()
	errCh := make(chan error, 1)
	go broker.readSerial(errCh)

	sender, _ := newTestClient()
	peerClient, _ := newTestClient()
	broker.clientsMu.Lock()
	broker.clients[sender] = struct{}{}
	broker.clients[peerClient] = struct{}{}
	broker.primary = sender
	broker.clientsMu.Unlock()

	testPacket := &meshtasticpb.MeshPacket{
		From: 123456,
		To:   789012,
		Id:   99,
		PayloadVariant: &meshtasticpb.MeshPacket_Decoded{
			Decoded: &meshtasticpb.Data{
				Portnum: meshtasticpb.PortNum_TEXT_MESSAGE_APP,
				Payload: []byte("hi"),
			},
		},
	}
	toRadio := &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_Packet{Packet: testPacket},
	}
	payload, err := proto.Marshal(toRadio)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	broker.handleClientPayload(sender, payload)

	echo := &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_Packet{Packet: testPacket},
	}
	echoPayload, err := proto.Marshal(echo)
	if err != nil {
		t.Fatalf("marshal echo: %v", err)
	}
	if err := frame.WriteFrame(radioSide, echoPayload); err != nil {
		t.Fatalf("write radio echo: %v", err)
	}

	recvPayload(t, peerClient.send)
	select {
	case got := <-peerClient.send:
		t.Fatalf("peer must not receive duplicate radio echo, got %d bytes", len(got))
	case <-time.After(200 * time.Millisecond):
	}

	select {
	case got := <-sender.send:
		t.Fatalf("sender must not receive radio echo, got %d bytes", len(got))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestProtocolBrokerAdminRadioEchoDeliveredToSender(t *testing.T) {
	radioSide, brokerSide := net.Pipe()
	defer radioSide.Close()
	defer brokerSide.Close()

	broker := New(brokerSide, false, false)
	go func() {
		reader := bufio.NewReader(radioSide)
		for {
			if _, err := frame.ReadFrame(reader); err != nil {
				return
			}
		}
	}()
	errCh := make(chan error, 1)
	go broker.readSerial(errCh)

	sender, _ := newTestClient()
	peerClient, _ := newTestClient()
	broker.clientsMu.Lock()
	broker.clients[sender] = struct{}{}
	broker.clients[peerClient] = struct{}{}
	broker.primary = sender
	broker.clientsMu.Unlock()

	request := &meshtasticpb.MeshPacket{
		From: 123456,
		To:   0,
		Id:   101,
		PayloadVariant: &meshtasticpb.MeshPacket_Decoded{
			Decoded: &meshtasticpb.Data{
				Portnum: meshtasticpb.PortNum_ADMIN_APP,
				Payload: mustMarshal(t, &meshtasticpb.AdminMessage{
					PayloadVariant: &meshtasticpb.AdminMessage_GetChannelRequest{
						GetChannelRequest: 0,
					},
				}),
			},
		},
	}
	toRadio := &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_Packet{Packet: request},
	}
	payload, err := proto.Marshal(toRadio)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	broker.handleClientPayload(sender, payload)

	response := &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_Packet{
			Packet: &meshtasticpb.MeshPacket{
				From: 123456,
				To:   0,
				Id:   101,
				PayloadVariant: &meshtasticpb.MeshPacket_Decoded{
					Decoded: &meshtasticpb.Data{
						Portnum: meshtasticpb.PortNum_ADMIN_APP,
						Payload: mustMarshal(t, &meshtasticpb.AdminMessage{
							PayloadVariant: &meshtasticpb.AdminMessage_GetChannelResponse{
								GetChannelResponse: &meshtasticpb.Channel{Index: 0},
							},
						}),
					},
				},
			},
		},
	}
	echoPayload, err := proto.Marshal(response)
	if err != nil {
		t.Fatalf("marshal echo: %v", err)
	}
	if err := frame.WriteFrame(radioSide, echoPayload); err != nil {
		t.Fatalf("write radio echo: %v", err)
	}

	recvPayload(t, peerClient.send)
	select {
	case got := <-peerClient.send:
		t.Fatalf("peer must not receive duplicate radio echo, got %d bytes", len(got))
	case <-time.After(200 * time.Millisecond):
	}

	got := recvPayload(t, sender.send)
	fr := &meshtasticpb.FromRadio{}
	if err := proto.Unmarshal(got, fr); err != nil {
		t.Fatalf("sender echo unmarshal: %v", err)
	}
	if fr.GetPacket().GetId() != 101 {
		t.Fatalf("sender must receive admin radio echo, got packet id=%d", fr.GetPacket().GetId())
	}
}

func mustMarshal(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestProtocolBrokerSecondaryPacketForwardsWhenReadOnlyFalse(t *testing.T) {
	serialR, serialW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer serialR.Close()
	defer serialW.Close()

	broker := New(serialW, false, false)
	primary, peer1 := newTestClient()
	defer peer1.Close()
	defer primary.conn.Close()
	secondary, peer2 := newTestClient()
	defer peer2.Close()
	defer secondary.conn.Close()

	broker.clientsMu.Lock()
	broker.clients[primary] = struct{}{}
	broker.clients[secondary] = struct{}{}
	broker.primary = primary
	broker.clientsMu.Unlock()

	toRadio := &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_Packet{Packet: &meshtasticpb.MeshPacket{Id: 77}},
	}
	payload, err := proto.Marshal(toRadio)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	broker.handleClientPayload(secondary, payload)

	out, err := frame.ReadFrame(bufio.NewReader(serialR))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	got := &meshtasticpb.ToRadio{}
	if err := proto.Unmarshal(out, got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetPacket().GetId() != 77 {
		t.Fatalf("expected secondary packet id=77 to reach serial, got %d", got.GetPacket().GetId())
	}

	echo := recvPayload(t, primary.send)
	fr := &meshtasticpb.FromRadio{}
	if err := proto.Unmarshal(echo, fr); err != nil {
		t.Fatalf("primary echo unmarshal: %v", err)
	}
	if fr.GetPacket().GetId() != 77 {
		t.Fatalf("primary should see sender echo id=77, got %d", fr.GetPacket().GetId())
	}
}

func TestProtocolBrokerSecondaryPacketRejectedWhenReadOnlyTrue(t *testing.T) {
	serialR, serialW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer serialR.Close()
	defer serialW.Close()

	broker := New(serialW, true, false)
	primary, peer1 := newTestClient()
	defer peer1.Close()
	defer primary.conn.Close()
	secondary, peer2 := newTestClient()
	defer peer2.Close()
	defer secondary.conn.Close()

	broker.clientsMu.Lock()
	broker.clients[primary] = struct{}{}
	broker.clients[secondary] = struct{}{}
	broker.primary = primary
	broker.clientsMu.Unlock()

	toRadio := &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_Packet{Packet: &meshtasticpb.MeshPacket{Id: 88}},
	}
	payload, err := proto.Marshal(toRadio)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		buf := make([]byte, 16)
		_, _ = serialR.Read(buf)
	}()

	broker.handleClientPayload(secondary, payload)

	select {
	case <-drainDone:
		t.Fatalf("read-only secondary packet must not reach serial")
	case <-time.After(150 * time.Millisecond):
	}
	_ = serialR.Close()
	<-drainDone
}

func TestProtocolBrokerNonPrimaryDisconnectDoesNotReachSerial(t *testing.T) {
	serialR, serialW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer serialR.Close()
	defer serialW.Close()

	broker := New(serialW, false, false)
	primary, peer1 := newTestClient()
	defer peer1.Close()
	secondary, peer2 := newTestClient()
	defer peer2.Close()

	broker.clientsMu.Lock()
	broker.clients[primary] = struct{}{}
	broker.clients[secondary] = struct{}{}
	broker.primary = primary
	broker.clientsMu.Unlock()

	toRadio := &meshtasticpb.ToRadio{PayloadVariant: &meshtasticpb.ToRadio_Disconnect{Disconnect: true}}
	payload, err := proto.Marshal(toRadio)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 16)
		_, _ = serialR.Read(buf)
	}()

	broker.handleClientPayload(secondary, payload)

	select {
	case <-done:
		t.Fatalf("secondary disconnect must not be written to serial")
	case <-time.After(150 * time.Millisecond):
	}
	_ = serialR.Close()
	<-done

	broker.clientsMu.RLock()
	_, stillPresent := broker.clients[secondary]
	broker.clientsMu.RUnlock()
	if stillPresent {
		t.Fatalf("secondary should be removed after disconnect request")
	}
}

func TestProtocolBrokerSecondaryWantConfigDoesNotReachSerial(t *testing.T) {
	serialR, serialW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer serialR.Close()
	defer serialW.Close()

	// readOnlyClients=false — confirms caching of WantConfigId is now
	// independent of the flag.
	broker := New(serialW, false, false)
	primary, peer1 := newTestClient()
	defer peer1.Close()
	defer primary.conn.Close()
	secondary, peer2 := newTestClient()
	defer peer2.Close()
	defer secondary.conn.Close()

	broker.clientsMu.Lock()
	broker.clients[primary] = struct{}{}
	broker.clients[secondary] = struct{}{}
	broker.primary = primary
	broker.clientsMu.Unlock()

	broker.cache.update(
		&meshtasticpb.FromRadio{PayloadVariant: &meshtasticpb.FromRadio_MyInfo{MyInfo: &meshtasticpb.MyNodeInfo{}}},
		[]byte("MYINFO"),
	)

	toRadio := &meshtasticpb.ToRadio{PayloadVariant: &meshtasticpb.ToRadio_WantConfigId{WantConfigId: 321}}
	payload, err := proto.Marshal(toRadio)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 16)
		_, _ = serialR.Read(buf)
	}()

	broker.handleClientPayload(secondary, payload)

	first := recvPayload(t, secondary.send)
	if string(first) != "MYINFO" {
		t.Fatalf("unexpected cache payload: got %q", first)
	}
	last := recvPayload(t, secondary.send)
	resp := &meshtasticpb.FromRadio{}
	if err := proto.Unmarshal(last, resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.GetConfigCompleteId() != 321 {
		t.Fatalf("config_complete_id mismatch: got %d", resp.GetConfigCompleteId())
	}

	select {
	case <-done:
		t.Fatalf("secondary WantConfigId must not reach serial when not primary")
	case <-time.After(100 * time.Millisecond):
	}
	_ = serialR.Close()
	<-done
}

func TestProtocolBrokerRepeatedWantConfigDoesNotLeakPending(t *testing.T) {
	serialR, serialW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer serialR.Close()
	defer serialW.Close()

	// Drain the serial side so forwardToSerial does not block on the pipe.
	go func() {
		buf := make([]byte, 1024)
		for {
			if _, err := serialR.Read(buf); err != nil {
				return
			}
		}
	}()

	broker := New(serialW, false, false)
	client, peer := newTestClient()
	defer peer.Close()
	defer client.conn.Close()

	broker.clientsMu.Lock()
	broker.clients[client] = struct{}{}
	broker.primary = client
	broker.clientsMu.Unlock()

	// The cache stays empty on purpose — that's the branch that actually
	// reserves a broker-side nonce. If the client retries WantConfigId
	// while one is still pending, the broker must replace the old
	// pending entry, not stack a new one next to it.
	for i := uint32(1); i <= 5; i++ {
		toRadio := &meshtasticpb.ToRadio{PayloadVariant: &meshtasticpb.ToRadio_WantConfigId{WantConfigId: i}}
		payload, err := proto.Marshal(toRadio)
		if err != nil {
			t.Fatalf("marshal[%d]: %v", i, err)
		}
		broker.handleClientPayload(client, payload)
	}

	broker.pendingMu.Lock()
	count := len(broker.pendingConfig)
	broker.pendingMu.Unlock()
	if count != 1 {
		t.Fatalf("expected exactly 1 pending config entry after 5 retries, got %d", count)
	}
}

func TestProtocolBrokerAddClientTracksPrimaryAlways(t *testing.T) {
	serialR, serialW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer serialR.Close()
	defer serialW.Close()

	broker := New(serialW, false, false)
	c1, c1Peer := net.Pipe()
	defer c1.Close()
	defer c1Peer.Close()
	c2, c2Peer := net.Pipe()
	defer c2.Close()
	defer c2Peer.Close()

	broker.AddClient(c1)
	broker.AddClient(c2)

	broker.clientsMu.RLock()
	primary := broker.primary
	count := len(broker.clients)
	broker.clientsMu.RUnlock()

	if primary == nil {
		t.Fatalf("primary must be tracked even when readOnlyClients=false")
	}
	if count != 2 {
		t.Fatalf("expected 2 clients, got %d", count)
	}
}
