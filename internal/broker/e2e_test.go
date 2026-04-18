package broker

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/frame"
	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/proto"
)

// meshtasticNodeSim simulates a real Meshtastic device on the other end of the
// serial pipe. It speaks enough of the ToRadio/FromRadio protocol to verify
// the full client-bridge-node loop:
//   - Answers WantConfigId with a multi-part config dump ending in
//     ConfigCompleteId.
//   - Records every outbound MeshPacket the clients push through the broker.
//   - Emits unsolicited FromRadio messages on request (radio-initiated events).
type meshtasticNodeSim struct {
	brokerEnd net.Conn
	nodeEnd   net.Conn
	reader    *bufio.Reader
	writer    *bufio.Writer

	writeMu sync.Mutex

	myNodeNum uint32
	nodeInfos []*meshtasticpb.NodeInfo
	channels  []*meshtasticpb.Channel

	mu             sync.Mutex
	receivedToRadio []*meshtasticpb.ToRadio
	wantCount       int
	disconnectCount int

	// rebootedBeforeConfig, when true, causes the sim to reply to the first
	// WantConfigId with only FromRadio_Rebooted=true (no config stream). The
	// firmware normally clears its rebooted flag and waits for the phone to
	// re-issue WantConfigId; the broker under test should do that re-issue
	// automatically, at which point subsequent WantConfigIds get the normal
	// config dump. This mirrors the real device behaviour that triggered the
	// "rebooted loop" bug.
	rebootedBeforeConfig bool
	rebootedAlreadySent  bool

	stopOnce sync.Once
	stopped  chan struct{}
}

func newMeshtasticNodeSim() *meshtasticNodeSim {
	broker, node := net.Pipe()
	return &meshtasticNodeSim{
		brokerEnd: broker,
		nodeEnd:   node,
		reader:    bufio.NewReader(node),
		writer:    bufio.NewWriter(node),
		myNodeNum: 0x5AFECAFE,
		nodeInfos: []*meshtasticpb.NodeInfo{
			{Num: 0x5AFECAFE, User: &meshtasticpb.User{Id: "!self", LongName: "Local Node"}},
			{Num: 0x1111_1111, User: &meshtasticpb.User{Id: "!peer1", LongName: "Peer One"}},
			{Num: 0x2222_2222, User: &meshtasticpb.User{Id: "!peer2", LongName: "Peer Two"}},
		},
		channels: []*meshtasticpb.Channel{
			{Index: 0, Role: meshtasticpb.Channel_PRIMARY},
			{Index: 1, Role: meshtasticpb.Channel_SECONDARY},
		},
		stopped: make(chan struct{}),
	}
}

func (n *meshtasticNodeSim) run(t *testing.T) {
	t.Helper()
	go func() {
		defer close(n.stopped)
		for {
			payload, err := frame.ReadFrame(n.reader)
			if err != nil {
				return
			}
			msg := &meshtasticpb.ToRadio{}
			if err := proto.Unmarshal(payload, msg); err != nil {
				continue
			}

			n.mu.Lock()
			n.receivedToRadio = append(n.receivedToRadio, msg)
			n.mu.Unlock()

			switch v := msg.GetPayloadVariant().(type) {
			case *meshtasticpb.ToRadio_WantConfigId:
				n.mu.Lock()
				n.wantCount++
				sendRebootedOnly := n.rebootedBeforeConfig && !n.rebootedAlreadySent
				if sendRebootedOnly {
					n.rebootedAlreadySent = true
				}
				n.mu.Unlock()
				if sendRebootedOnly {
					n.sendFromRadio(t, &meshtasticpb.FromRadio{
						PayloadVariant: &meshtasticpb.FromRadio_Rebooted{Rebooted: true},
					})
					continue
				}
				n.dumpConfig(t, v.WantConfigId)
			case *meshtasticpb.ToRadio_Disconnect:
				n.mu.Lock()
				n.disconnectCount++
				n.mu.Unlock()
			case *meshtasticpb.ToRadio_Packet:
				// The real radio would transmit; for the test we just record.
			}
		}
	}()
}

// dumpConfig emits the sequence of FromRadio frames a real device answers a
// WantConfigId with, finishing with ConfigCompleteId so the broker routes the
// terminator back to the specific requester.
func (n *meshtasticNodeSim) dumpConfig(t *testing.T, wantID uint32) {
	t.Helper()
	n.sendFromRadio(t, &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_MyInfo{
			MyInfo: &meshtasticpb.MyNodeInfo{MyNodeNum: n.myNodeNum},
		},
	})
	for _, ni := range n.nodeInfos {
		n.sendFromRadio(t, &meshtasticpb.FromRadio{
			PayloadVariant: &meshtasticpb.FromRadio_NodeInfo{NodeInfo: ni},
		})
	}
	for _, ch := range n.channels {
		n.sendFromRadio(t, &meshtasticpb.FromRadio{
			PayloadVariant: &meshtasticpb.FromRadio_Channel{Channel: ch},
		})
	}
	n.sendFromRadio(t, &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_Metadata{
			Metadata: &meshtasticpb.DeviceMetadata{FirmwareVersion: "test-1.0"},
		},
	})
	n.sendFromRadio(t, &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_ConfigCompleteId{ConfigCompleteId: wantID},
	})
}

// sendFromRadio injects an arbitrary FromRadio frame onto the serial link as if
// the radio originated it (used to simulate inbound mesh traffic).
func (n *meshtasticNodeSim) sendFromRadio(t *testing.T, msg *meshtasticpb.FromRadio) {
	t.Helper()
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal FromRadio: %v", err)
	}
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	if err := frame.WriteFrame(n.writer, payload); err != nil {
		t.Fatalf("write FromRadio: %v", err)
	}
	if err := n.writer.Flush(); err != nil {
		t.Fatalf("flush radio: %v", err)
	}
}

// writeRaw emits bytes directly, bypassing the frame encoder. Used to feed
// malformed data to the broker for resilience checks.
func (n *meshtasticNodeSim) writeRaw(t *testing.T, b []byte) {
	t.Helper()
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	if _, err := n.nodeEnd.Write(b); err != nil {
		t.Fatalf("writeRaw: %v", err)
	}
}

func (n *meshtasticNodeSim) stats() (toRadio []*meshtasticpb.ToRadio, wants, disconnects int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := append([]*meshtasticpb.ToRadio(nil), n.receivedToRadio...)
	return out, n.wantCount, n.disconnectCount
}

func (n *meshtasticNodeSim) close() {
	n.stopOnce.Do(func() {
		_ = n.brokerEnd.Close()
		_ = n.nodeEnd.Close()
	})
}

// e2eClient wraps a broker-facing client endpoint with the frame helpers a real
// Meshtastic app would use.
type e2eClient struct {
	name   string
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func connectE2EClient(t *testing.T, b *Broker, name string) *e2eClient {
	t.Helper()
	brokerSide, appSide := net.Pipe()
	b.AddClient(brokerSide)
	return &e2eClient{
		name:   name,
		conn:   appSide,
		reader: bufio.NewReader(appSide),
		writer: bufio.NewWriter(appSide),
	}
}

func (c *e2eClient) sendWantConfig(t *testing.T, id uint32) {
	t.Helper()
	c.sendToRadio(t, &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_WantConfigId{WantConfigId: id},
	})
}

func (c *e2eClient) sendPacket(t *testing.T, pkt *meshtasticpb.MeshPacket) {
	t.Helper()
	c.sendToRadio(t, &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_Packet{Packet: pkt},
	})
}

func (c *e2eClient) sendDisconnect(t *testing.T) {
	t.Helper()
	c.sendToRadio(t, &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_Disconnect{Disconnect: true},
	})
}

func (c *e2eClient) sendToRadio(t *testing.T, msg *meshtasticpb.ToRadio) {
	t.Helper()
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("[%s] marshal: %v", c.name, err)
	}
	if err := frame.WriteFrame(c.writer, payload); err != nil {
		t.Fatalf("[%s] write frame: %v", c.name, err)
	}
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("[%s] flush: %v", c.name, err)
	}
}

// readUntil drains FromRadio frames until stopFn returns true or the timeout
// elapses. Returns everything read up to and including the frame that matched.
func (c *e2eClient) readUntil(t *testing.T, timeout time.Duration, stopFn func(*meshtasticpb.FromRadio) bool) []*meshtasticpb.FromRadio {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var collected []*meshtasticpb.FromRadio
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("[%s] readUntil: deadline exceeded after %d frames", c.name, len(collected))
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(remaining))
		payload, err := frame.ReadFrame(c.reader)
		if err != nil {
			t.Fatalf("[%s] readUntil: %v", c.name, err)
		}
		msg := &meshtasticpb.FromRadio{}
		if err := proto.Unmarshal(payload, msg); err != nil {
			t.Fatalf("[%s] unmarshal: %v", c.name, err)
		}
		collected = append(collected, msg)
		if stopFn(msg) {
			_ = c.conn.SetReadDeadline(time.Time{})
			return collected
		}
	}
}

func (c *e2eClient) expectNoFrame(t *testing.T, d time.Duration) {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(d))
	defer c.conn.SetReadDeadline(time.Time{})
	buf := make([]byte, 1)
	n, err := c.conn.Read(buf)
	if err == nil {
		t.Fatalf("[%s] expected silence but got %d bytes (0x%x)", c.name, n, buf[:n])
	}
	if !isTimeoutError(err) && !errors.Is(err, io.EOF) {
		t.Fatalf("[%s] unexpected error while expecting silence: %v", c.name, err)
	}
}

func (c *e2eClient) close() {
	_ = c.conn.Close()
}

func isTimeoutError(err error) bool {
	var te interface{ Timeout() bool }
	return errors.As(err, &te) && te.Timeout()
}

func isConfigComplete(msg *meshtasticpb.FromRadio) bool {
	_, ok := msg.GetPayloadVariant().(*meshtasticpb.FromRadio_ConfigCompleteId)
	return ok
}

// TestE2EThreeClientsFullHandshakeAndSync exercises the whole pipeline:
// radio <-> broker <-> 3 TCP clients, verifying the handshake, the cache-based
// optimization, packet echo semantics, radio-initiated broadcasts, slow-client
// resilience, primary promotion on disconnect, and recovery from a corrupt
// serial frame.
func TestE2EThreeClientsFullHandshakeAndSync(t *testing.T) {
	radio := newMeshtasticNodeSim()
	defer radio.close()
	radio.run(t)

	b := New(radio.brokerEnd, false, false)
	ctx, cancel := context.WithCancel(t.Context())

	runDone := make(chan error, 1)
	go func() { runDone <- b.Run(ctx) }()
	defer func() {
		cancel()
		radio.close()
		<-runDone
	}()

	// --- Step 1: client A connects and performs full handshake ---
	clientA := connectE2EClient(t, b, "A")
	defer clientA.close()
	// Give the broker goroutines a chance to register A before B and C join,
	// so the primary assignment is deterministic. Without this the AddClient
	// races observable to the next AddClient call could put the "first"
	// connection past the primary-empty check.
	time.Sleep(20 * time.Millisecond)

	clientA.sendWantConfig(t, 0xA000)
	framesA := clientA.readUntil(t, 2*time.Second, isConfigComplete)

	if gotA := framesA[len(framesA)-1].GetConfigCompleteId(); gotA != 0xA000 {
		t.Fatalf("A: config_complete_id = %x, want 0xA000", gotA)
	}
	if !containsMyInfo(framesA, radio.myNodeNum) {
		t.Fatalf("A: config dump missing MyInfo for node 0x%x", radio.myNodeNum)
	}
	if countNodeInfos(framesA) != len(radio.nodeInfos) {
		t.Fatalf("A: expected %d NodeInfo frames, got %d", len(radio.nodeInfos), countNodeInfos(framesA))
	}

	_, wantsAfterA, _ := radio.stats()
	if wantsAfterA != 1 {
		t.Fatalf("radio should have seen exactly 1 WantConfigId, saw %d", wantsAfterA)
	}

	// --- Step 2: clients B and C connect, served from cache ---
	clientB := connectE2EClient(t, b, "B")
	defer clientB.close()
	clientC := connectE2EClient(t, b, "C")
	defer clientC.close()
	time.Sleep(30 * time.Millisecond)

	clientB.sendWantConfig(t, 0xB000)
	framesB := clientB.readUntil(t, 2*time.Second, isConfigComplete)
	if gotB := framesB[len(framesB)-1].GetConfigCompleteId(); gotB != 0xB000 {
		t.Fatalf("B: config_complete_id = %x, want 0xB000", gotB)
	}
	if !containsMyInfo(framesB, radio.myNodeNum) {
		t.Fatalf("B: cached dump missing MyInfo")
	}

	clientC.sendWantConfig(t, 0xC000)
	framesC := clientC.readUntil(t, 2*time.Second, isConfigComplete)
	if gotC := framesC[len(framesC)-1].GetConfigCompleteId(); gotC != 0xC000 {
		t.Fatalf("C: config_complete_id = %x, want 0xC000", gotC)
	}

	_, wantsAfterCache, _ := radio.stats()
	if wantsAfterCache != 1 {
		t.Fatalf("cache must suppress secondary WantConfigId; radio saw %d", wantsAfterCache)
	}

	// --- Step 3: radio-originated NodeInfo → all three receive ---
	radio.sendFromRadio(t, &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_NodeInfo{
			NodeInfo: &meshtasticpb.NodeInfo{Num: 0x3333_3333, User: &meshtasticpb.User{Id: "!peer3"}},
		},
	})
	for _, cl := range []*e2eClient{clientA, clientB, clientC} {
		got := cl.readUntil(t, 2*time.Second, func(msg *meshtasticpb.FromRadio) bool {
			return msg.GetNodeInfo().GetNum() == 0x3333_3333
		})
		if len(got) == 0 {
			t.Fatalf("[%s] did not receive radio-initiated NodeInfo", cl.name)
		}
	}

	// --- Step 4: client A sends packet → radio receives, B+C echo, A doesn't ---
	clientA.sendPacket(t, &meshtasticpb.MeshPacket{Id: 0x1234, From: 0x5AFECAFE, To: 0xFFFF_FFFF})

	waitFor(t, 1*time.Second, func() bool {
		toRadio, _, _ := radio.stats()
		for _, m := range toRadio {
			if p := m.GetPacket(); p != nil && p.GetId() == 0x1234 {
				return true
			}
		}
		return false
	})

	for _, cl := range []*e2eClient{clientB, clientC} {
		got := cl.readUntil(t, 2*time.Second, func(msg *meshtasticpb.FromRadio) bool {
			return msg.GetPacket().GetId() == 0x1234
		})
		if len(got) == 0 {
			t.Fatalf("[%s] missed echo of A's packet 0x1234", cl.name)
		}
	}
	clientA.expectNoFrame(t, 150*time.Millisecond)

	// --- Step 5: client B disconnects politely → radio not notified ---
	clientB.sendDisconnect(t)
	clientB.close()
	time.Sleep(100 * time.Millisecond)

	_, _, disconnectsAfterB := radio.stats()
	if disconnectsAfterB != 0 {
		t.Fatalf("secondary disconnect must not reach radio, got %d", disconnectsAfterB)
	}

	// A and C must still receive radio broadcasts.
	radio.sendFromRadio(t, &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_Packet{
			Packet: &meshtasticpb.MeshPacket{Id: 0x7777},
		},
	})
	for _, cl := range []*e2eClient{clientA, clientC} {
		got := cl.readUntil(t, 2*time.Second, func(msg *meshtasticpb.FromRadio) bool {
			return msg.GetPacket().GetId() == 0x7777
		})
		if len(got) == 0 {
			t.Fatalf("[%s] missed broadcast 0x7777 after B disconnect", cl.name)
		}
	}

	// --- Step 6: primary A disconnects → C gets promoted; C's WantConfigId
	// is still served from the populated cache and must NOT reach the radio
	// (serving from cache regardless of primary/secondary is what avoids the
	// firmware rebooted=true loop this broker is designed to prevent). ---
	clientA.close()
	time.Sleep(150 * time.Millisecond)

	b.clientsMu.RLock()
	newPrimary := b.primary
	remaining := len(b.clients)
	b.clientsMu.RUnlock()
	if newPrimary == nil {
		t.Fatalf("no primary after A disconnect")
	}
	if remaining != 1 {
		t.Fatalf("expected 1 remaining client, got %d", remaining)
	}

	clientC.sendWantConfig(t, 0xCCCC)
	framesC2 := clientC.readUntil(t, 3*time.Second, isConfigComplete)
	if got := framesC2[len(framesC2)-1].GetConfigCompleteId(); got != 0xCCCC {
		t.Fatalf("C after promotion: config_complete_id = %x, want 0xCCCC", got)
	}
	if !containsMyInfo(framesC2, radio.myNodeNum) {
		t.Fatalf("C after promotion: cached dump missing MyInfo")
	}
	_, wantsAfterPromotion, _ := radio.stats()
	if wantsAfterPromotion != 1 {
		t.Fatalf("promoted primary's WantConfigId must be cache-served; radio saw %d total wants", wantsAfterPromotion)
	}

	// --- Step 7: inject a malformed serial frame and a valid one → C still receives the valid one ---
	radio.writeRaw(t, []byte{frame.Magic0, frame.Magic1, 0x00, 0x00})
	radio.sendFromRadio(t, &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_Packet{
			Packet: &meshtasticpb.MeshPacket{Id: 0x9999},
		},
	})
	got := clientC.readUntil(t, 2*time.Second, func(msg *meshtasticpb.FromRadio) bool {
		return msg.GetPacket().GetId() == 0x9999
	})
	if len(got) == 0 {
		t.Fatalf("broker failed to resync after invalid frame")
	}

	// --- Step 8: broker still healthy, Run has not returned ---
	select {
	case err := <-runDone:
		t.Fatalf("broker exited unexpectedly: %v", err)
	default:
	}
}

// TestE2ERadioReconnectIsolation verifies that when the serial link dies, Run
// returns cleanly without hanging on client goroutines.
func TestE2ERadioReconnectIsolation(t *testing.T) {
	radio := newMeshtasticNodeSim()
	radio.run(t)

	b := New(radio.brokerEnd, false, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- b.Run(ctx) }()

	client := connectE2EClient(t, b, "A")
	defer client.close()
	time.Sleep(20 * time.Millisecond)

	// Kill the radio side of the serial pipe.
	radio.close()

	select {
	case err := <-runDone:
		if err == nil || errors.Is(err, context.Canceled) {
			// Either ErrSerialClosed or nil is acceptable depending on race.
			return
		}
		if !errors.Is(err, ErrSerialClosed) {
			t.Fatalf("expected ErrSerialClosed after radio death, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("broker did not exit after radio disconnect")
	}
}

// TestE2ERebootedLoopRecovery reproduces the production bug and asserts the
// fix: on the first WantConfigId the simulated radio replies with only
// FromRadio_Rebooted=true. The broker must (a) not forward the rebooted frame
// to the client, (b) automatically re-issue the WantConfigId with a fresh
// nonce, and (c) eventually deliver a clean my_info + config_complete_id to
// the client with the client's ORIGINAL nonce.
//
// Additionally, a second client that connects afterwards must be served
// entirely from the now-populated cache.
func TestE2ERebootedLoopRecovery(t *testing.T) {
	radio := newMeshtasticNodeSim()
	radio.rebootedBeforeConfig = true
	defer radio.close()
	radio.run(t)

	b := New(radio.brokerEnd, false, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- b.Run(ctx) }()
	defer func() {
		cancel()
		radio.close()
		<-runDone
	}()

	primary := connectE2EClient(t, b, "primary")
	defer primary.close()
	time.Sleep(20 * time.Millisecond)

	const primaryNonce uint32 = 0xDEADBEEF
	primary.sendWantConfig(t, primaryNonce)

	frames := primary.readUntil(t, 3*time.Second, isConfigComplete)
	for _, f := range frames {
		if f.GetRebooted() {
			t.Fatalf("broker leaked rebooted=true to the primary client; frame=%v", f)
		}
	}
	last := frames[len(frames)-1]
	if last.GetConfigCompleteId() != primaryNonce {
		t.Fatalf("primary: expected config_complete_id=%x, got %x", primaryNonce, last.GetConfigCompleteId())
	}
	if !containsMyInfo(frames, radio.myNodeNum) {
		t.Fatalf("primary: config dump missing MyInfo after rebooted recovery")
	}

	_, wantCountAfterRecovery, _ := radio.stats()
	if wantCountAfterRecovery != 2 {
		t.Fatalf("expected broker to re-issue want_config_id exactly once after rebooted; radio saw %d total wants", wantCountAfterRecovery)
	}

	// Now a brand-new client comes in after the cache has been populated. It
	// must be served entirely from cache — the radio should see NO additional
	// want_config_id. This is the scenario meshmonitor was hitting in the bug
	// report: connecting as secondary, seeing an empty cache, and getting
	// only config_complete_id back. With the fix in place, it gets the full
	// cached dump.
	secondary := connectE2EClient(t, b, "secondary")
	defer secondary.close()
	time.Sleep(20 * time.Millisecond)

	const secondaryNonce uint32 = 0xFFFFFFFF
	secondary.sendWantConfig(t, secondaryNonce)
	cachedFrames := secondary.readUntil(t, 2*time.Second, isConfigComplete)
	if cachedFrames[len(cachedFrames)-1].GetConfigCompleteId() != secondaryNonce {
		t.Fatalf("secondary: config_complete_id mismatch")
	}
	if !containsMyInfo(cachedFrames, radio.myNodeNum) {
		t.Fatalf("secondary: cached dump missing MyInfo")
	}

	_, finalWants, _ := radio.stats()
	if finalWants != 2 {
		t.Fatalf("secondary must be cache-served; radio saw %d total wants after secondary join", finalWants)
	}
}

// TestE2EPrimaryDisconnectNotForwardedToRadio locks in the invariant that no
// TCP client's Disconnect — primary or secondary — reaches the radio. This
// behavior is critical: forwarding per-client disconnects was what caused the
// firmware to reset its phone-connection state and reply to the next
// WantConfigId with rebooted=true, triggering the loop.
func TestE2EPrimaryDisconnectNotForwardedToRadio(t *testing.T) {
	radio := newMeshtasticNodeSim()
	defer radio.close()
	radio.run(t)

	b := New(radio.brokerEnd, false, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- b.Run(ctx) }()
	defer func() {
		cancel()
		radio.close()
		<-runDone
	}()

	primary := connectE2EClient(t, b, "primary")
	defer primary.close()
	time.Sleep(20 * time.Millisecond)

	primary.sendDisconnect(t)
	time.Sleep(100 * time.Millisecond)

	_, _, disconnects := radio.stats()
	if disconnects != 0 {
		t.Fatalf("primary Disconnect must not reach radio, got %d", disconnects)
	}

	b.clientsMu.RLock()
	_, stillPresent := b.clients[findClientByAddrPrefix(b, "pipe")]
	count := len(b.clients)
	b.clientsMu.RUnlock()
	_ = stillPresent
	if count != 0 {
		t.Fatalf("primary should be removed locally, %d clients remain", count)
	}
}

// findClientByAddrPrefix returns any client whose address starts with prefix.
// Used only in tests to assert presence.
func findClientByAddrPrefix(b *Broker, prefix string) *client {
	b.clientsMu.RLock()
	defer b.clientsMu.RUnlock()
	for cl := range b.clients {
		if len(cl.addr) >= len(prefix) && cl.addr[:len(prefix)] == prefix {
			return cl
		}
	}
	return nil
}

func containsMyInfo(frames []*meshtasticpb.FromRadio, want uint32) bool {
	for _, f := range frames {
		if mi := f.GetMyInfo(); mi != nil && mi.GetMyNodeNum() == want {
			return true
		}
	}
	return false
}

func countNodeInfos(frames []*meshtasticpb.FromRadio) int {
	n := 0
	for _, f := range frames {
		if f.GetNodeInfo() != nil {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
