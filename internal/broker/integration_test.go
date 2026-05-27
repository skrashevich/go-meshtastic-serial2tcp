package broker

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/frame"
	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/proto"
)

// testClientConn wraps a bidirectional pipe so the test can read what the
// broker wrote to the client and write what the client would send.
type testClientConn struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func dialBroker(t *testing.T, b *Broker) *testClientConn {
	t.Helper()
	c1, c2 := net.Pipe()
	b.AddClient(c1)
	return &testClientConn{
		conn:   c2,
		reader: bufio.NewReader(c2),
		writer: bufio.NewWriter(c2),
	}
}

func (c *testClientConn) sendToRadio(t *testing.T, msg *meshtasticpb.ToRadio) {
	t.Helper()
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal ToRadio: %v", err)
	}
	if err := frame.WriteFrame(c.writer, payload); err != nil {
		t.Fatalf("write frame: %v", err)
	}
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func (c *testClientConn) readFromRadio(t *testing.T, timeout time.Duration) *meshtasticpb.FromRadio {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	defer c.conn.SetReadDeadline(time.Time{})
	payload, err := frame.ReadFrame(c.reader)
	if err != nil {
		t.Fatalf("readFromRadio: %v", err)
	}
	msg := &meshtasticpb.FromRadio{}
	if err := proto.Unmarshal(payload, msg); err != nil {
		t.Fatalf("unmarshal FromRadio: %v", err)
	}
	return msg
}

func (c *testClientConn) expectNoFromRadio(t *testing.T, timeout time.Duration) {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	defer c.conn.SetReadDeadline(time.Time{})
	buf := make([]byte, 1)
	n, err := c.conn.Read(buf)
	if err == nil {
		t.Fatalf("unexpected data available: %x (n=%d)", buf[:n], n)
	}
}

func (c *testClientConn) close() {
	_ = c.conn.Close()
}

// radioSim simulates the radio side of the serial link for tests.
type radioSim struct {
	broker net.Conn
	radio  net.Conn
	reader *bufio.Reader
}

func newRadioSim() *radioSim {
	a, b := net.Pipe()
	return &radioSim{
		broker: a,
		radio:  b,
		reader: bufio.NewReader(b),
	}
}

func (r *radioSim) write(t *testing.T, msg *meshtasticpb.FromRadio) {
	t.Helper()
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := frame.WriteFrame(r.radio, payload); err != nil {
		t.Fatalf("radio write: %v", err)
	}
}

func (r *radioSim) readToRadio(t *testing.T, timeout time.Duration) *meshtasticpb.ToRadio {
	t.Helper()
	_ = r.radio.SetReadDeadline(time.Now().Add(timeout))
	defer r.radio.SetReadDeadline(time.Time{})
	payload, err := frame.ReadFrame(r.reader)
	if err != nil {
		t.Fatalf("radio read: %v", err)
	}
	msg := &meshtasticpb.ToRadio{}
	if err := proto.Unmarshal(payload, msg); err != nil {
		t.Fatalf("unmarshal ToRadio: %v", err)
	}
	return msg
}

func (r *radioSim) expectNothing(t *testing.T, timeout time.Duration) {
	t.Helper()
	_ = r.radio.SetReadDeadline(time.Now().Add(timeout))
	defer r.radio.SetReadDeadline(time.Time{})
	buf := make([]byte, 1)
	n, err := r.radio.Read(buf)
	if err == nil {
		t.Fatalf("unexpected data on serial: %x (n=%d)", buf[:n], n)
	}
}

func (r *radioSim) close() {
	_ = r.broker.Close()
	_ = r.radio.Close()
}

// TestBrokerMultiClientLifecycle verifies the full client-sync contract:
// primary tracking, cached config for secondaries, cross-client broadcast,
// sender-excluded echo, and primary promotion on disconnect.
func TestBrokerMultiClientLifecycle(t *testing.T) {
	radio := newRadioSim()
	defer radio.close()

	b := New(radio.broker, false, false, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- b.Run(ctx) }()
	defer func() {
		cancel()
		<-runDone
	}()

	primary := dialBroker(t, b)
	defer primary.close()
	secondary := dialBroker(t, b)
	defer secondary.close()
	tertiary := dialBroker(t, b)
	defer tertiary.close()
	// Let all AddClient goroutines register before proceeding.
	time.Sleep(30 * time.Millisecond)

	// Pre-populate cache as if the radio had already answered an earlier
	// WantConfigId from the primary.
	myInfoFrame := &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_MyInfo{MyInfo: &meshtasticpb.MyNodeInfo{MyNodeNum: 1234}},
	}
	myInfoPayload, err := proto.Marshal(myInfoFrame)
	if err != nil {
		t.Fatalf("marshal myInfo: %v", err)
	}
	b.cache.update(myInfoFrame, myInfoPayload)

	// Secondary sends WantConfigId → must be served from cache, never
	// reaches serial.
	secondary.sendToRadio(t, &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_WantConfigId{WantConfigId: 55},
	})

	first := secondary.readFromRadio(t, time.Second)
	if first.GetMyInfo().GetMyNodeNum() != 1234 {
		t.Fatalf("expected cached MyInfo for secondary, got %v", first)
	}
	last := secondary.readFromRadio(t, time.Second)
	if last.GetConfigCompleteId() != 55 {
		t.Fatalf("expected config_complete_id=55, got %d", last.GetConfigCompleteId())
	}
	radio.expectNothing(t, 100*time.Millisecond)

	// Radio-originated FromRadio packet must reach all three clients.
	nodeInfo := &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_NodeInfo{
			NodeInfo: &meshtasticpb.NodeInfo{Num: 999},
		},
	}
	radio.write(t, nodeInfo)

	for name, cl := range map[string]*testClientConn{
		"primary":   primary,
		"secondary": secondary,
		"tertiary":  tertiary,
	} {
		msg := cl.readFromRadio(t, time.Second)
		if msg.GetNodeInfo().GetNum() != 999 {
			t.Fatalf("%s: expected node_info=999, got %v", name, msg)
		}
	}

	// Tertiary sends an outgoing MeshPacket; broker must forward to serial
	// and echo to primary + secondary but NOT back to tertiary.
	tertiary.sendToRadio(t, &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_Packet{Packet: &meshtasticpb.MeshPacket{Id: 4242}},
	})

	outbound := radio.readToRadio(t, time.Second)
	if outbound.GetPacket().GetId() != 4242 {
		t.Fatalf("radio did not receive packet id=4242, got %d", outbound.GetPacket().GetId())
	}

	for name, cl := range map[string]*testClientConn{
		"primary":   primary,
		"secondary": secondary,
	} {
		echo := cl.readFromRadio(t, time.Second)
		if echo.GetPacket().GetId() != 4242 {
			t.Fatalf("%s: expected echo id=4242, got %v", name, echo)
		}
	}
	tertiary.expectNoFromRadio(t, 150*time.Millisecond)

	// Primary disconnects → secondary or tertiary should be promoted.
	primary.close()
	time.Sleep(150 * time.Millisecond)

	b.clientsMu.RLock()
	newPrimary := b.primary
	remaining := len(b.clients)
	b.clientsMu.RUnlock()
	if newPrimary == nil {
		t.Fatalf("after primary disconnect, a new primary must be promoted")
	}
	if remaining != 2 {
		t.Fatalf("expected 2 remaining clients, got %d", remaining)
	}
}

func TestBrokerSerialInvalidFrameIsNonFatal(t *testing.T) {
	radio := newRadioSim()
	defer radio.close()

	b := New(radio.broker, false, false, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- b.Run(ctx) }()
	defer func() {
		cancel()
		<-runDone
	}()

	client := dialBroker(t, b)
	defer client.close()
	time.Sleep(20 * time.Millisecond)

	// Invalid frame: magic bytes followed by zero length. The broker must
	// log a warning and keep reading the next frame instead of tearing the
	// serial connection down.
	if _, err := radio.radio.Write([]byte{frame.Magic0, frame.Magic1, 0x00, 0x00}); err != nil {
		t.Fatalf("write bad frame: %v", err)
	}

	radio.write(t, &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_Packet{Packet: &meshtasticpb.MeshPacket{Id: 7}},
	})

	got := client.readFromRadio(t, 2*time.Second)
	if got.GetPacket().GetId() != 7 {
		t.Fatalf("expected recovery to deliver id=7, got %v", got)
	}

	select {
	case err := <-runDone:
		t.Fatalf("broker exited unexpectedly: %v", err)
	default:
	}
}
