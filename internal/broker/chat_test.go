package broker

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/frame"
	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/proto"
)

func TestSendTextMessage(t *testing.T) {
	brokerEnd, serialEnd := net.Pipe()
	b := New(brokerEnd, false, false, nil)

	myInfo := &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_MyInfo{
			MyInfo: &meshtasticpb.MyNodeInfo{MyNodeNum: 0xABCD},
		},
	}
	payload, err := proto.Marshal(myInfo)
	if err != nil {
		t.Fatal(err)
	}
	b.cache.update(myInfo, payload)

	done := make(chan struct{})
	go func() {
		defer close(done)
		reader := bufio.NewReader(serialEnd)
		raw, err := frame.ReadFrame(reader)
		if err != nil {
			t.Errorf("read frame: %v", err)
			return
		}
		toRadio := &meshtasticpb.ToRadio{}
		if err := proto.Unmarshal(raw, toRadio); err != nil {
			t.Errorf("unmarshal: %v", err)
			return
		}
		pkt := toRadio.GetPacket()
		if pkt == nil {
			t.Error("expected packet")
			return
		}
		if pkt.GetTo() != BroadcastNode {
			t.Fatalf("to: got 0x%x want broadcast", pkt.GetTo())
		}
		if string(pkt.GetDecoded().GetPayload()) != "hello mesh" {
			t.Fatalf("payload: %q", pkt.GetDecoded().GetPayload())
		}
	}()

	id, err := b.SendTextMessage(0, BroadcastNode, "hello mesh")
	if err != nil {
		t.Fatalf("SendTextMessage: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero packet id")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for serial frame")
	}
	_ = serialEnd.Close()
	_ = brokerEnd.Close()
}

func TestSendTextMessageRequiresNodeID(t *testing.T) {
	brokerEnd, _ := net.Pipe()
	defer brokerEnd.Close()
	b := New(brokerEnd, false, false, nil)
	_, err := b.SendTextMessage(0, BroadcastNode, "hi")
	if err != ErrNodeIDUnknown {
		t.Fatalf("got %v want ErrNodeIDUnknown", err)
	}
}

func TestLocalNodeNum(t *testing.T) {
	brokerEnd, _ := net.Pipe()
	defer brokerEnd.Close()
	b := New(brokerEnd, false, false, nil)
	if _, ok := b.LocalNodeNum(); ok {
		t.Fatal("expected missing node id")
	}
	frame := &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_MyInfo{
			MyInfo: &meshtasticpb.MyNodeInfo{MyNodeNum: 42},
		},
	}
	data, _ := proto.Marshal(frame)
	b.cache.update(frame, data)
	num, ok := b.LocalNodeNum()
	if !ok || num != 42 {
		t.Fatalf("got %d ok=%v", num, ok)
	}
}
