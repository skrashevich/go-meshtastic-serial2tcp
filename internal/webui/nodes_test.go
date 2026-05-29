package webui

import (
	"testing"
	"time"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/proto"
)

func TestUpdateNodeFromInfo(t *testing.T) {
	h := NewHub()
	h.SetLocalNodeNum(0xABCD)
	h.UpdateNodeFromInfo(&meshtasticpb.NodeInfo{
		Num:       0x1234,
		LastHeard: uint32(time.Now().Unix()),
		Snr:       7.5,
		User: &meshtasticpb.User{
			Id:        "!00001234",
			LongName:  "Alpha",
			ShortName: "A1",
		},
	})

	nodes := h.SnapshotNodes()
	if len(nodes) != 1 {
		t.Fatalf("nodes: got %d want 1", len(nodes))
	}
	n := nodes[0]
	if n.LongName != "Alpha" || n.Num != 0x1234 {
		t.Fatalf("unexpected node: %+v", n)
	}
	if !n.Online {
		t.Fatal("expected node online")
	}
}

func TestTouchNodeActivity(t *testing.T) {
	h := NewHub()
	h.touchNodeActivity(0xBEEF, 3.2, time.Now().UTC())

	nodes := h.SnapshotNodes()
	if len(nodes) != 1 || nodes[0].Num != 0xBEEF {
		t.Fatalf("unexpected nodes: %+v", nodes)
	}
	if nodes[0].Snr != 3.2 {
		t.Fatalf("snr: got %v", nodes[0].Snr)
	}
}

func TestObserveFromRadioNodeInfo(t *testing.T) {
	h := NewHub()
	frame := &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_NodeInfo{
			NodeInfo: &meshtasticpb.NodeInfo{
				Num:  0x42,
				User: &meshtasticpb.User{LongName: "Peer"},
			},
		},
	}
	payload, err := proto.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	h.ObservePayload("serial -> broker", "", payload, true)

	nodes := h.SnapshotNodes()
	if len(nodes) != 1 || nodes[0].LongName != "Peer" {
		t.Fatalf("nodes: %+v", nodes)
	}
}

func TestObserveFromRadioPacketUpdatesActivity(t *testing.T) {
	h := NewHub()
	frame := &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_Packet{
			Packet: &meshtasticpb.MeshPacket{
				From:   0x99,
				RxSnr:  5,
				Channel: 0,
				PayloadVariant: &meshtasticpb.MeshPacket_Decoded{
					Decoded: &meshtasticpb.Data{
						Portnum: meshtasticpb.PortNum_TEXT_MESSAGE_APP,
						Payload: []byte("hi"),
					},
				},
			},
		},
	}
	payload, err := proto.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	h.ObservePayload("serial -> broker", "", payload, true)

	nodes := h.SnapshotNodes()
	if len(nodes) != 1 || nodes[0].Num != 0x99 {
		t.Fatalf("nodes: %+v", nodes)
	}
}

func TestSubscribeNodes(t *testing.T) {
	h := NewHub()
	ch, unsub := h.SubscribeNodes(4)
	defer unsub()

	h.UpdateNodeFromInfo(&meshtasticpb.NodeInfo{Num: 1, User: &meshtasticpb.User{LongName: "One"}})
	select {
	case nodes := <-ch:
		if len(nodes) != 1 || nodes[0].LongName != "One" {
			t.Fatalf("unexpected update: %+v", nodes)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for node update")
	}
}
