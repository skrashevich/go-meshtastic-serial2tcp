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

func TestParseCannedMessages(t *testing.T) {
	got := ParseCannedMessages(" hi |there| | ")
	if len(got) != 2 || got[0] != "hi" || got[1] != "there" {
		t.Fatalf("got %v", got)
	}
}

func TestFetchCannedMessages(t *testing.T) {
	radioSide, brokerEnd := net.Pipe()
	defer radioSide.Close()
	defer brokerEnd.Close()

	b := New(brokerEnd, false, false, nil)
	myInfo := &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_MyInfo{
			MyInfo: &meshtasticpb.MyNodeInfo{MyNodeNum: 0xBEEF},
		},
	}
	payload, _ := proto.Marshal(myInfo)
	b.cache.update(myInfo, payload)

	errCh := make(chan error, 1)
	go b.readSerial(errCh)

	done := make(chan struct{})
	go func() {
		defer close(done)
		reader := bufio.NewReader(radioSide)
		raw, err := frame.ReadFrame(reader)
		if err != nil {
			t.Errorf("read: %v", err)
			return
		}
		toRadio := &meshtasticpb.ToRadio{}
		if err := proto.Unmarshal(raw, toRadio); err != nil {
			t.Errorf("unmarshal: %v", err)
			return
		}
		reqID := toRadio.GetPacket().GetId()
		resp := &meshtasticpb.FromRadio{
			PayloadVariant: &meshtasticpb.FromRadio_Packet{
				Packet: &meshtasticpb.MeshPacket{
					From: 0xBEEF,
					To:   0,
					Id:   reqID,
					PayloadVariant: &meshtasticpb.MeshPacket_Decoded{
						Decoded: &meshtasticpb.Data{
							Portnum: meshtasticpb.PortNum_ADMIN_APP,
							Payload: mustMarshal(t, &meshtasticpb.AdminMessage{
								PayloadVariant: &meshtasticpb.AdminMessage_GetCannedMessageModuleMessagesResponse{
									GetCannedMessageModuleMessagesResponse: "SOS|OK|On my way",
								},
							}),
						},
					},
				},
			},
		}
		data, _ := proto.Marshal(resp)
		_ = frame.WriteFrame(radioSide, data)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msgs, err := b.FetchCannedMessages(ctx)
	if err != nil {
		t.Fatalf("FetchCannedMessages: %v", err)
	}
	if len(msgs) != 3 || msgs[1] != "OK" {
		t.Fatalf("got %v", msgs)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("radio handler timeout")
	}
}
