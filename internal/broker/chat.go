package broker

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/proto"
)

const (
	// BroadcastNode is Meshtastic's broadcast destination (channel message).
	BroadcastNode = 0xFFFFFFFF
	maxTextLen    = 200
)

var (
	ErrNodeIDUnknown   = errors.New("local node id not available yet; wait for radio config sync")
	ErrEmptyMessage    = errors.New("message text is empty")
	ErrMessageTooLong  = errors.New("message exceeds maximum length")
	ErrSerialNotReady  = errors.New("serial link not ready")
)

func (c *configCache) localNodeNum() (uint32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.myInfo) == 0 {
		return 0, false
	}
	frame := &meshtasticpb.FromRadio{}
	if err := proto.Unmarshal(c.myInfo, frame); err != nil {
		return 0, false
	}
	num := frame.GetMyInfo().GetMyNodeNum()
	return num, num != 0
}

// LocalNodeNum returns this radio's node number from the config cache.
func (b *Broker) LocalNodeNum() (uint32, bool) {
	return b.cache.localNodeNum()
}

func (b *Broker) nextPacketID() uint32 {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 1
	}
	id := binary.BigEndian.Uint32(buf[:])
	if id == 0 {
		id = 1
	}
	return id
}

// SendTextMessage sends a decoded text message to the radio and echoes it to TCP clients.
func (b *Broker) SendTextMessage(channelIndex int32, to uint32, text string) (uint32, error) {
	select {
	case <-b.done:
		return 0, ErrSerialNotReady
	default:
	}

	text = strings.TrimSpace(text)
	if text == "" {
		return 0, ErrEmptyMessage
	}
	if len(text) > maxTextLen {
		return 0, fmt.Errorf("%w (%d bytes, max %d)", ErrMessageTooLong, len(text), maxTextLen)
	}
	if channelIndex < 0 {
		return 0, fmt.Errorf("invalid channel index %d", channelIndex)
	}

	from, ok := b.LocalNodeNum()
	if !ok {
		return 0, ErrNodeIDUnknown
	}

	pkt := &meshtasticpb.MeshPacket{
		From:    from,
		To:      to,
		Channel: uint32(channelIndex),
		Id:      b.nextPacketID(),
		HopLimit: 3,
		WantAck: true,
		PayloadVariant: &meshtasticpb.MeshPacket_Decoded{
			Decoded: &meshtasticpb.Data{
				Portnum: meshtasticpb.PortNum_TEXT_MESSAGE_APP,
				Payload: []byte(text),
			},
		},
	}

	toRadio := &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_Packet{Packet: pkt},
	}
	if err := b.forwardToSerial(toRadio); err != nil {
		return 0, err
	}

	fromRadio := &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_Packet{Packet: pkt},
	}
	if data, err := proto.Marshal(fromRadio); err == nil {
		b.broadcast(data)
	}
	return pkt.GetId(), nil
}
