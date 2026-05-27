package broker

import (
	"context"
	"errors"
	"strings"
	"time"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/proto"
)

const cannedFetchTimeout = 4 * time.Second

var ErrCannedUnavailable = errors.New("canned messages unavailable")

type cannedWait struct {
	ch chan cannedWaitResult
}

type cannedWaitResult struct {
	messages []string
}

// ParseCannedMessages splits the firmware pipe-delimited canned message blob.
func ParseCannedMessages(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, "|")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func extractCannedFromPacket(pkt *meshtasticpb.MeshPacket) (string, bool) {
	if pkt == nil {
		return "", false
	}
	data := pkt.GetDecoded()
	if data == nil || data.GetPortnum() != meshtasticpb.PortNum_ADMIN_APP {
		return "", false
	}
	admin := &meshtasticpb.AdminMessage{}
	if err := proto.Unmarshal(data.GetPayload(), admin); err != nil {
		return "", false
	}
	switch v := admin.GetPayloadVariant().(type) {
	case *meshtasticpb.AdminMessage_GetCannedMessageModuleMessagesResponse:
		return v.GetCannedMessageModuleMessagesResponse, true
	default:
		return "", false
	}
}

func (b *Broker) registerCannedWait(id uint32) chan cannedWaitResult {
	ch := make(chan cannedWaitResult, 1)
	b.cannedMu.Lock()
	if b.cannedWaiters == nil {
		b.cannedWaiters = make(map[uint32]cannedWait)
	}
	b.cannedWaiters[id] = cannedWait{ch: ch}
	b.cannedMu.Unlock()
	return ch
}

func (b *Broker) unregisterCannedWait(id uint32) {
	b.cannedMu.Lock()
	delete(b.cannedWaiters, id)
	b.cannedMu.Unlock()
}

func (b *Broker) tryDeliverCanned(pkt *meshtasticpb.MeshPacket) bool {
	raw, ok := extractCannedFromPacket(pkt)
	if !ok {
		return false
	}
	id := pkt.GetId()
	b.cannedMu.Lock()
	wait, found := b.cannedWaiters[id]
	b.cannedMu.Unlock()
	if !found {
		return false
	}
	msgs := ParseCannedMessages(raw)
	select {
	case wait.ch <- cannedWaitResult{messages: msgs}:
	default:
	}
	b.storeCannedCache(msgs)
	return true
}

func (b *Broker) storeCannedCache(msgs []string) {
	b.cannedMu.Lock()
	b.cannedCache = append([]string(nil), msgs...)
	b.cannedCacheAt = time.Now()
	b.cannedMu.Unlock()
}

func (b *Broker) cachedCanned() []string {
	b.cannedMu.Lock()
	defer b.cannedMu.Unlock()
	if len(b.cannedCache) == 0 {
		return nil
	}
	if time.Since(b.cannedCacheAt) > 2*time.Minute {
		b.cannedCache = nil
		return nil
	}
	out := make([]string, len(b.cannedCache))
	copy(out, b.cannedCache)
	return out
}

// FetchCannedMessages returns predefined canned messages from the radio via AdminMessage.
func (b *Broker) FetchCannedMessages(ctx context.Context) ([]string, error) {
	if cached := b.cachedCanned(); len(cached) > 0 {
		return cached, nil
	}

	select {
	case <-b.done:
		return nil, ErrSerialNotReady
	default:
	}

	from, ok := b.LocalNodeNum()
	if !ok {
		return nil, ErrNodeIDUnknown
	}

	pktID := b.nextPacketID()
	waitCh := b.registerCannedWait(pktID)
	defer b.unregisterCannedWait(pktID)

	admin := &meshtasticpb.AdminMessage{
		PayloadVariant: &meshtasticpb.AdminMessage_GetCannedMessageModuleMessagesRequest{
			GetCannedMessageModuleMessagesRequest: true,
		},
	}
	adminPayload, err := proto.Marshal(admin)
	if err != nil {
		return nil, err
	}

	pkt := &meshtasticpb.MeshPacket{
		From: from,
		To:   0,
		Id:   pktID,
		PayloadVariant: &meshtasticpb.MeshPacket_Decoded{
			Decoded: &meshtasticpb.Data{
				Portnum:      meshtasticpb.PortNum_ADMIN_APP,
				Payload:      adminPayload,
				WantResponse: true,
			},
		},
	}
	toRadio := &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_Packet{Packet: pkt},
	}
	if err := b.forwardToSerial(toRadio); err != nil {
		return nil, err
	}

	timeout := cannedFetchTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.done:
		return nil, ErrSerialNotReady
	case <-timer.C:
		return nil, ErrCannedUnavailable
	case res := <-waitCh:
		if len(res.messages) == 0 {
			return nil, ErrCannedUnavailable
		}
		return res.messages, nil
	}
}

func (b *Broker) initCannedState() {
	b.cannedWaiters = make(map[uint32]cannedWait)
}
