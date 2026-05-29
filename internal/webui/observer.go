package webui

import (
	"time"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/proto"
)

// ObservePayload records a decoded protobuf frame for the web UI.
func (h *Hub) ObservePayload(direction, addr string, payload []byte, fromRadio bool) {
	if h == nil || len(payload) == 0 {
		return
	}

	if fromRadio {
		frame := &meshtasticpb.FromRadio{}
		if err := proto.Unmarshal(payload, frame); err != nil {
			return
		}
		h.observeFromRadio(direction, addr, frame)
		return
	}

	frame := &meshtasticpb.ToRadio{}
	if err := proto.Unmarshal(payload, frame); err != nil {
		return
	}
	h.observeToRadio(direction, addr, frame)
}

func (h *Hub) observeFromRadio(direction, addr string, frame *meshtasticpb.FromRadio) {
	ev := Event{
		Direction: direction,
		Addr:      addr,
		Variant:   protoVariantLabel(frame),
		JSON:      marshalProtoJSON(frame),
	}

	switch v := frame.GetPayloadVariant().(type) {
	case *meshtasticpb.FromRadio_Channel:
		if ch := v.Channel; ch != nil {
			name := ""
			role := ch.GetRole().String()
			if ch.GetSettings() != nil {
				name = ch.GetSettings().GetName()
			}
			h.UpdateChannel(ch.GetIndex(), name, role)
			ev.Category = "channel"
			ev.ChannelIndex = ptrInt32(ch.GetIndex())
			ev.ChannelName = name
			ev.Summary = "channel definition"
		}
	case *meshtasticpb.FromRadio_Packet:
		ev.Category = "packet"
		h.enrichPacketEvent(&ev, v.Packet)
		h.cachePacketProto(v.Packet)
		h.tryRecordChatPacket(v.Packet)
	case *meshtasticpb.FromRadio_NodeInfo:
		if ni := v.NodeInfo; ni != nil {
			h.UpdateNodeFromInfo(ni)
			ev.Category = "config"
			ev.Summary = nodeInfoSummary(ni)
		}
	case *meshtasticpb.FromRadio_MyInfo:
		if mi := v.MyInfo; mi != nil {
			h.SetLocalNodeNum(mi.GetMyNodeNum())
			ev.Category = "config"
			ev.Summary = "MyInfo"
		}
	case *meshtasticpb.FromRadio_Config,
		*meshtasticpb.FromRadio_ModuleConfig,
		*meshtasticpb.FromRadio_ConfigCompleteId,
		*meshtasticpb.FromRadio_Metadata,
		*meshtasticpb.FromRadio_DeviceuiConfig:
		ev.Category = "config"
	default:
		ev.Category = "debug"
	}

	h.Record(ev)
}

func (h *Hub) observeToRadio(direction, addr string, frame *meshtasticpb.ToRadio) {
	ev := Event{
		Direction: direction,
		Addr:      addr,
		Variant:   protoVariantLabelTo(frame),
		JSON:      marshalProtoJSON(frame),
	}

	switch v := frame.GetPayloadVariant().(type) {
	case *meshtasticpb.ToRadio_Packet:
		ev.Category = "packet"
		h.enrichPacketEvent(&ev, v.Packet)
		h.cachePacketProto(v.Packet)
		h.tryRecordChatPacket(v.Packet)
	case *meshtasticpb.ToRadio_WantConfigId:
		ev.Category = "config"
		ev.Summary = "WantConfigId"
	default:
		ev.Category = "debug"
	}

	h.Record(ev)
}

func (h *Hub) enrichPacketEvent(ev *Event, pkt *meshtasticpb.MeshPacket) {
	if pkt == nil {
		return
	}
	idx := int32(pkt.GetChannel())
	ev.ChannelIndex = &idx
	h.mu.RLock()
	if ch, ok := h.channels[idx]; ok {
		ev.ChannelName = ch.Name
	}
	h.mu.RUnlock()
	if data := pkt.GetDecoded(); data != nil {
		ev.Summary = packetSummary(data)
	}
	at := ev.Time
	if at.IsZero() {
		at = time.Now().UTC()
	}
	h.touchNodeActivity(pkt.GetFrom(), pkt.GetRxSnr(), at)
}

func packetSummary(data *meshtasticpb.Data) string {
	if data == nil {
		return ""
	}
	port := data.GetPortnum().String()
	payload := data.GetPayload()
	if len(payload) == 0 {
		return port
	}
	if isTextPort(data.GetPortnum()) && isUTF8(payload) {
		return port + ": " + string(payload)
	}
	return port
}

func isTextPort(p meshtasticpb.PortNum) bool {
	switch p {
	case meshtasticpb.PortNum_TEXT_MESSAGE_APP,
		meshtasticpb.PortNum_REPLY_APP,
		meshtasticpb.PortNum_DETECTION_SENSOR_APP,
		meshtasticpb.PortNum_ALERT_APP:
		return true
	default:
		return false
	}
}

func isUTF8(b []byte) bool {
	for i := 0; i < len(b); i++ {
		if b[i] < 0x20 && b[i] != '\n' && b[i] != '\r' && b[i] != '\t' {
			return false
		}
	}
	return true
}

func ptrInt32(v int32) *int32 { return &v }

func (h *Hub) tryRecordChatPacket(pkt *meshtasticpb.MeshPacket) {
	if h == nil || pkt == nil {
		return
	}
	var local uint32
	if r := h.radio(); r != nil {
		local, _ = r.LocalNodeNum()
	}
	h.TryRecordChatFromPacket(pkt, local)
}
