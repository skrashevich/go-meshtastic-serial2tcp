package webui

import meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"

func (h *Hub) cachePacketProto(pkt *meshtasticpb.MeshPacket) {
	if h == nil || pkt == nil {
		return
	}
	id := pkt.GetId()
	if id == 0 {
		return
	}
	json := marshalProtoJSONFull(pkt)
	if json == "" {
		return
	}
	h.mu.Lock()
	if h.packetProtos == nil {
		h.packetProtos = make(map[uint32]string)
	}
	h.packetProtos[id] = json
	if len(h.packetProtos) > maxPacketProtoCache {
		for k := range h.packetProtos {
			delete(h.packetProtos, k)
			if len(h.packetProtos) <= maxPacketProtoCache/2 {
				break
			}
		}
	}
	h.mu.Unlock()
}

func (h *Hub) protoJSONForPacket(pkt *meshtasticpb.MeshPacket) string {
	if pkt == nil {
		return ""
	}
	id := pkt.GetId()
	if id != 0 {
		h.mu.RLock()
		if s, ok := h.packetProtos[id]; ok {
			h.mu.RUnlock()
			return s
		}
		h.mu.RUnlock()
	}
	return marshalProtoJSONFull(pkt)
}

func (h *Hub) lookupPacketProto(id uint32) string {
	if id == 0 {
		return ""
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.packetProtos[id]
}
