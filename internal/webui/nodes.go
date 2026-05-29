package webui

import (
	"encoding/json"
	"time"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
)

const nodeOnlineWindow = 2 * time.Hour

// NodeView is a mesh node as observed from pass-through serial traffic.
type NodeView struct {
	Num          uint32    `json:"num"`
	UserID       string    `json:"user_id,omitempty"`
	LongName     string    `json:"long_name,omitempty"`
	ShortName    string    `json:"short_name,omitempty"`
	HwModel      string    `json:"hw_model,omitempty"`
	Role         string    `json:"role,omitempty"`
	Snr          float32   `json:"snr,omitempty"`
	LastHeard    uint32    `json:"last_heard,omitempty"`
	LastActivity time.Time `json:"last_activity,omitempty"`
	HopsAway     *uint32   `json:"hops_away,omitempty"`
	ViaMqtt      bool      `json:"via_mqtt,omitempty"`
	IsFavorite   bool      `json:"is_favorite,omitempty"`
	IsIgnored    bool      `json:"is_ignored,omitempty"`
	IsLocal      bool      `json:"is_local,omitempty"`
	BatteryLevel *uint32   `json:"battery_level,omitempty"`
	Latitude     *float64  `json:"latitude,omitempty"`
	Longitude    *float64  `json:"longitude,omitempty"`
	Altitude     *int32    `json:"altitude,omitempty"`
	Online       bool      `json:"online"`
	Updated      time.Time `json:"updated"`
}

func (h *Hub) SetLocalNodeNum(num uint32) {
	if h == nil || num == 0 {
		return
	}
	h.mu.Lock()
	h.localNodeNum = num
	if n, ok := h.nodes[num]; ok {
		n.IsLocal = true
		h.nodes[num] = n
	}
	h.mu.Unlock()
	h.notifyNodes()
}

func (h *Hub) UpdateNodeFromInfo(info *meshtasticpb.NodeInfo) {
	if h == nil || info == nil || info.GetNum() == 0 {
		return
	}
	now := time.Now().UTC()
	num := info.GetNum()

	h.mu.Lock()
	if h.nodes == nil {
		h.nodes = make(map[uint32]NodeView)
	}
	n := h.nodes[num]
	n.Num = num
	n.Updated = now
	if n.LastActivity.IsZero() {
		n.LastActivity = now
	}
	if info.GetLastHeard() != 0 {
		n.LastHeard = info.GetLastHeard()
	}
	if info.GetSnr() != 0 {
		n.Snr = info.GetSnr()
	}
	n.HopsAway = info.HopsAway
	n.ViaMqtt = info.GetViaMqtt()
	n.IsFavorite = info.GetIsFavorite()
	n.IsIgnored = info.GetIsIgnored()
	n.IsLocal = num == h.localNodeNum

	if u := info.GetUser(); u != nil {
		if id := u.GetId(); id != "" {
			n.UserID = id
		}
		if name := u.GetLongName(); name != "" {
			n.LongName = name
		}
		if short := u.GetShortName(); short != "" {
			n.ShortName = short
		}
		if model := u.GetHwModel(); model != meshtasticpb.HardwareModel_UNSET {
			n.HwModel = model.String()
		}
		n.Role = u.GetRole().String()
	}
	if pos := info.GetPosition(); pos != nil {
		if pos.LatitudeI != nil {
			lat := float64(*pos.LatitudeI) * 1e-7
			n.Latitude = &lat
		}
		if pos.LongitudeI != nil {
			lon := float64(*pos.LongitudeI) * 1e-7
			n.Longitude = &lon
		}
		if pos.Altitude != nil {
			n.Altitude = pos.Altitude
		}
	}
	if dm := info.GetDeviceMetrics(); dm != nil && dm.BatteryLevel != nil {
		n.BatteryLevel = dm.BatteryLevel
	}

	n.Online = nodeOnline(n, now)
	h.nodes[num] = n
	h.mu.Unlock()
	h.notifyNodes()
}

func (h *Hub) touchNodeActivity(num uint32, snr float32, at time.Time) {
	if h == nil || num == 0 || num == 0xFFFFFFFF {
		return
	}
	h.mu.Lock()
	if h.nodes == nil {
		h.nodes = make(map[uint32]NodeView)
	}
	n := h.nodes[num]
	if n.Num == 0 {
		n.Num = num
	}
	n.LastActivity = at
	n.Updated = at
	if snr != 0 {
		n.Snr = snr
	}
	n.IsLocal = num == h.localNodeNum
	n.Online = nodeOnline(n, at)
	h.nodes[num] = n
	h.mu.Unlock()
	h.notifyNodes()
}

func nodeOnline(n NodeView, now time.Time) bool {
	if ts := activityUnix(n); ts != 0 {
		return now.Sub(time.Unix(int64(ts), 0)) <= nodeOnlineWindow
	}
	return false
}

func activityUnix(n NodeView) uint32 {
	if !n.LastActivity.IsZero() {
		return uint32(n.LastActivity.Unix())
	}
	return n.LastHeard
}

func (h *Hub) SnapshotNodes() []NodeView {
	h.mu.RLock()
	defer h.mu.RUnlock()
	now := time.Now().UTC()
	out := make([]NodeView, 0, len(h.nodes))
	for _, n := range h.nodes {
		n.IsLocal = n.Num == h.localNodeNum
		n.Online = nodeOnline(n, now)
		out = append(out, n)
	}
	return out
}

func (h *Hub) SubscribeNodes(buffer int) (<-chan []NodeView, func()) {
	if buffer <= 0 {
		buffer = 16
	}
	ch := make(chan []NodeView, buffer)
	h.mu.Lock()
	if h.nodeSubs == nil {
		h.nodeSubs = make(map[chan []NodeView]struct{})
	}
	h.nodeSubs[ch] = struct{}{}
	h.mu.Unlock()

	unsub := func() {
		h.mu.Lock()
		delete(h.nodeSubs, ch)
		h.mu.Unlock()
		close(ch)
	}
	return ch, unsub
}

func (h *Hub) MarshalNodes(nodes []NodeView) ([]byte, error) {
	type payload struct {
		Nodes []NodeView `json:"nodes"`
	}
	return json.Marshal(payload{Nodes: nodes})
}

func (h *Hub) notifyNodes() {
	nodes := h.SnapshotNodes()
	h.mu.RLock()
	subs := make([]chan []NodeView, 0, len(h.nodeSubs))
	for ch := range h.nodeSubs {
		subs = append(subs, ch)
	}
	h.mu.RUnlock()
	for _, ch := range subs {
		select {
		case ch <- nodes:
		default:
		}
	}
}

func nodeInfoSummary(info *meshtasticpb.NodeInfo) string {
	if info == nil {
		return "NodeInfo"
	}
	user := info.GetUser()
	name := user.GetLongName()
	if name == "" {
		name = user.GetShortName()
	}
	if name != "" {
		return "NodeInfo " + name
	}
	return "NodeInfo"
}
