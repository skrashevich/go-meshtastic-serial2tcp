package broker

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/frame"
	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/webui"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	clientSendBuffer  = 64
	clientSendTimeout = 5 * time.Second
	debugMaxDecoded   = 2048
)

var (
	ErrSerialClosed = errors.New("serial disconnected")
)

type payloadKind int

const (
	payloadUnknown payloadKind = iota
	payloadFromRadio
	payloadToRadio
)

type client struct {
	conn net.Conn
	send chan []byte
	done chan struct{}
	addr string

	warnMu         sync.Mutex
	warnedReadOnly bool
	warnedSlow     bool

	closeOnce sync.Once
}

func newClient(conn net.Conn) *client {
	return &client{
		conn: conn,
		send: make(chan []byte, clientSendBuffer),
		done: make(chan struct{}),
		addr: conn.RemoteAddr().String(),
	}
}

func (c *client) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// enqueue does a non-blocking send. It never closes the send channel so the
// caller never races with close(); instead, shutdown is signalled via c.done.
// Returns false if the client is closed or the send buffer is full.
func (c *client) enqueue(payload []byte) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.send <- payload:
		return true
	case <-c.done:
		return false
	default:
		return false
	}
}

// enqueueWithTimeout blocks up to the given timeout waiting for buffer space,
// while still respecting client shutdown (c.done) and broker shutdown.
// Because c.send is never closed, a send into it cannot panic.
func (c *client) enqueueWithTimeout(payload []byte, timeout time.Duration, brokerDone <-chan struct{}) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case c.send <- payload:
		return true
	case <-c.done:
		return false
	case <-brokerDone:
		return false
	case <-timer.C:
		return false
	}
}

func (c *client) markReadOnlyWarning() bool {
	c.warnMu.Lock()
	defer c.warnMu.Unlock()
	if c.warnedReadOnly {
		return false
	}
	c.warnedReadOnly = true
	return true
}

func (c *client) markSlowWarning() bool {
	c.warnMu.Lock()
	defer c.warnMu.Unlock()
	if c.warnedSlow {
		return false
	}
	c.warnedSlow = true
	return true
}

func (c *client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

type configCache struct {
	mu           sync.RWMutex
	myInfo       []byte
	nodeInfo     map[uint32][]byte
	config       map[string][]byte
	moduleConfig map[string][]byte
	channels     map[int32][]byte
	metadata     []byte
	deviceUI     []byte
}

func newConfigCache() *configCache {
	return &configCache{
		nodeInfo:     make(map[uint32][]byte),
		config:       make(map[string][]byte),
		moduleConfig: make(map[string][]byte),
		channels:     make(map[int32][]byte),
	}
}

func (c *configCache) update(frame *meshtasticpb.FromRadio, payload []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	clone := append([]byte(nil), payload...)
	switch v := frame.GetPayloadVariant().(type) {
	case *meshtasticpb.FromRadio_MyInfo:
		c.myInfo = clone
	case *meshtasticpb.FromRadio_NodeInfo:
		if v.NodeInfo != nil {
			c.nodeInfo[v.NodeInfo.GetNum()] = clone
		}
	case *meshtasticpb.FromRadio_Config:
		if v.Config != nil {
			key := fmt.Sprintf("%T", v.Config.GetPayloadVariant())
			c.config[key] = clone
		}
	case *meshtasticpb.FromRadio_ModuleConfig:
		if v.ModuleConfig != nil {
			key := fmt.Sprintf("%T", v.ModuleConfig.GetPayloadVariant())
			c.moduleConfig[key] = clone
		}
	case *meshtasticpb.FromRadio_Channel:
		if v.Channel != nil {
			c.channels[v.Channel.GetIndex()] = clone
		}
	case *meshtasticpb.FromRadio_Metadata:
		c.metadata = clone
	case *meshtasticpb.FromRadio_DeviceuiConfig:
		c.deviceUI = clone
	}
}

type cacheSnapshot struct {
	myInfo       []byte
	configs      [][]byte
	moduleConfig [][]byte
	channels     [][]byte
	nodeInfo     [][]byte
	metadata     []byte
	deviceUI     []byte
}

func (c *configCache) snapshot() cacheSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	snap := cacheSnapshot{
		myInfo:   append([]byte(nil), c.myInfo...),
		metadata: append([]byte(nil), c.metadata...),
		deviceUI: append([]byte(nil), c.deviceUI...),
	}

	configKeys := make([]string, 0, len(c.config))
	for key := range c.config {
		configKeys = append(configKeys, key)
	}
	sort.Strings(configKeys)
	for _, key := range configKeys {
		snap.configs = append(snap.configs, append([]byte(nil), c.config[key]...))
	}

	moduleKeys := make([]string, 0, len(c.moduleConfig))
	for key := range c.moduleConfig {
		moduleKeys = append(moduleKeys, key)
	}
	sort.Strings(moduleKeys)
	for _, key := range moduleKeys {
		snap.moduleConfig = append(snap.moduleConfig, append([]byte(nil), c.moduleConfig[key]...))
	}

	channelKeys := make([]int32, 0, len(c.channels))
	for key := range c.channels {
		channelKeys = append(channelKeys, key)
	}
	sort.Slice(channelKeys, func(i, j int) bool { return channelKeys[i] < channelKeys[j] })
	for _, key := range channelKeys {
		snap.channels = append(snap.channels, append([]byte(nil), c.channels[key]...))
	}

	nodeKeys := make([]uint32, 0, len(c.nodeInfo))
	for key := range c.nodeInfo {
		nodeKeys = append(nodeKeys, key)
	}
	sort.Slice(nodeKeys, func(i, j int) bool { return nodeKeys[i] < nodeKeys[j] })
	for _, key := range nodeKeys {
		snap.nodeInfo = append(snap.nodeInfo, append([]byte(nil), c.nodeInfo[key]...))
	}

	return snap
}

func (c *configCache) empty() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.myInfo == nil && len(c.nodeInfo) == 0 && len(c.config) == 0 &&
		len(c.moduleConfig) == 0 && len(c.channels) == 0 && c.metadata == nil && c.deviceUI == nil
}

// ready reports whether the cache holds a complete device config dump.
// NodeInfo alone can arrive from live mesh traffic before the initial
// WantConfigId handshake finishes; serving that partial state breaks
// clients waiting for myInfo/channels (--info, --get 0, etc.).
func (c *configCache) ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.myInfo != nil
}

func (c *configCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.myInfo = nil
	c.nodeInfo = make(map[uint32][]byte)
	c.config = make(map[string][]byte)
	c.moduleConfig = make(map[string][]byte)
	c.channels = make(map[int32][]byte)
	c.metadata = nil
	c.deviceUI = nil
}

type configRequest struct {
	client     *client
	originalID uint32
}

const (
	outboundEchoSuppress      = 30 * time.Second
	brokerBootstrapConfigID   = 1
)

// bootstrapClientAddr labels broker-initiated WantConfigId traffic in logs
// and observability when no TCP client owns the handshake.
const bootstrapClientAddr = "bootstrap"

type outboundEntry struct {
	client *client
	until  time.Time
}

type Broker struct {
	// serial is the device-side endpoint. The concrete type is usually
	// *os.File opened by the serial package, but any io.ReadWriter works —
	// this keeps the broker testable with an in-memory net.Pipe or similar
	// bidirectional plumbing.
	serial          io.ReadWriter
	serialMu        sync.Mutex
	clients         map[*client]struct{}
	clientsMu       sync.RWMutex
	primary         *client
	cache           *configCache
	readOnlyClients bool
	debug           bool
	observability   *webui.Hub

	pendingMu     sync.Mutex
	pendingConfig map[uint32]configRequest

	outboundMu       sync.Mutex
	outboundByPacket map[uint32]outboundEntry

	cannedMu      sync.Mutex
	cannedWaiters map[uint32]cannedWait
	cannedCache   []string
	cannedCacheAt time.Time

	done    chan struct{}
	errOnce sync.Once
	err     error
}

func (b *Broker) noteOutboundPacket(cl *client, pkt *meshtasticpb.MeshPacket) {
	if pkt == nil || pkt.GetId() == 0 {
		return
	}
	b.outboundMu.Lock()
	defer b.outboundMu.Unlock()
	now := time.Now()
	for id, entry := range b.outboundByPacket {
		if now.After(entry.until) {
			delete(b.outboundByPacket, id)
		}
	}
	b.outboundByPacket[pkt.GetId()] = outboundEntry{
		client: cl,
		until:  now.Add(outboundEchoSuppress),
	}
}

func (b *Broker) consumeOutboundOrigin(pkt *meshtasticpb.MeshPacket) *client {
	if pkt == nil || pkt.GetId() == 0 {
		return nil
	}
	b.outboundMu.Lock()
	defer b.outboundMu.Unlock()
	entry, ok := b.outboundByPacket[pkt.GetId()]
	if !ok {
		return nil
	}
	delete(b.outboundByPacket, pkt.GetId())
	if time.Now().After(entry.until) || entry.client == nil || entry.client.isClosed() {
		return nil
	}
	return entry.client
}

func New(serial io.ReadWriter, readOnlyClients bool, debug bool, observability *webui.Hub) *Broker {
	b := &Broker{
		serial:          serial,
		clients:         make(map[*client]struct{}),
		cache:           newConfigCache(),
		readOnlyClients: readOnlyClients,
		debug:           debug,
		observability:   observability,
		pendingConfig:   make(map[uint32]configRequest),
		outboundByPacket: make(map[uint32]outboundEntry),
		done:            make(chan struct{}),
	}
	b.initCannedState()
	return b
}

func (b *Broker) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go b.readSerial(errCh)
	if b.observability != nil {
		go b.bootstrapConfig()
	}

	select {
	case <-ctx.Done():
		b.fail(ctx.Err())
	case err := <-errCh:
		b.fail(err)
	}

	<-b.done
	return b.err
}

func (b *Broker) fail(err error) {
	b.errOnce.Do(func() {
		if err == nil {
			err = ErrSerialClosed
		}
		b.err = err
		close(b.done)
	})
}

func (b *Broker) logDecodedPayload(direction, addr string, payload []byte, kind payloadKind) {
	if b.observability != nil {
		switch kind {
		case payloadFromRadio:
			b.observability.ObservePayload(direction, addr, payload, true)
		case payloadToRadio:
			b.observability.ObservePayload(direction, addr, payload, false)
		}
	}
	if !b.debug {
		return
	}
	var msg proto.Message
	switch kind {
	case payloadFromRadio:
		frame := &meshtasticpb.FromRadio{}
		if err := proto.Unmarshal(payload, frame); err != nil {
			return
		}
		msg = frame
	case payloadToRadio:
		frame := &meshtasticpb.ToRadio{}
		if err := proto.Unmarshal(payload, frame); err != nil {
			return
		}
		msg = frame
	default:
		return
	}

	data, err := protojson.MarshalOptions{
		UseProtoNames: true,
		Multiline:     false,
	}.Marshal(msg)
	if err != nil {
		return
	}

	if decodedPayload, ok := describeMeshPayload(msg); ok {
		if updated, err := injectDecodedPayloadJSON(data, decodedPayload); err == nil {
			data = updated
		}
	}

	decoded := string(data)
	truncated := ""
	if len(decoded) > debugMaxDecoded {
		decoded = decoded[:debugMaxDecoded]
		truncated = fmt.Sprintf(" ...(truncated %d chars)", len(decoded)-debugMaxDecoded)
	}
	label := direction
	if addr != "" {
		label = fmt.Sprintf("%s [%s]", direction, addr)
	}
	variant := protoVariantLabel(msg)
	if variant != "" {
		label = fmt.Sprintf("%s <%s>", label, variant)
	}
	log.Printf("%s decoded: %s%s", label, decoded, truncated)
}

func describeMeshPayload(msg proto.Message) (any, bool) {
	packet := extractMeshPacket(msg)
	if packet == nil {
		return "", false
	}
	data := packet.GetDecoded()
	if data == nil {
		return "", false
	}
	return decodeDataPayload(data)
}

func extractMeshPacket(msg proto.Message) *meshtasticpb.MeshPacket {
	switch m := msg.(type) {
	case *meshtasticpb.FromRadio:
		return m.GetPacket()
	case *meshtasticpb.ToRadio:
		return m.GetPacket()
	default:
		return nil
	}
}

// packetSuppressesOriginEcho reports whether the radio's echo of an outbound
// MeshPacket should be withheld from the client that sent it. Chat-style apps
// already show the user's message locally; suppressing the echo avoids a
// duplicate in the sender's UI. Admin and other request/response ports must
// still receive the radio-processed echo (ACK, errors, updated payloads).
func packetSuppressesOriginEcho(pkt *meshtasticpb.MeshPacket) bool {
	if pkt == nil {
		return false
	}
	data := pkt.GetDecoded()
	if data == nil {
		// Encrypted or opaque payloads: deliver the echo so config/admin
		// sessions are not left waiting for a reply that never arrives.
		return false
	}
	switch data.GetPortnum() {
	case meshtasticpb.PortNum_TEXT_MESSAGE_APP,
		meshtasticpb.PortNum_TEXT_MESSAGE_COMPRESSED_APP,
		meshtasticpb.PortNum_REPLY_APP:
		return true
	default:
		return false
	}
}

func decodeDataPayload(data *meshtasticpb.Data) (any, bool) {
	if data == nil {
		return nil, false
	}

	port := data.GetPortnum()
	payload := data.GetPayload()

	switch port {
	case meshtasticpb.PortNum_TEXT_MESSAGE_APP,
		meshtasticpb.PortNum_DETECTION_SENSOR_APP,
		meshtasticpb.PortNum_ALERT_APP,
		meshtasticpb.PortNum_REPLY_APP,
		meshtasticpb.PortNum_RANGE_TEST_APP:
		return formatTextPayload(payload), true
	case meshtasticpb.PortNum_TEXT_MESSAGE_COMPRESSED_APP:
		return formatHexPayload(payload), true
	case meshtasticpb.PortNum_POSITION_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.Position{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_NODEINFO_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.User{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_ROUTING_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.Routing{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_ADMIN_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.AdminMessage{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_WAYPOINT_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.Waypoint{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_KEY_VERIFICATION_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.KeyVerificationAdmin{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_PAXCOUNTER_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.Paxcount{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_STORE_FORWARD_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.StoreAndForward{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_NODE_STATUS_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.StatusMessage{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_TELEMETRY_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.Telemetry{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_TRACEROUTE_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.RouteDiscovery{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_NEIGHBORINFO_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.NeighborInfo{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_MAP_REPORT_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.MapReport{}, payload); ok {
			return out, true
		}
	case meshtasticpb.PortNum_POWERSTRESS_APP:
		if out, ok := formatProtoPayload(&meshtasticpb.PowerStressMessage{}, payload); ok {
			return out, true
		}
	}

	if len(payload) == 0 {
		return "<empty>", true
	}
	return formatHexPayload(payload), true
}

func formatProtoPayload(msg proto.Message, payload []byte) (any, bool) {
	if len(payload) == 0 {
		return "<empty>", true
	}
	if err := proto.Unmarshal(payload, msg); err != nil {
		return nil, false
	}
	data, err := protojson.MarshalOptions{
		UseProtoNames: true,
		Multiline:     false,
	}.Marshal(msg)
	if err != nil {
		return nil, false
	}
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return string(data), true
	}
	return decoded, true
}

func formatTextPayload(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	if !utf8.Valid(payload) {
		return formatHexPayload(payload)
	}
	return string(payload)
}

func formatHexPayload(payload []byte) string {
	if len(payload) == 0 {
		return "0x"
	}
	return "0x" + hex.EncodeToString(payload)
}

func injectDecodedPayloadJSON(data []byte, decoded any) ([]byte, error) {
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, err
	}
	packet, ok := root["packet"].(map[string]any)
	if !ok {
		return data, nil
	}
	decodedMap, ok := packet["decoded"].(map[string]any)
	if !ok {
		return data, nil
	}
	decodedMap["payload_decoded"] = decoded
	updated, err := json.Marshal(root)
	if err != nil {
		return data, nil
	}
	return updated, nil
}

func (b *Broker) readSerial(errCh chan<- error) {
	reader := bufio.NewReader(b.serial)
	for {
		select {
		case <-b.done:
			return
		default:
		}

		payload, err := frame.ReadFrame(reader)
		if err != nil {
			if errors.Is(err, frame.ErrInvalidFrame) {
				// Transient serial glitches (bit flips, partially-received
				// frames after reconnect) should not tear down the broker.
				// Log once and keep scanning for the next magic prefix.
				log.Printf("Warning: %v; resyncing serial stream", err)
				continue
			}
			// Any read error on the serial link means the device is gone —
			// io.EOF for real TTY closures, net.ErrClosed for net.Pipe in
			// tests, and *os.PathError wrapping either for explicit Close().
			// Normalize to ErrSerialClosed so the server loop can distinguish
			// "serial died, reconnect" from "broker aborted by ctx".
			errCh <- ErrSerialClosed
			return
		}
		b.logDecodedPayload("serial -> broker", "", payload, payloadFromRadio)

		msg := &meshtasticpb.FromRadio{}
		if err := proto.Unmarshal(payload, msg); err != nil {
			// Unparseable protobuf is likely corruption; drop instead of
			// broadcasting garbage to clients that may reject or misrender it.
			log.Printf("Warning: failed to decode FromRadio (%d bytes): %v", len(payload), err)
			continue
		}

		b.cache.update(msg, payload)
		if label := cacheUpdateLabel(msg); label != "" {
			b.logConfig("cache store %s (%s)", label, b.cache.describe())
		}
		if b.routeFromRadio(msg, payload) {
			continue
		}
		if pkt := msg.GetPacket(); pkt != nil {
			if b.tryDeliverCanned(pkt) {
				continue
			}
			if origin := b.consumeOutboundOrigin(pkt); origin != nil {
				// Radio echo of a client-originated packet. Chat ports are
				// suppressed for the sender (synthetic echo already went to
				// peers); admin and other RPC-style ports must reach the sender
				// with the firmware's processed reply.
				if !packetSuppressesOriginEcho(pkt) {
					b.sendToClient(origin, payload)
				}
				continue
			}
		}
		b.broadcast(payload)
	}
}

func (b *Broker) routeFromRadio(frame *meshtasticpb.FromRadio, payload []byte) bool {
	switch v := frame.GetPayloadVariant().(type) {
	case *meshtasticpb.FromRadio_ConfigCompleteId:
		b.handleConfigComplete(v.ConfigCompleteId, payload)
		return true
	case *meshtasticpb.FromRadio_Rebooted:
		// Absorb rebooted=true: never forwarded to clients (some libraries
		// treat it as a teardown signal and drop TCP mid-handshake). See
		// handleRebooted for cache reset + pending re-issue semantics.
		b.handleRebooted()
		return true
	default:
		return false
	}
}

// handleRebooted is invoked when the radio sends FromRadio.rebooted=true.
// It clears the config cache (stale) and re-issues any in-flight client
// WantConfigId so the firmware starts sending the config dump instead of
// sitting idle after the rebooted notification.
func (b *Broker) handleRebooted() {
	log.Printf("Radio sent rebooted=true; clearing cache and re-issuing pending config requests")
	b.cache.reset()

	b.pendingMu.Lock()
	old := b.pendingConfig
	b.pendingConfig = make(map[uint32]configRequest)
	b.pendingMu.Unlock()

	for _, req := range old {
		if req.client != nil && req.client.isClosed() {
			continue
		}
		newID := b.reserveConfigRequest(req.client, req.originalID)
		addr := bootstrapClientAddr
		if req.client != nil {
			addr = req.client.addr
		}
		b.logConfig("rebooted re-issue WantConfigId client=%s original=0x%x wire=0x%x",
			addr, req.originalID, newID)
		toRadio := &meshtasticpb.ToRadio{
			PayloadVariant: &meshtasticpb.ToRadio_WantConfigId{WantConfigId: newID},
		}
		if err := b.forwardToSerial(toRadio); err != nil {
			b.fail(err)
			return
		}
	}
}

func (b *Broker) handleConfigComplete(id uint32, payload []byte) {
	req, ok := b.takeConfigRequest(id)
	if !ok {
		log.Printf("Dropping ConfigCompleteId without pending request: id=%d", id)
		return
	}
	if req.client == nil {
		b.logConfig("ConfigCompleteId from radio wire=0x%x (bootstrap)", id)
		b.PublishStatus()
		return
	}

	b.logConfig("ConfigCompleteId from radio wire=0x%x -> client=%s original=0x%x",
		id, req.client.addr, req.originalID)

	response := &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_ConfigCompleteId{
			ConfigCompleteId: req.originalID,
		},
	}
	data, err := proto.Marshal(response)
	if err != nil {
		return
	}
	b.sendToClient(req.client, data)
	b.logConfig("sent ConfigCompleteId to client=%s original=0x%x", req.client.addr, req.originalID)
}

func (b *Broker) AddClient(conn net.Conn) {
	cl := newClient(conn)

	b.clientsMu.Lock()
	b.clients[cl] = struct{}{}
	makePrimary := false
	if b.primary == nil {
		b.primary = cl
		makePrimary = true
	}
	b.clientsMu.Unlock()

	switch {
	case makePrimary:
		log.Printf("Client connected (primary): %s", cl.addr)
	case b.readOnlyClients:
		log.Printf("Client connected (read-only secondary): %s", cl.addr)
	default:
		log.Printf("Client connected (secondary): %s", cl.addr)
	}

	go b.writeLoop(cl)
	go b.readLoop(cl)
	b.PublishStatus()
}

func (b *Broker) PublishStatus() {
	if b.observability == nil {
		return
	}
	b.clientsMu.RLock()
	count := len(b.clients)
	primary := ""
	if b.primary != nil {
		primary = b.primary.addr
	}
	b.clientsMu.RUnlock()
	localNode, _ := b.LocalNodeNum()
	b.observability.SetStatus(webui.Status{
		SerialConnected: true,
		ClientCount:     count,
		PrimaryAddr:     primary,
		CacheReady:      b.cache.ready(),
		CacheSummary:    b.cache.describe(),
		LocalNodeNum:    localNode,
	})
}

// isPrimary reports whether the given client currently owns the radio session.
// The primary client is the first one to connect (promotion on disconnect is
// automatic); it is the only client whose WantConfigId is forwarded to the
// radio and whose Disconnect is propagated to the radio. Secondary clients
// receive cached config responses and — when readOnlyClients is false — may
// still send packets, which are also forwarded on their behalf.
func (b *Broker) isPrimary(cl *client) bool {
	b.clientsMu.RLock()
	defer b.clientsMu.RUnlock()
	return b.primary == cl
}

func (b *Broker) readLoop(client *client) {
	reader := bufio.NewReader(client.conn)
	for {
		payload, err := frame.ReadFrame(reader)
		if err != nil {
			b.removeClient(client)
			return
		}
		b.logDecodedPayload("client -> broker", client.addr, payload, payloadToRadio)
		b.handleClientPayload(client, payload)
	}
}

func (b *Broker) handleClientPayload(cl *client, payload []byte) {
	primary := b.isPrimary(cl)

	toRadio := &meshtasticpb.ToRadio{}
	if err := proto.Unmarshal(payload, toRadio); err != nil {
		// Unparseable payload: only the primary is allowed to pass raw bytes
		// through to the radio. Secondary clients are ignored to avoid
		// corrupting the serial stream.
		if primary {
			if serialErr := b.writeSerial(payload); serialErr != nil {
				b.fail(serialErr)
			}
		}
		return
	}

	switch v := toRadio.GetPayloadVariant().(type) {
	case *meshtasticpb.ToRadio_WantConfigId:
		originalID := v.WantConfigId
		// Cache-first regardless of primary/secondary. Re-handshaking on
		// every client's WantConfigId made the firmware reply with just
		// rebooted=true, which dropped the client and never populated the
		// cache. The wire is only hit on a cold start or after rebooted.
		if !b.cache.ready() {
			// If this client already has an in-flight config request, drop
			// it: reserving a new nonce without clearing the old one would
			// leave a ghost entry in pendingConfig that only gets cleaned
			// up when the client disconnects, leaking memory across client
			// retries.
			b.dropPendingForClient(cl)
			newID := b.reserveConfigRequest(cl, originalID)
			b.logConfig("WantConfigId client=%s primary=%v cache=miss original=0x%x wire=0x%x",
				cl.addr, primary, originalID, newID)
			v.WantConfigId = newID
			if err := b.forwardToSerial(toRadio); err != nil {
				b.fail(err)
			}
		} else {
			b.logConfig("WantConfigId client=%s primary=%v cache=hit original=0x%x cache=%s",
				cl.addr, primary, originalID, b.cache.describe())
			b.sendCachedConfig(cl, originalID)
		}
	case *meshtasticpb.ToRadio_Disconnect:
		// Per-client TCP disconnects are never forwarded to the radio:
		// firmware treats a phone disconnect as "reset phone state", which
		// then makes the next WantConfigId reply rebooted=true and starts
		// the loop this broker is designed to avoid.
		b.removeClient(cl)
		return
	case *meshtasticpb.ToRadio_Packet:
		if primary || !b.readOnlyClients {
			if err := b.forwardToSerial(toRadio); err != nil {
				b.fail(err)
				return
			}
			b.noteOutboundPacket(cl, v.Packet)
			// Echo the outgoing packet to the other clients as a FromRadio
			// so their UI stays in sync with what was transmitted. The
			// sender is excluded to avoid a loopback the app would render
			// as a duplicate message.
			fromRadio := &meshtasticpb.FromRadio{
				PayloadVariant: &meshtasticpb.FromRadio_Packet{
					Packet: v.Packet,
				},
			}
			broadcastPayload, err := proto.Marshal(fromRadio)
			if err != nil {
				log.Printf("Warning: failed to marshal packet for broadcast: %v", err)
				return
			}
			b.broadcastExcept(broadcastPayload, cl)
		} else {
			b.warnReadOnlyClient(cl)
		}
	default:
		if primary || !b.readOnlyClients {
			if err := b.forwardToSerial(toRadio); err != nil {
				b.fail(err)
			}
		} else {
			b.warnReadOnlyClient(cl)
		}
	}
}

func (b *Broker) writeLoop(cl *client) {
	writer := bufio.NewWriter(cl.conn)
	for {
		select {
		case <-b.done:
			b.removeClient(cl)
			return
		case <-cl.done:
			return
		case payload := <-cl.send:
			if err := frame.WriteFrame(writer, payload); err != nil {
				b.removeClient(cl)
				return
			}
			if err := writer.Flush(); err != nil {
				b.removeClient(cl)
				return
			}
		}
	}
}

func (b *Broker) broadcast(payload []byte) {
	b.broadcastExcept(payload, nil)
}

// broadcastExcept delivers payload to every client except the one given.
// A nil except sends to all clients.
func (b *Broker) broadcastExcept(payload []byte, except *client) {
	clients := b.snapshotClients()
	for _, cl := range clients {
		if cl == except {
			continue
		}
		b.sendToClient(cl, payload)
	}
}

// sendToClient enqueues payload for the client without blocking. A full
// buffer is treated as terminal for all clients: the client is disconnected
// so the app can reconnect and resync from cache. Silent frame-dropping is
// avoided because it leaves the client's state quietly out of sync with the
// radio.
func (b *Broker) warnReadOnlyClient(cl *client) {
	if cl.markReadOnlyWarning() {
		log.Printf("Ignoring traffic from read-only client: %s", cl.addr)
	}
}

func (b *Broker) disconnectSlowClient(cl *client) {
	if cl.isClosed() {
		return
	}
	if cl.markSlowWarning() {
		log.Printf("Client too slow, disconnecting: %s", cl.addr)
	}
	b.removeClient(cl)
}

func (b *Broker) sendToClient(cl *client, payload []byte) {
	if ok := cl.enqueue(payload); ok {
		b.logDecodedPayload("broker -> client", cl.addr, payload, payloadFromRadio)
		return
	}
	b.disconnectSlowClient(cl)
}

// sendToClientBlocking enqueues payload with a per-message timeout. It is
// used when replaying cached config so a momentarily busy client does not
// cause instant disconnect, but a persistently stuck client is still dropped.
func (b *Broker) sendToClientBlocking(cl *client, payload []byte) bool {
	if ok := cl.enqueueWithTimeout(payload, clientSendTimeout, b.done); ok {
		b.logDecodedPayload("broker -> client", cl.addr, payload, payloadFromRadio)
		return true
	}
	b.disconnectSlowClient(cl)
	return false
}

func (b *Broker) snapshotClients() []*client {
	b.clientsMu.RLock()
	defer b.clientsMu.RUnlock()
	clients := make([]*client, 0, len(b.clients))
	for client := range b.clients {
		clients = append(clients, client)
	}
	return clients
}

func (b *Broker) forwardToSerial(toRadio *meshtasticpb.ToRadio) error {
	payload, err := proto.Marshal(toRadio)
	if err != nil {
		return err
	}
	return b.writeSerial(payload)
}

func (b *Broker) writeSerial(payload []byte) error {
	select {
	case <-b.done:
		return ErrSerialClosed
	default:
	}

	b.serialMu.Lock()
	defer b.serialMu.Unlock()

	b.logDecodedPayload("broker -> serial", "", payload, payloadToRadio)
	if err := frame.WriteFrame(b.serial, payload); err != nil {
		return ErrSerialClosed
	}
	return nil
}

func (b *Broker) removeClient(cl *client) {
	var (
		removed    bool
		wasPrimary bool
		newPrimary *client
	)

	b.clientsMu.Lock()
	if _, ok := b.clients[cl]; ok {
		delete(b.clients, cl)
		removed = true
	}
	// Primary is tracked unconditionally; when it leaves, promote any
	// remaining client so the radio keeps a responsible owner. Without this
	// a subsequent WantConfigId from a "secondary" would never reach the
	// radio and the bridge would effectively freeze.
	if b.primary == cl {
		wasPrimary = true
		b.primary = nil
		for candidate := range b.clients {
			b.primary = candidate
			newPrimary = candidate
			break
		}
	}
	b.clientsMu.Unlock()

	if removed {
		log.Printf("Client disconnected: %s", cl.addr)
	}
	if wasPrimary && newPrimary != nil {
		log.Printf("Client promoted to primary: %s", newPrimary.addr)
	}

	b.dropPendingForClient(cl)
	cl.close()
	if removed {
		b.PublishStatus()
	}
}

func (b *Broker) CloseAll() {
	for _, client := range b.snapshotClients() {
		b.removeClient(client)
	}
}

func (b *Broker) reserveConfigRequest(client *client, original uint32) uint32 {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	for {
		var buf [4]byte
		var id uint32
		if _, err := rand.Read(buf[:]); err != nil {
			// crypto/rand is effectively infallible on supported platforms;
			// degrade to a time-based nonce rather than crashing. Still
			// respect uniqueness and the "nonzero" invariant below.
			id = uint32(time.Now().UnixNano())
		} else {
			id = binary.BigEndian.Uint32(buf[:])
		}
		if id == 0 {
			continue
		}
		if _, exists := b.pendingConfig[id]; exists {
			continue
		}
		b.pendingConfig[id] = configRequest{client: client, originalID: original}
		return id
	}
}

func (b *Broker) takeConfigRequest(id uint32) (configRequest, bool) {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	req, ok := b.pendingConfig[id]
	if !ok {
		return configRequest{}, false
	}
	delete(b.pendingConfig, id)
	return req, true
}

// bootstrapConfig requests a config dump from the radio when the broker owns
// the serial session but no TCP client has sent WantConfigId yet. Without this
// the firmware stays idle and the web UI never receives channels or node info.
func (b *Broker) bootstrapConfig() {
	if b.observability == nil || b.cache.ready() {
		return
	}

	b.pendingMu.Lock()
	for _, req := range b.pendingConfig {
		if req.client == nil {
			b.pendingMu.Unlock()
			return
		}
	}
	b.pendingMu.Unlock()

	newID := b.reserveConfigRequest(nil, brokerBootstrapConfigID)
	b.logConfig("bootstrap WantConfigId wire=0x%x", newID)
	toRadio := &meshtasticpb.ToRadio{
		PayloadVariant: &meshtasticpb.ToRadio_WantConfigId{WantConfigId: newID},
	}
	if err := b.forwardToSerial(toRadio); err != nil {
		b.fail(err)
	}
}

func (b *Broker) dropPendingForClient(cl *client) {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	for id, req := range b.pendingConfig {
		if req.client == cl {
			delete(b.pendingConfig, id)
		}
	}
}

func (b *Broker) sendCachedConfig(client *client, requestID uint32) {
	snap := b.cache.snapshot()
	if b.cache.empty() {
		log.Printf("Config cache empty; replying with config_complete_id only for %s", client.addr)
	} else {
		b.logConfig("sendCachedConfig client=%s requestID=0x%x snapshot=%s",
			client.addr, requestID, describeCacheSnapshot(snap))
	}

	if len(snap.myInfo) > 0 {
		if !b.sendToClientBlocking(client, snap.myInfo) {
			return
		}
	}
	for _, payload := range snap.configs {
		if !b.sendToClientBlocking(client, payload) {
			return
		}
	}
	for _, payload := range snap.moduleConfig {
		if !b.sendToClientBlocking(client, payload) {
			return
		}
	}
	for _, payload := range snap.channels {
		if !b.sendToClientBlocking(client, payload) {
			return
		}
	}
	for _, payload := range snap.nodeInfo {
		if !b.sendToClientBlocking(client, payload) {
			return
		}
	}
	if len(snap.metadata) > 0 {
		if !b.sendToClientBlocking(client, snap.metadata) {
			return
		}
	}
	if len(snap.deviceUI) > 0 {
		if !b.sendToClientBlocking(client, snap.deviceUI) {
			return
		}
	}

	response := &meshtasticpb.FromRadio{
		PayloadVariant: &meshtasticpb.FromRadio_ConfigCompleteId{
			ConfigCompleteId: requestID,
		},
	}
	data, err := proto.Marshal(response)
	if err != nil {
		return
	}
	b.sendToClientBlocking(client, data)
	b.logConfig("sendCachedConfig client=%s done ConfigCompleteId=0x%x", client.addr, requestID)
}
