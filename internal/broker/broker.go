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
	"os"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/frame"
	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	clientSendBuffer  = 64
	clientSendTimeout = 5 * time.Second
	debugMaxBytes     = 256
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
	conn           net.Conn
	send           chan []byte
	addr           string
	warnedReadOnly bool
	warnedSlow     bool
	closed         bool
	mu             sync.Mutex
}

func (c *client) enqueue(payload []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	select {
	case c.send <- payload:
		return true
	default:
		return false
	}
}

func (c *client) markReadOnlyWarning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.warnedReadOnly {
		return false
	}
	c.warnedReadOnly = true
	return true
}

func (c *client) markSlowWarning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.warnedSlow {
		return false
	}
	c.warnedSlow = true
	return true
}

func (c *client) enqueueWithTimeout(payload []byte, timeout time.Duration, done <-chan struct{}) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	sendChan := c.send
	c.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return false
	case sendChan <- payload:
		return true
	case <-timer.C:
		return false
	}
}

func (c *client) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	close(c.send)
	c.mu.Unlock()
	_ = c.conn.Close()
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

type configRequest struct {
	client     *client
	originalID uint32
}

type Broker struct {
	serial          *os.File
	serialMu        sync.Mutex
	clients         map[*client]struct{}
	clientsMu       sync.RWMutex
	primary         *client
	cache           *configCache
	readOnlyClients bool
	debug           bool

	pendingMu     sync.Mutex
	pendingConfig map[uint32]configRequest

	done    chan struct{}
	errOnce sync.Once
	err     error
}

func New(serial *os.File, readOnlyClients bool, debug bool) *Broker {
	return &Broker{
		serial:          serial,
		clients:         make(map[*client]struct{}),
		cache:           newConfigCache(),
		readOnlyClients: readOnlyClients,
		debug:           debug,
		pendingConfig:   make(map[uint32]configRequest),
		done:            make(chan struct{}),
	}
}

func (b *Broker) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go b.readSerial(errCh)

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

func (b *Broker) logPayload(direction, addr string, payload []byte, kind payloadKind) {
	if !b.debug {
		return
	}
	label := direction
	if addr != "" {
		label = fmt.Sprintf("%s [%s]", direction, addr)
	}
	limit := payload
	truncated := ""
	if len(payload) > debugMaxBytes {
		limit = payload[:debugMaxBytes]
		truncated = fmt.Sprintf(" ...(truncated %d bytes)", len(payload)-debugMaxBytes)
	}
	hexPayload := hex.EncodeToString(limit)
	if len(payload) == 0 {
		log.Printf("%s (0 bytes)", label)
		return
	}
	log.Printf("%s (%d bytes): %s%s", label, len(payload), hexPayload, truncated)
	//b.logDecodedPayload(direction, addr, payload, kind)
}

func (b *Broker) logDecodedPayload(direction, addr string, payload []byte, kind payloadKind) {
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
			if errors.Is(err, io.EOF) {
				errCh <- ErrSerialClosed
				return
			}
			errCh <- err
			return
		}
		b.logDecodedPayload("serial -> broker", "", payload, payloadFromRadio)

		msg := &meshtasticpb.FromRadio{}
		if err := proto.Unmarshal(payload, msg); err != nil {
			b.broadcast(payload)
			continue
		}

		b.cache.update(msg, payload)
		if b.routeFromRadio(msg, payload) {
			continue
		}
		b.broadcast(payload)
	}
}

func (b *Broker) routeFromRadio(frame *meshtasticpb.FromRadio, payload []byte) bool {
	switch v := frame.GetPayloadVariant().(type) {
	case *meshtasticpb.FromRadio_ConfigCompleteId:
		b.handleConfigComplete(v.ConfigCompleteId, payload)
		return true
	default:
		return false
	}
}

func (b *Broker) handleConfigComplete(id uint32, payload []byte) {
	req := b.takeConfigRequest(id)
	if req.client == nil {
		b.broadcast(payload)
		return
	}

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
}

func (b *Broker) AddClient(conn net.Conn) {
	client := &client{
		conn: conn,
		send: make(chan []byte, clientSendBuffer),
		addr: conn.RemoteAddr().String(),
	}

	b.clientsMu.Lock()
	b.clients[client] = struct{}{}
	makePrimary := false
	readOnly := false
	if b.readOnlyClients {
		if b.primary == nil {
			b.primary = client
			makePrimary = true
		} else {
			readOnly = true
		}
	}
	b.clientsMu.Unlock()

	switch {
	case makePrimary:
		log.Printf("Client connected (primary): %s", client.addr)
	case readOnly:
		log.Printf("Client connected (read-only): %s", client.addr)
	default:
		log.Printf("Client connected (read-write): %s", client.addr)
	}

	go b.writeLoop(client)
	go b.readLoop(client)
}

func (b *Broker) isPrimary(client *client) bool {
	if !b.readOnlyClients {
		return true
	}
	b.clientsMu.RLock()
	defer b.clientsMu.RUnlock()
	return b.primary == client
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

func (b *Broker) handleClientPayload(client *client, payload []byte) {
	toRadio := &meshtasticpb.ToRadio{}
	if err := proto.Unmarshal(payload, toRadio); err != nil {
		if b.isPrimary(client) {
			if err := b.writeSerial(payload); err != nil {
				b.fail(err)
			}
		}
		return
	}

	switch v := toRadio.GetPayloadVariant().(type) {
	case *meshtasticpb.ToRadio_WantConfigId:
		if b.isPrimary(client) {
			newID := b.reserveConfigRequest(client, v.WantConfigId)
			v.WantConfigId = newID
			if err := b.forwardToSerial(toRadio); err != nil {
				b.fail(err)
			}
		} else {
			b.sendCachedConfig(client, v.WantConfigId)
		}
	case *meshtasticpb.ToRadio_Disconnect:
		if b.isPrimary(client) {
			if err := b.forwardToSerial(toRadio); err != nil {
				b.fail(err)
			}
		}
		b.removeClient(client)
		return
	default:
		if b.isPrimary(client) {
			if err := b.forwardToSerial(toRadio); err != nil {
				b.fail(err)
			}
		} else if client.markReadOnlyWarning() {
			log.Printf("Ignoring request from read-only client: %s", client.addr)
		}
	}
}

func (b *Broker) writeLoop(client *client) {
	writer := bufio.NewWriter(client.conn)
	for {
		select {
		case <-b.done:
			b.removeClient(client)
			return
		case payload, ok := <-client.send:
			if !ok {
				return
			}
			if err := frame.WriteFrame(writer, payload); err != nil {
				b.removeClient(client)
				return
			}
			if err := writer.Flush(); err != nil {
				b.removeClient(client)
				return
			}
		}
	}
}

func (b *Broker) broadcast(payload []byte) {
	clients := b.snapshotClients()
	for _, client := range clients {
		b.sendToClient(client, payload)
	}
}

func (b *Broker) sendToClient(client *client, payload []byte) {
	if ok := client.enqueue(payload); ok {
		b.logDecodedPayload("broker -> client", client.addr, payload, payloadFromRadio)
		return
	}
	if !b.isPrimary(client) {
		if client.markSlowWarning() {
			log.Printf("Client send buffer full; dropping frames: %s", client.addr)
		}
		return
	}
	log.Printf("Client too slow, disconnecting: %s", client.addr)
	b.removeClient(client)
}

func (b *Broker) sendToClientBlocking(client *client, payload []byte) bool {
	if ok := client.enqueueWithTimeout(payload, clientSendTimeout, b.done); ok {
		b.logDecodedPayload("broker -> client", client.addr, payload, payloadFromRadio)
		return true
	}
	if !b.isPrimary(client) {
		if client.markSlowWarning() {
			log.Printf("Client send buffer full; dropping frames: %s", client.addr)
		}
		return false
	}
	log.Printf("Client too slow, disconnecting: %s", client.addr)
	b.removeClient(client)
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
	if b.readOnlyClients {
		if b.primary == cl {
			wasPrimary = true
			b.primary = nil
			for candidate := range b.clients {
				b.primary = candidate
				newPrimary = candidate
				break
			}
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
}

func (b *Broker) CloseAll() {
	b.clientsMu.RLock()
	clients := make([]*client, 0, len(b.clients))
	for client := range b.clients {
		clients = append(clients, client)
	}
	b.clientsMu.RUnlock()

	for _, client := range clients {
		b.removeClient(client)
	}
}

func (b *Broker) reserveConfigRequest(client *client, original uint32) uint32 {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	for {
		var buf [4]byte
		if _, err := rand.Read(buf[:]); err != nil {
			id := uint32(time.Now().UnixNano())
			b.pendingConfig[id] = configRequest{client: client, originalID: original}
			return id
		}
		id := binary.BigEndian.Uint32(buf[:])
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

func (b *Broker) takeConfigRequest(id uint32) configRequest {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	req := b.pendingConfig[id]
	delete(b.pendingConfig, id)
	return req
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
		log.Printf("Config cache empty; replying with config_complete_id only")
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
}
