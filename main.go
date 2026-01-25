package main

import (
	"bufio"
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grandcat/zeroconf"
	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic/meshtastic"
	"github.com/skrashevich/go-meshtastic-serial2tcp/termios"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const (
	minRuntime        = 3 * time.Second
	maxRapidFails     = 5
	clientSendBuffer  = 64
	clientSendTimeout = 5 * time.Second
	maxFrameSize      = 64*1024 - 1
	version           = "0.1"
)

const (
	frameMagic0 = 0x94
	frameMagic1 = 0xC3
)

type config struct {
	device         string
	baud           int
	tcpPort        int
	reconnectDelay time.Duration
	serviceName    string
	mdnsEnabled    bool
}

var (
	errSerialClosed = errors.New("serial disconnected")
)

func main() {
	log.SetFlags(0)

	cfg, healthcheck := loadConfig()
	if healthcheck {
		exitCode := runHealthcheck(cfg)
		os.Exit(exitCode)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	printBanner(cfg)

	var mdnsServer *zeroconf.Server
	if cfg.mdnsEnabled {
		server, err := startMDNS(cfg)
		if err != nil {
			log.Printf("Warning: mDNS disabled: %v", err)
		} else {
			mdnsServer = server
			log.Printf("✓ mDNS service registered: %s", cfg.serviceName)
			log.Printf("  Service type: _meshtastic._tcp.local.")
			log.Printf("  Port: %d", cfg.tcpPort)
		}
	} else {
		log.Printf("mDNS discovery disabled")
	}

	if err := runServer(ctx, cfg); err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}

	if mdnsServer != nil {
		mdnsServer.Shutdown()
	}
}

func loadConfig() (config, bool) {
	defaultDevice := getenv("SERIAL_DEVICE", "/dev/ttyUSB0")
	baud := getenvInt("BAUD_RATE", 115200)
	tcpPort := getenvInt("TCP_PORT", 4403)
	reconnectDelaySeconds := getenvInt("RECONNECT_DELAY", 5)
	mdnsEnabled := getenvBool("MDNS_ENABLED", true)
	serviceName := getenv("SERVICE_NAME", "")

	healthcheck := flag.Bool("healthcheck", false, "exit 0 if the TCP port is reachable")
	deviceFlag := flag.String("device", defaultDevice, "serial device path (env SERIAL_DEVICE)")
	baudFlag := flag.Int("baud", baud, "serial baud rate (env BAUD_RATE)")
	tcpPortFlag := flag.Int("tcp-port", tcpPort, "TCP listen port (env TCP_PORT)")
	reconnectDelayFlag := flag.Int("reconnect-delay", reconnectDelaySeconds, "seconds to wait before reconnect (env RECONNECT_DELAY)")
	mdnsEnabledFlag := flag.Bool("mdns", mdnsEnabled, "enable mDNS discovery (env MDNS_ENABLED)")
	serviceNameFlag := flag.String("service-name", serviceName, "mDNS service name (env SERVICE_NAME)")

	flag.Parse()

	device := strings.TrimSpace(*deviceFlag)
	if device == "" {
		log.Printf("Warning: invalid device '%s', using default %s", *deviceFlag, defaultDevice)
		device = defaultDevice
	}
	baud = normalizePositiveInt("baud", *baudFlag, baud)
	tcpPort = normalizePositiveInt("tcp-port", *tcpPortFlag, tcpPort)
	reconnectDelaySeconds = normalizePositiveInt("reconnect-delay", *reconnectDelayFlag, reconnectDelaySeconds)
	mdnsEnabled = *mdnsEnabledFlag
	serviceName = strings.TrimSpace(*serviceNameFlag)
	if serviceName == "" {
		serviceName = fmt.Sprintf("Meshtastic Serial Bridge (%s)", sanitizeDeviceName(device))
	}

	return config{
		device:         device,
		baud:           baud,
		tcpPort:        tcpPort,
		reconnectDelay: time.Duration(reconnectDelaySeconds) * time.Second,
		serviceName:    serviceName,
		mdnsEnabled:    mdnsEnabled,
	}, *healthcheck
}

func printBanner(cfg config) {
	ver := strings.TrimSpace(version)
	if ver == "" {
		ver = "unknown"
	}

	log.Printf("Meshtastic Serial Bridge v%s", ver)
	log.Printf("  Device: %s", cfg.device)
	log.Printf("  Baud: %d", cfg.baud)
	log.Printf("  TCP Port: %d", cfg.tcpPort)
	log.Printf("  Reconnect Delay: %s", cfg.reconnectDelay)
}

func runServer(ctx context.Context, cfg config) error {
	addr := fmt.Sprintf("0.0.0.0:%d", cfg.tcpPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Printf("Listening on: %s", addr)
	log.Printf("Ready for TCP connections")

	var brokerMu sync.RWMutex
	var broker *protocolBroker

	acceptErrCh := make(chan error, 1)
	go func() {
		for {
			conn, err := acceptWithContext(ctx, listener)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				acceptErrCh <- err
				return
			}

			brokerMu.RLock()
			current := broker
			brokerMu.RUnlock()
			if current == nil {
				log.Printf("Client rejected (serial not ready): %s", conn.RemoteAddr())
				_ = conn.Close()
				continue
			}

			current.addClient(conn)
		}
	}()

	rapidFailCount := 0

	for {
		if err := waitForDevice(ctx, cfg.device); err != nil {
			return err
		}

		if err := disableHUPCL(cfg.device); err != nil {
			log.Printf("Warning: could not disable HUPCL: %v", err)
			log.Printf("Device may reboot on disconnect")
		}

		serial, err := openSerial(cfg.device, cfg.baud)
		if err != nil {
			rapidFailCount++
			if rapidFailCount >= maxRapidFails {
				return fmt.Errorf("too many rapid failures (%d). check device permissions, baud rate, and port availability", maxRapidFails)
			}
			log.Printf("Warning: failed to open serial device: %v", err)
			log.Printf("Rapid failure %d of %d", rapidFailCount, maxRapidFails)
			if cfg.reconnectDelay > 0 {
				log.Printf("Bridge disconnected, waiting %s before retry...", cfg.reconnectDelay)
				if err := sleepWithContext(ctx, cfg.reconnectDelay); err != nil {
					return nil
				}
			}
			continue
		}

		start := time.Now()
		log.Printf("  Connected to: %s @ %dbps", cfg.device, cfg.baud)

		b := newProtocolBroker(serial)
		brokerMu.Lock()
		broker = b
		brokerMu.Unlock()

		serialErrCh := make(chan error, 1)
		go func() {
			serialErrCh <- b.run(ctx)
		}()

		var brokerErr error
		select {
		case <-ctx.Done():
			brokerErr = ctx.Err()
		case err := <-acceptErrCh:
			brokerErr = err
		case err := <-serialErrCh:
			brokerErr = err
		}

		brokerMu.Lock()
		broker = nil
		brokerMu.Unlock()
		b.closeAll()
		_ = serial.Close()

		runtime := time.Since(start)
		switch {
		case brokerErr == nil || errors.Is(brokerErr, context.Canceled):
			return nil
		case errors.Is(brokerErr, errSerialClosed):
			log.Printf("Serial disconnected")
		default:
			log.Printf("Broker ended with error: %v", brokerErr)
		}

		if brokerErr != nil && runtime < minRuntime {
			rapidFailCount++
			log.Printf("Warning: bridge exited after %s", runtime.Truncate(time.Millisecond))
			if rapidFailCount >= maxRapidFails {
				return fmt.Errorf("too many rapid failures (%d). check device permissions, baud rate, and port availability", maxRapidFails)
			}
			log.Printf("Rapid failure %d of %d", rapidFailCount, maxRapidFails)
		} else if brokerErr == nil || runtime >= minRuntime {
			rapidFailCount = 0
		}

		if cfg.reconnectDelay > 0 {
			log.Printf("Bridge disconnected, waiting %s before retry...", cfg.reconnectDelay)
			if err := sleepWithContext(ctx, cfg.reconnectDelay); err != nil {
				return nil
			}
		}
	}
}

func acceptWithContext(ctx context.Context, listener net.Listener) (net.Conn, error) {
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		return listener.Accept()
	}

	for {
		_ = tcpListener.SetDeadline(time.Now().Add(1 * time.Second))
		conn, err := tcpListener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				continue
			}
			return nil, err
		}
		return conn, nil
	}
}

func waitForDevice(ctx context.Context, device string) error {
	if deviceExists(device) {
		return nil
	}

	log.Printf("Waiting for device %s...", device)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if deviceExists(device) {
				log.Printf("Device %s found", device)
				return nil
			}
		}
	}
}

func deviceExists(device string) bool {
	_, err := os.Stat(device)
	return err == nil
}

func disableHUPCL(device string) error {
	fd, err := unix.Open(device, unix.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)

	termiosState, err := termios.GetTermios(fd)
	if err != nil {
		return err
	}

	termiosState.Cflag &^= unix.HUPCL
	if err := termios.SetTermios(fd, termiosState); err != nil {
		return err
	}

	log.Printf("HUPCL disabled")
	return nil
}

func openSerial(device string, baud int) (*os.File, error) {
	fd, err := unix.Open(device, unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}

	if err := unix.SetNonblock(fd, false); err != nil {
		unix.Close(fd)
		return nil, err
	}

	termiosState, err := termios.GetTermios(fd)
	if err != nil {
		unix.Close(fd)
		return nil, err
	}

	termios.SetRawMode(termiosState)
	termiosState.Cflag |= unix.CLOCAL | unix.CREAD
	termiosState.Cflag &^= unix.HUPCL

	if err := termios.SetBaudRate(termiosState, baud); err != nil {
		unix.Close(fd)
		return nil, err
	}

	if err := termios.SetTermios(fd, termiosState); err != nil {
		unix.Close(fd)
		return nil, err
	}

	file := os.NewFile(uintptr(fd), device)
	if file == nil {
		unix.Close(fd)
		return nil, errors.New("failed to open serial device")
	}

	return file, nil
}

func readFrame(r *bufio.Reader) ([]byte, error) {
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != frameMagic0 {
			continue
		}

		b, err = r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != frameMagic1 {
			continue
		}

		var lenBuf [2]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return nil, err
		}

		length := binary.BigEndian.Uint16(lenBuf[:])
		if length == 0 || length > maxFrameSize {
			lengthLE := binary.LittleEndian.Uint16(lenBuf[:])
			if lengthLE == 0 || lengthLE > maxFrameSize {
				return nil, fmt.Errorf("invalid frame length: %d", length)
			}
			length = lengthLE
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		return payload, nil
	}
}

func writeFrame(w io.Writer, payload []byte) error {
	if len(payload) > maxFrameSize {
		return fmt.Errorf("frame too large: %d", len(payload))
	}

	var header [4]byte
	header[0] = frameMagic0
	header[1] = frameMagic1
	binary.BigEndian.PutUint16(header[2:], uint16(len(payload)))

	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, payload)
}

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
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		c.mu.Unlock()
		return false
	case c.send <- payload:
		c.mu.Unlock()
		return true
	case <-timer.C:
		c.mu.Unlock()
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

type protocolBroker struct {
	serial    *os.File
	serialMu  sync.Mutex
	clients   map[*client]struct{}
	clientsMu sync.RWMutex
	primary   *client
	cache     *configCache

	pendingMu     sync.Mutex
	pendingConfig map[uint32]configRequest

	done    chan struct{}
	errOnce sync.Once
	err     error
}

func newProtocolBroker(serial *os.File) *protocolBroker {
	return &protocolBroker{
		serial:        serial,
		clients:       make(map[*client]struct{}),
		cache:         newConfigCache(),
		pendingConfig: make(map[uint32]configRequest),
		done:          make(chan struct{}),
	}
}

func (b *protocolBroker) run(ctx context.Context) error {
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

func (b *protocolBroker) fail(err error) {
	b.errOnce.Do(func() {
		if err == nil {
			err = errSerialClosed
		}
		b.err = err
		close(b.done)
	})
}

func (b *protocolBroker) readSerial(errCh chan<- error) {
	reader := bufio.NewReader(b.serial)
	for {
		select {
		case <-b.done:
			return
		default:
		}

		payload, err := readFrame(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				errCh <- errSerialClosed
				return
			}
			errCh <- err
			return
		}

		frame := &meshtasticpb.FromRadio{}
		if err := proto.Unmarshal(payload, frame); err != nil {
			b.broadcast(payload)
			continue
		}

		b.cache.update(frame, payload)
		if b.routeFromRadio(frame, payload) {
			continue
		}
		b.broadcast(payload)
	}
}

func (b *protocolBroker) routeFromRadio(frame *meshtasticpb.FromRadio, payload []byte) bool {
	switch v := frame.GetPayloadVariant().(type) {
	case *meshtasticpb.FromRadio_ConfigCompleteId:
		b.handleConfigComplete(v.ConfigCompleteId, payload)
		return true
	default:
		return false
	}
}

func (b *protocolBroker) handleConfigComplete(id uint32, payload []byte) {
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

func (b *protocolBroker) addClient(conn net.Conn) {
	client := &client{
		conn: conn,
		send: make(chan []byte, clientSendBuffer),
		addr: conn.RemoteAddr().String(),
	}

	var makePrimary bool
	b.clientsMu.Lock()
	b.clients[client] = struct{}{}
	if b.primary == nil {
		b.primary = client
		makePrimary = true
	}
	b.clientsMu.Unlock()

	if makePrimary {
		log.Printf("Client connected (primary): %s", client.addr)
	} else {
		log.Printf("Client connected (read-only): %s", client.addr)
	}

	go b.writeLoop(client)
	go b.readLoop(client)
}

func (b *protocolBroker) isPrimary(client *client) bool {
	b.clientsMu.RLock()
	defer b.clientsMu.RUnlock()
	return b.primary == client
}

func (b *protocolBroker) readLoop(client *client) {
	reader := bufio.NewReader(client.conn)
	for {
		payload, err := readFrame(reader)
		if err != nil {
			b.removeClient(client)
			return
		}
		b.handleClientPayload(client, payload)
	}
}

func (b *protocolBroker) handleClientPayload(client *client, payload []byte) {
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

func (b *protocolBroker) writeLoop(client *client) {
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
			if err := writeFrame(writer, payload); err != nil {
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

func (b *protocolBroker) broadcast(payload []byte) {
	clients := b.snapshotClients()
	for _, client := range clients {
		b.sendToClient(client, payload)
	}
}

func (b *protocolBroker) sendToClient(client *client, payload []byte) {
	if ok := client.enqueue(payload); ok {
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

func (b *protocolBroker) sendToClientBlocking(client *client, payload []byte) bool {
	if ok := client.enqueueWithTimeout(payload, clientSendTimeout, b.done); ok {
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

func (b *protocolBroker) snapshotClients() []*client {
	b.clientsMu.RLock()
	defer b.clientsMu.RUnlock()
	clients := make([]*client, 0, len(b.clients))
	for client := range b.clients {
		clients = append(clients, client)
	}
	return clients
}

func (b *protocolBroker) forwardToSerial(toRadio *meshtasticpb.ToRadio) error {
	payload, err := proto.Marshal(toRadio)
	if err != nil {
		return err
	}
	return b.writeSerial(payload)
}

func (b *protocolBroker) writeSerial(payload []byte) error {
	select {
	case <-b.done:
		return errSerialClosed
	default:
	}

	b.serialMu.Lock()
	defer b.serialMu.Unlock()

	if err := writeFrame(b.serial, payload); err != nil {
		return errSerialClosed
	}
	return nil
}

func (b *protocolBroker) removeClient(cl *client) {
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
}

func (b *protocolBroker) closeAll() {
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

func (b *protocolBroker) reserveConfigRequest(client *client, original uint32) uint32 {
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

func (b *protocolBroker) takeConfigRequest(id uint32) configRequest {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	req := b.pendingConfig[id]
	delete(b.pendingConfig, id)
	return req
}

func (b *protocolBroker) dropPendingForClient(cl *client) {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	for id, req := range b.pendingConfig {
		if req.client == cl {
			delete(b.pendingConfig, id)
		}
	}
}

func (b *protocolBroker) sendCachedConfig(client *client, requestID uint32) {
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

func writeAll(dst io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := dst.Write(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		buf = buf[n:]
	}
	return nil
}

func startMDNS(cfg config) (*zeroconf.Server, error) {
	txt := []string{
		"bridge=serial",
		fmt.Sprintf("port=%d", cfg.tcpPort),
		fmt.Sprintf("serial_device=%s", cfg.device),
		fmt.Sprintf("baud_rate=%d", cfg.baud),
	}

	return zeroconf.Register(cfg.serviceName, "_meshtastic._tcp", "local.", cfg.tcpPort, txt, nil)
}

func runHealthcheck(cfg config) int {
	addr := fmt.Sprintf("127.0.0.1:%d", cfg.tcpPort)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return 1
	}
	_ = conn.Close()
	return 0
}

func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getenvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		log.Printf("Warning: invalid %s '%s', using default %d", key, value, fallback)
		return fallback
	}

	return parsed
}

func getenvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		log.Printf("Warning: invalid %s '%s', using default %v", key, value, fallback)
		return fallback
	}
}

func normalizePositiveInt(name string, value int, fallback int) int {
	if value <= 0 {
		log.Printf("Warning: invalid %s '%d', using default %d", name, value, fallback)
		return fallback
	}
	return value
}

func sanitizeDeviceName(device string) string {
	replacer := strings.NewReplacer("/", "_", ".", "_")
	return replacer.Replace(device)
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
