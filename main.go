package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grandcat/zeroconf"
	"github.com/skrashevich/go-meshtastic-serial2tcp/termios"
	"golang.org/x/sys/unix"
)

const (
	minRuntime    = 3 * time.Second
	maxRapidFails = 5
	version       = "0.1"
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
	errClientClosed = errors.New("client disconnected")
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

	rapidFailCount := 0

	for {
		conn, err := acceptWithContext(ctx, listener)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		log.Printf("Client connected: %s", conn.RemoteAddr())
		start := time.Now()

		err = handleConnection(ctx, conn, cfg)

		runtime := time.Since(start)
		switch {
		case err == nil || errors.Is(err, context.Canceled):
			log.Printf("Connection ended")
		case errors.Is(err, errClientClosed):
			log.Printf("Client disconnected")
		case errors.Is(err, errSerialClosed):
			log.Printf("Serial disconnected")
		default:
			log.Printf("Connection ended with error: %v", err)
		}

		if err != nil && !errors.Is(err, errClientClosed) && runtime < minRuntime {
			rapidFailCount++
			log.Printf("Warning: bridge exited after %s", runtime.Truncate(time.Millisecond))
			if rapidFailCount >= maxRapidFails {
				return fmt.Errorf("too many rapid failures (%d). check device permissions, baud rate, and port availability", maxRapidFails)
			}
			log.Printf("Rapid failure %d of %d", rapidFailCount, maxRapidFails)
		} else if err == nil || errors.Is(err, errClientClosed) {
			rapidFailCount = 0
		} else if runtime >= minRuntime {
			rapidFailCount = 0
		}

		if cfg.reconnectDelay > 0 && !errors.Is(err, errClientClosed) {
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

func handleConnection(ctx context.Context, conn net.Conn, cfg config) error {
	defer conn.Close()

	if err := waitForDevice(ctx, cfg.device); err != nil {
		return err
	}

	if err := disableHUPCL(cfg.device); err != nil {
		log.Printf("Warning: could not disable HUPCL: %v", err)
		log.Printf("Device may reboot on disconnect")
	}

	serial, err := openSerial(cfg.device, cfg.baud)
	if err != nil {
		return err
	}
	defer serial.Close()

	log.Printf("  Connected to: %s @ %dbps", cfg.device, cfg.baud)

	return bridge(ctx, conn, serial)
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

func bridge(ctx context.Context, conn net.Conn, serial *os.File) error {
	var once sync.Once
	closeAll := func() {
		_ = conn.Close()
		_ = serial.Close()
	}

	errCh := make(chan error, 2)

	go func() {
		<-ctx.Done()
		once.Do(closeAll)
	}()

	copyFn := func(dst io.Writer, src io.Reader, eofErr error) {
		errCh <- copyLoop(dst, src, eofErr)
		once.Do(closeAll)
	}

	go copyFn(conn, serial, errSerialClosed)
	go copyFn(serial, conn, errClientClosed)

	err := <-errCh
	<-errCh

	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func copyLoop(dst io.Writer, src io.Reader, eofErr error) error {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if werr := writeAll(dst, buf[:n]); werr != nil {
				return werr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return eofErr
			}
			return err
		}
	}
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
