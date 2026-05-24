package config

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Device          string
	Baud            int
	TCPPort         int
	HTTPPort        int
	HTTPEnabled     bool
	ChannelPSK      string
	ChannelName     string
	ReconnectDelay  time.Duration
	ServiceName     string
	MDNSEnabled     bool
	ReadOnlyClients bool
	Debug           bool
	Daemon          bool
}

func Load() (Config, bool) {
	defaultDevice := getenv("SERIAL_DEVICE", "/dev/ttyUSB0")
	baud := getenvInt("BAUD_RATE", 115200)
	tcpPort := getenvInt("TCP_PORT", 4403)
	reconnectDelaySeconds := getenvInt("RECONNECT_DELAY", 5)
	mdnsEnabled := getenvBool("MDNS_ENABLED", true)
	readOnlyClients := getenvBool("READ_ONLY_CLIENTS", false)
	debug := getenvBool("DEBUG", false)
	daemon := getenvBool("DAEMON", false)
	httpEnabled := getenvBool("HTTP_ENABLED", true)
	httpPort := getenvInt("HTTP_PORT", 8080)
	channelPSK := getenv("CHANNEL_PSK", "")
	channelName := getenv("CHANNEL_NAME", "LongFast")
	serviceName := getenv("SERVICE_NAME", "")

	healthcheck := flag.Bool("healthcheck", false, "exit 0 if the TCP port is reachable")
	deviceFlag := flag.String("device", defaultDevice, "serial device path (env SERIAL_DEVICE)")
	baudFlag := flag.Int("baud", baud, "serial baud rate (env BAUD_RATE)")
	tcpPortFlag := flag.Int("tcp-port", tcpPort, "TCP listen port (env TCP_PORT)")
	reconnectDelayFlag := flag.Int("reconnect-delay", reconnectDelaySeconds, "seconds to wait before reconnect (env RECONNECT_DELAY)")
	mdnsEnabledFlag := flag.Bool("mdns", mdnsEnabled, "enable mDNS discovery (env MDNS_ENABLED)")
	readOnlyClientsFlag := flag.Bool("read-only-clients", readOnlyClients, "make secondary clients read-only (env READ_ONLY_CLIENTS)")
	debugFlag := flag.Bool("debug", debug, "enable debug logging: protobuf JSON + [config] handshake trace (env DEBUG)")
	debugShortFlag := flag.Bool("D", false, "shorthand for -debug")
	httpEnabledFlag := flag.Bool("http", httpEnabled, "enable Meshtastic HTTP API for OpenClaw (env HTTP_ENABLED)")
	httpPortFlag := flag.Int("http-port", httpPort, "HTTP listen port (env HTTP_PORT)")
	channelPSKFlag := flag.String("channel-psk", channelPSK, "default channel PSK (base64) for /api/v1/messages (env CHANNEL_PSK)")
	channelNameFlag := flag.String("channel-name", channelName, "channel name for PSK hash (env CHANNEL_NAME)")
	serviceNameFlag := flag.String("service-name", serviceName, "mDNS service name (env SERVICE_NAME)")
	daemonFlag := flag.Bool("d", daemon, "run in background as a daemon (env DAEMON)")

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
	readOnlyClients = *readOnlyClientsFlag
	debug = *debugFlag || *debugShortFlag
	daemon = *daemonFlag
	httpEnabled = *httpEnabledFlag
	httpPort = normalizePositiveInt("http-port", *httpPortFlag, httpPort)
	channelPSK = strings.TrimSpace(*channelPSKFlag)
	channelName = strings.TrimSpace(*channelNameFlag)
	if channelName == "" {
		channelName = "LongFast"
	}
	serviceName = strings.TrimSpace(*serviceNameFlag)
	if serviceName == "" {
		serviceName = fmt.Sprintf("Meshtastic Serial Bridge (%s)", sanitizeDeviceName(device))
	}

	return Config{
		Device:          device,
		Baud:            baud,
		TCPPort:         tcpPort,
		HTTPPort:        httpPort,
		HTTPEnabled:     httpEnabled,
		ChannelPSK:      channelPSK,
		ChannelName:     channelName,
		ReconnectDelay:  time.Duration(reconnectDelaySeconds) * time.Second,
		ServiceName:     serviceName,
		MDNSEnabled:     mdnsEnabled,
		ReadOnlyClients: readOnlyClients,
		Debug:           debug,
		Daemon:          daemon,
	}, *healthcheck
}

func PrintBanner(cfg Config, version string) {
	ver := strings.TrimSpace(version)
	if ver == "" {
		ver = "unknown"
	}

	log.Printf("Meshtastic Serial Bridge v%s", ver)
	log.Printf("  Device: %s", cfg.Device)
	log.Printf("  Baud: %d", cfg.Baud)
	log.Printf("  TCP Port: %d", cfg.TCPPort)
	if cfg.HTTPEnabled {
		log.Printf("  HTTP Port: %d", cfg.HTTPPort)
	}
	log.Printf("  Reconnect Delay: %s", cfg.ReconnectDelay)
	if cfg.ReadOnlyClients {
		log.Printf("  Read-Only Clients: enabled")
	}
	if cfg.Debug {
		log.Printf("  Debug Logging: enabled (protobuf decode + [config] WantConfigId/ConfigCompleteId)")
	}
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
