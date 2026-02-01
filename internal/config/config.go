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
	ReconnectDelay  time.Duration
	ServiceName     string
	MDNSEnabled     bool
	ReadOnlyClients bool
	Debug           bool
}

func Load() (Config, bool) {
	defaultDevice := getenv("SERIAL_DEVICE", "/dev/ttyUSB0")
	baud := getenvInt("BAUD_RATE", 115200)
	tcpPort := getenvInt("TCP_PORT", 4403)
	reconnectDelaySeconds := getenvInt("RECONNECT_DELAY", 5)
	mdnsEnabled := getenvBool("MDNS_ENABLED", true)
	readOnlyClients := getenvBool("READ_ONLY_CLIENTS", false)
	debug := getenvBool("DEBUG", false)
	serviceName := getenv("SERVICE_NAME", "")

	healthcheck := flag.Bool("healthcheck", false, "exit 0 if the TCP port is reachable")
	deviceFlag := flag.String("device", defaultDevice, "serial device path (env SERIAL_DEVICE)")
	baudFlag := flag.Int("baud", baud, "serial baud rate (env BAUD_RATE)")
	tcpPortFlag := flag.Int("tcp-port", tcpPort, "TCP listen port (env TCP_PORT)")
	reconnectDelayFlag := flag.Int("reconnect-delay", reconnectDelaySeconds, "seconds to wait before reconnect (env RECONNECT_DELAY)")
	mdnsEnabledFlag := flag.Bool("mdns", mdnsEnabled, "enable mDNS discovery (env MDNS_ENABLED)")
	readOnlyClientsFlag := flag.Bool("read-only-clients", readOnlyClients, "make secondary clients read-only (env READ_ONLY_CLIENTS)")
	debugFlag := flag.Bool("debug", debug, "enable debug logging (env DEBUG)")
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
	readOnlyClients = *readOnlyClientsFlag
	debug = *debugFlag
	serviceName = strings.TrimSpace(*serviceNameFlag)
	if serviceName == "" {
		serviceName = fmt.Sprintf("Meshtastic Serial Bridge (%s)", sanitizeDeviceName(device))
	}

	return Config{
		Device:          device,
		Baud:            baud,
		TCPPort:         tcpPort,
		ReconnectDelay:  time.Duration(reconnectDelaySeconds) * time.Second,
		ServiceName:     serviceName,
		MDNSEnabled:     mdnsEnabled,
		ReadOnlyClients: readOnlyClients,
		Debug:           debug,
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
	log.Printf("  Reconnect Delay: %s", cfg.ReconnectDelay)
	if cfg.ReadOnlyClients {
		log.Printf("  Read-Only Clients: enabled")
	}
	if cfg.Debug {
		log.Printf("  Debug Logging: enabled")
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
