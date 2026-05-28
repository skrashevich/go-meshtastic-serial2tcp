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
	WebUI           bool
	WebUIAddr       string
}

func Load() (Config, bool) {
	defaultDevice := getenv("SERIAL_DEVICE", "/dev/ttyUSB0")
	baud := getenvInt("BAUD_RATE", 115200)
	tcpPort := getenvInt("TCP_PORT", 4403)
	reconnectDelaySeconds := getenvInt("RECONNECT_DELAY", 5)
	mdnsEnabled := getenvBool("MDNS_ENABLED", true)
	readOnlyClients := getenvBool("READ_ONLY_CLIENTS", false)
	debug := getenvBool("DEBUG", false)
	webUI := getenvBool("WEB_UI", false)
	_, webUIAddrFromEnv := os.LookupEnv("WEB_UI_ADDR")
	webUIAddr := getenv("WEB_UI_ADDR", "127.0.0.1:9080")
	webUIAddrFromCLI := flagPassed("web-ui-addr")
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
	webUIFlag := flag.Bool("web-ui", webUI, "enable local web UI for channel traffic and debug (env WEB_UI)")
	webUIAddrFlag := flag.String("web-ui-addr", webUIAddr, "web UI listen address; also enables the UI when set on the CLI or via WEB_UI_ADDR (env WEB_UI_ADDR)")
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
	debug = *debugFlag || *debugShortFlag
	webUI = *webUIFlag || webUIAddrFromEnv || webUIAddrFromCLI
	webUIAddr = strings.TrimSpace(*webUIAddrFlag)
	if webUIAddr == "" {
		webUIAddr = "127.0.0.1:9080"
	}
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
		WebUI:           webUI,
		WebUIAddr:       webUIAddr,
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
		log.Printf("  Debug Logging: enabled (protobuf decode + [config] WantConfigId/ConfigCompleteId)")
	}
	if cfg.WebUI {
		log.Printf("  Web UI: http://%s", cfg.WebUIAddr)
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

// flagPassed reports whether name was set on the command line (e.g. -web-ui-addr or -web-ui-addr=host:port).
func flagPassed(name string) bool {
	prefix := "-" + name
	for i, arg := range os.Args[1:] {
		if arg == prefix || strings.HasPrefix(arg, prefix+"=") {
			return true
		}
		if arg == prefix && i+1 < len(os.Args)-1 {
			return true
		}
	}
	return false
}
