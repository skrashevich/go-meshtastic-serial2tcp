package mdns

import (
	"fmt"

	"github.com/grandcat/zeroconf"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/config"
)

func Start(cfg config.Config) (*zeroconf.Server, error) {
	txt := []string{
		"bridge=serial",
		fmt.Sprintf("port=%d", cfg.TCPPort),
		fmt.Sprintf("serial_device=%s", cfg.Device),
		fmt.Sprintf("baud_rate=%d", cfg.Baud),
	}

	return zeroconf.Register(cfg.ServiceName, "_meshtastic._tcp", "local.", cfg.TCPPort, txt, nil)
}
