package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/grandcat/zeroconf"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/config"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/mdns"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/server"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/webui"
)

const version = "0.1"

func main() {
	log.SetFlags(0)

	cfg, healthcheck := config.Load()
	if healthcheck {
		exitCode := server.RunHealthcheck(cfg)
		os.Exit(exitCode)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	config.PrintBanner(cfg, version)

	var mdnsServer *zeroconf.Server
	if cfg.MDNSEnabled {
		server, err := mdns.Start(cfg)
		if err != nil {
			log.Printf("Warning: mDNS disabled: %v", err)
		} else {
			mdnsServer = server
			log.Printf("✓ mDNS service registered: %s", cfg.ServiceName)
			log.Printf("  Service type: _meshtastic._tcp.local.")
			log.Printf("  Port: %d", cfg.TCPPort)
		}
	} else {
		log.Printf("mDNS discovery disabled")
	}

	var hub *webui.Hub
	if cfg.WebUI {
		hub = webui.NewHub()
		ui := webui.NewServer(hub, cfg.WebUIAddr)
		go func() {
			if err := ui.Start(ctx); err != nil {
				log.Printf("Web UI stopped: %v", err)
			}
		}()
	}

	if err := server.Run(ctx, cfg, hub); err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}

	if mdnsServer != nil {
		mdnsServer.Shutdown()
	}
}
