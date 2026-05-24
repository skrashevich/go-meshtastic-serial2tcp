package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/grandcat/zeroconf"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/broker"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/config"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/daemon"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/httpapi"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/mdns"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/server"
)

const version = "0.1"

func main() {
	log.SetFlags(0)

	cfg, healthcheck := config.Load()
	if healthcheck {
		exitCode := server.RunHealthcheck(cfg)
		os.Exit(exitCode)
	}

	if cfg.Daemon && !daemon.IsChild() {
		if err := daemon.Daemonize(); err != nil {
			log.Printf("ERROR: %v", err)
			os.Exit(1)
		}
		os.Exit(0)
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

	var brokerMu sync.RWMutex
	var current *broker.Broker
	brokerFn := func() *broker.Broker {
		brokerMu.RLock()
		defer brokerMu.RUnlock()
		return current
	}

	var httpSrv *httpapi.Server
	if cfg.HTTPEnabled {
		httpSrv = httpapi.NewServer(brokerFn, cfg.ChannelPSK, cfg.ChannelName)
		addr := fmt.Sprintf("0.0.0.0:%d", cfg.HTTPPort)
		go func() {
			if err := httpapi.Run(ctx, addr, httpSrv); err != nil {
				log.Printf("ERROR: HTTP API: %v", err)
				stop()
			}
		}()
	}

	onBroker := func(b *broker.Broker) {
		brokerMu.Lock()
		current = b
		brokerMu.Unlock()
		if httpSrv != nil {
			b.SetFromRadioObserver(httpSrv.BrokerObserver())
		}
	}

	if err := server.Run(ctx, cfg, onBroker); err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}

	if mdnsServer != nil {
		mdnsServer.Shutdown()
	}
}
