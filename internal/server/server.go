package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/broker"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/config"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/serial"
)

const (
	minRuntime    = 3 * time.Second
	maxRapidFails = 5
)

func Run(ctx context.Context, cfg config.Config) error {
	addr := fmt.Sprintf("0.0.0.0:%d", cfg.TCPPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Printf("Listening on: %s", addr)
	log.Printf("Ready for TCP connections")

	var brokerMu sync.RWMutex
	var current *broker.Broker

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
			active := current
			brokerMu.RUnlock()
			if active == nil {
				log.Printf("Client rejected (serial not ready): %s", conn.RemoteAddr())
				_ = conn.Close()
				continue
			}

			active.AddClient(conn)
		}
	}()

	rapidFailCount := 0

	for {
		if err := waitForDevice(ctx, cfg.Device); err != nil {
			return err
		}

		if err := serial.DisableHUPCL(cfg.Device); err != nil {
			log.Printf("Warning: could not disable HUPCL: %v", err)
			log.Printf("Device may reboot on disconnect")
		}

		serialConn, err := serial.Open(cfg.Device, cfg.Baud)
		if err != nil {
			rapidFailCount++
			if rapidFailCount >= maxRapidFails {
				return fmt.Errorf("too many rapid failures (%d). check device permissions, baud rate, and port availability", maxRapidFails)
			}
			log.Printf("Warning: failed to open serial device: %v", err)
			log.Printf("Rapid failure %d of %d", rapidFailCount, maxRapidFails)
			if cfg.ReconnectDelay > 0 {
				log.Printf("Bridge disconnected, waiting %s before retry...", cfg.ReconnectDelay)
				if err := sleepWithContext(ctx, cfg.ReconnectDelay); err != nil {
					return nil
				}
			}
			continue
		}

		start := time.Now()
		log.Printf("  Connected to: %s @ %dbps", cfg.Device, cfg.Baud)

		b := broker.New(serialConn, cfg.ReadOnlyClients, cfg.Debug)
		brokerMu.Lock()
		current = b
		brokerMu.Unlock()

		serialErrCh := make(chan error, 1)
		go func() {
			serialErrCh <- b.Run(ctx)
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
		current = nil
		brokerMu.Unlock()
		b.CloseAll()
		_ = serialConn.Close()

		runtime := time.Since(start)
		switch {
		case brokerErr == nil || errors.Is(brokerErr, context.Canceled):
			return nil
		case errors.Is(brokerErr, broker.ErrSerialClosed):
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

		if cfg.ReconnectDelay > 0 {
			log.Printf("Bridge disconnected, waiting %s before retry...", cfg.ReconnectDelay)
			if err := sleepWithContext(ctx, cfg.ReconnectDelay); err != nil {
				return nil
			}
		}
	}
}

func RunHealthcheck(cfg config.Config) int {
	addr := fmt.Sprintf("127.0.0.1:%d", cfg.TCPPort)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return 1
	}
	_ = conn.Close()
	return 0
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
	if serial.DeviceExists(device) {
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
			if serial.DeviceExists(device) {
				log.Printf("Device %s found", device)
				return nil
			}
		}
	}
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
