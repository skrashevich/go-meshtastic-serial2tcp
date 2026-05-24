package httpapi

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"
)

func Run(ctx context.Context, addr string, srv *Server) error {
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("HTTP API listening on: http://%s", addr)
	log.Printf("  Meshtastic API: /api/v1/fromradio, /api/v1/toradio")
	log.Printf("  JSON messages:  /api/v1/messages?psk=<base64>&channel=LongFast")

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server: %w", err)
	}
	return nil
}
