package broker

import (
	"context"
	"testing"
	"time"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/webui"
)

func TestBrokerBootstrapConfigWithoutTCPClient(t *testing.T) {
	radio := newMeshtasticNodeSim()
	defer radio.close()
	radio.run(t)

	hub := webui.NewHub()
	b := New(radio.brokerEnd, false, false, hub)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- b.Run(ctx) }()
	defer func() {
		cancel()
		radio.close()
		<-runDone
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.cache.ready() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !b.cache.ready() {
		t.Fatal("bootstrap did not populate config cache without a TCP client")
	}

	_, wantCount, _ := radio.stats()
	if wantCount != 1 {
		t.Fatalf("radio should have seen exactly 1 bootstrap WantConfigId, saw %d", wantCount)
	}

	num, ok := b.LocalNodeNum()
	if !ok || num != radio.myNodeNum {
		t.Fatalf("LocalNodeNum = %x ready=%v, want 0x%x", num, ok, radio.myNodeNum)
	}
}

func TestBrokerBootstrapConfigRebootedRetry(t *testing.T) {
	radio := newMeshtasticNodeSim()
	radio.rebootedBeforeConfig = true
	defer radio.close()
	radio.run(t)

	hub := webui.NewHub()
	b := New(radio.brokerEnd, false, false, hub)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- b.Run(ctx) }()
	defer func() {
		cancel()
		radio.close()
		<-runDone
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.cache.ready() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !b.cache.ready() {
		t.Fatal("bootstrap did not recover from rebooted=true and populate cache")
	}

	_, wantCount, _ := radio.stats()
	if wantCount != 2 {
		t.Fatalf("expected bootstrap + rebooted re-issue (2 WantConfigId), saw %d", wantCount)
	}
}
