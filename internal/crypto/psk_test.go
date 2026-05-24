package crypto

import (
	"testing"
)

func TestParsePSKDefault(t *testing.T) {
	psk, err := ParsePSK("AQ==")
	if err != nil {
		t.Fatalf("ParsePSK: %v", err)
	}
	if len(psk) != 16 {
		t.Fatalf("expected 16 bytes, got %d", len(psk))
	}
}

func TestExpandPSKShortKey(t *testing.T) {
	psk, err := ExpandPSK([]byte{1})
	if err != nil {
		t.Fatalf("ExpandPSK: %v", err)
	}
	if len(psk) != 16 {
		t.Fatalf("expected 16 bytes, got %d", len(psk))
	}
}

func TestChannelHash(t *testing.T) {
	psk, err := ParsePSK("AQ==")
	if err != nil {
		t.Fatalf("ParsePSK: %v", err)
	}
	hash, err := ChannelHash("LongFast", psk)
	if err != nil {
		t.Fatalf("ChannelHash: %v", err)
	}
	if hash == 0 {
		t.Fatalf("expected non-zero hash")
	}
}
