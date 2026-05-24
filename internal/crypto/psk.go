package crypto

import (
	"encoding/base64"
	"fmt"
	"strings"
)

var defaultPSK = []byte{
	0xd4, 0xf1, 0xbb, 0x3a, 0x20, 0x29, 0x07, 0x59,
	0xf0, 0xbc, 0xff, 0xab, 0xcf, 0x4e, 0x69, 0x01,
}

// ParsePSK decodes a Meshtastic channel key from base64 (URL-safe or standard).
func ParsePSK(key string) ([]byte, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, fmt.Errorf("empty psk")
	}
	if key == "AQ==" {
		key = "1PG7OiApB1nwvP+rz05pAQ=="
	}

	normalized := strings.NewReplacer("-", "+", "_", "/").Replace(key)
	raw, err := base64.StdEncoding.DecodeString(normalized)
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(normalized)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 psk: %w", err)
		}
	}
	return ExpandPSK(raw)
}

// ExpandPSK applies Meshtastic short-key expansion rules.
func ExpandPSK(raw []byte) ([]byte, error) {
	switch len(raw) {
	case 0:
		return nil, nil
	case 1:
		switch raw[0] {
		case 0:
			return nil, nil
		default:
			out := append([]byte(nil), defaultPSK...)
			out[len(out)-1] += raw[0] - 1
			return out, nil
		}
	case 16, 32:
		return append([]byte(nil), raw...), nil
	default:
		if len(raw) < 16 {
			out := make([]byte, 16)
			copy(out, raw)
			return out, nil
		}
		out := make([]byte, 32)
		copy(out, raw)
		return out, nil
	}
}

func xorHash(data []byte) byte {
	var code byte
	for _, b := range data {
		code ^= b
	}
	return code
}

// ChannelHash returns the Meshtastic channel hash for a name + PSK pair.
func ChannelHash(name string, psk []byte) (byte, error) {
	if psk == nil {
		return 0, fmt.Errorf("psk required")
	}
	h := xorHash([]byte(name))
	h ^= xorHash(psk)
	return h, nil
}
