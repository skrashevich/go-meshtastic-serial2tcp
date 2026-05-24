package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/proto"
)

// DecryptPacket decrypts a channel MeshPacket using AES-CTR.
func DecryptPacket(packet *meshtasticpb.MeshPacket, key []byte) (*meshtasticpb.Data, error) {
	if packet == nil {
		return nil, fmt.Errorf("nil packet")
	}
	encrypted := packet.GetEncrypted()
	if len(encrypted) == 0 {
		if data := packet.GetDecoded(); data != nil {
			return data, nil
		}
		return nil, fmt.Errorf("packet has no encrypted payload")
	}
	if len(key) != 16 && len(key) != 32 {
		return nil, fmt.Errorf("invalid key length %d", len(key))
	}

	nonce := make([]byte, 16)
	binary.LittleEndian.PutUint64(nonce[0:8], uint64(packet.GetId()))
	binary.LittleEndian.PutUint32(nonce[8:12], packet.GetFrom())
	// block counter at bytes 12-15 stays zero

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	stream := cipher.NewCTR(block, nonce)
	plain := make([]byte, len(encrypted))
	stream.XORKeyStream(plain, encrypted)

	data := &meshtasticpb.Data{}
	if err := proto.Unmarshal(plain, data); err != nil {
		return nil, fmt.Errorf("decrypted payload is not Data: %w", err)
	}
	return data, nil
}
