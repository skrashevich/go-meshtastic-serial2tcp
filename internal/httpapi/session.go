package httpapi

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/broker"
	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/frame"
)

var errNoData = errors.New("no data")

const (
	sessionCookieName = "meshtastic_session"
	sessionTTL        = 30 * time.Minute
	readTimeout       = 2 * time.Second
)

type Session struct {
	id   string
	conn net.Conn
}

type SessionManager struct {
	mu       sync.Mutex
	sessions map[string]*sessionEntry
}

type sessionEntry struct {
	session  *Session
	lastUsed time.Time
}

func NewSessionManager() *SessionManager {
	return &SessionManager{sessions: make(map[string]*sessionEntry)}
}

func (m *SessionManager) GetOrCreate(b *broker.Broker, sessionID string) (*Session, string, error) {
	if b == nil {
		return nil, "", errBrokerUnavailable
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupLocked(time.Now())

	if sessionID != "" {
		if entry, ok := m.sessions[sessionID]; ok {
			entry.lastUsed = time.Now()
			return entry.session, sessionID, nil
		}
	}

	session, id, err := newSession(b)
	if err != nil {
		return nil, "", err
	}
	m.sessions[id] = &sessionEntry{session: session, lastUsed: time.Now()}
	return session, id, nil
}

func (m *SessionManager) cleanupLocked(now time.Time) {
	for id, entry := range m.sessions {
		if now.Sub(entry.lastUsed) > sessionTTL {
			entry.session.Close()
			delete(m.sessions, id)
		}
	}
}

func newSession(b *broker.Broker) (*Session, string, error) {
	clientConn, brokerConn := net.Pipe()
	b.AddClient(brokerConn)

	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		_ = clientConn.Close()
		_ = brokerConn.Close()
		return nil, "", err
	}
	id := hex.EncodeToString(idBytes)
	return &Session{id: id, conn: clientConn}, id, nil
}

func (s *Session) Close() {
	_ = s.conn.Close()
}

func (s *Session) Send(payload []byte) error {
	return frame.WriteFrame(s.conn, payload)
}

func (s *Session) Receive() ([]byte, error) {
	if err := s.conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(s.conn)
	payload, err := frame.ReadFrame(reader)
	_ = s.conn.SetReadDeadline(time.Time{})
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil, errNoData
		}
		if errors.Is(err, io.EOF) {
			return nil, errNoData
		}
		return nil, err
	}
	return payload, nil
}

func (s *Session) ReceiveAll(max int) ([][]byte, error) {
	if max <= 0 {
		max = 256
	}
	out := make([][]byte, 0, 8)
	for len(out) < max {
		payload, err := s.Receive()
		if errors.Is(err, errNoData) {
			break
		}
		if err != nil {
			return out, err
		}
		out = append(out, payload)
	}
	return out, nil
}
