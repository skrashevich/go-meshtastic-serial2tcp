package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/skrashevich/go-meshtastic-serial2tcp/internal/broker"
	mcrypto "github.com/skrashevich/go-meshtastic-serial2tcp/internal/crypto"
	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/proto"
)

var errBrokerUnavailable = errors.New("serial bridge not ready")

const protobufSchema = "https://raw.githubusercontent.com/meshtastic/protobufs/master/meshtastic/mesh.proto"

type Server struct {
	brokerFn    func() *broker.Broker
	sessions    *SessionManager
	store       *Store
	defaultPSK  []byte
	channelName string
}

func NewServer(brokerFn func() *broker.Broker, channelPSK, channelName string) *Server {
	var defaultPSK []byte
	if channelPSK != "" {
		if psk, err := mcrypto.ParsePSK(channelPSK); err == nil {
			defaultPSK = psk
		}
	}
	if channelName == "" {
		channelName = "LongFast"
	}
	return &Server{
		brokerFn:    brokerFn,
		sessions:    NewSessionManager(),
		store:       NewStore(defaultStoreCapacity),
		defaultPSK:  defaultPSK,
		channelName: channelName,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/fromradio", s.handleFromRadio)
	mux.HandleFunc("/api/v1/toradio", s.handleToRadio)
	mux.HandleFunc("/api/v1/messages", s.handleMessages)
	mux.HandleFunc("/health", s.handleHealth)
	return mux
}

func (s *Server) ObserveFromRadio(frame *meshtasticpb.FromRadio) {
	packet := frame.GetPacket()
	if packet == nil {
		return
	}
	data := packet.GetDecoded()
	if data == nil && len(s.defaultPSK) > 0 {
		if decrypted, err := mcrypto.DecryptPacket(packet, s.defaultPSK); err == nil {
			data = decrypted
		}
	}
	if msg, ok := messageFromPacket(packet, data); ok {
		s.store.Add(msg)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.brokerFn() == nil {
		http.Error(w, "serial bridge not ready", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleFromRadio(w http.ResponseWriter, r *http.Request) {
	setCORS(w, "GET, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	session, sessionID, err := s.sessionForRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	setSessionCookie(w, sessionID)

	setProtobufHeaders(w)
	all := strings.EqualFold(r.URL.Query().Get("all"), "true")

	if all {
		payloads, err := session.ReceiveAll(512)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		for _, payload := range payloads {
			s.observePayload(payload)
			if _, err := w.Write(payload); err != nil {
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	payload, err := session.Receive()
	if errors.Is(err, errNoData) {
		w.WriteHeader(http.StatusOK)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	s.observePayload(payload)
	if _, err := w.Write(payload); err != nil {
		return
	}
}

func (s *Server) handleToRadio(w http.ResponseWriter, r *http.Request) {
	setCORS(w, "PUT, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(body) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	session, sessionID, err := s.sessionForRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	setSessionCookie(w, sessionID)

	if err := session.Send(body); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	setCORS(w, "GET, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pskParam := strings.TrimSpace(r.URL.Query().Get("psk"))
	channelName := strings.TrimSpace(r.URL.Query().Get("channel"))
	if channelName == "" {
		channelName = s.channelName
	}

	var channelHash *byte
	if pskParam != "" {
		psk, err := mcrypto.ParsePSK(pskParam)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		hash, err := mcrypto.ChannelHash(channelName, psk)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		channelHash = &hash
	} else if len(s.defaultPSK) > 0 {
		hash, err := mcrypto.ChannelHash(channelName, s.defaultPSK)
		if err == nil {
			channelHash = &hash
		}
	}

	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	messages := s.store.List(channelHash, since, limit)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"messages": messages,
		"count":    len(messages),
	})
}

func (s *Server) sessionForRequest(r *http.Request) (*Session, string, error) {
	sessionID := ""
	if cookie, err := r.Cookie(sessionCookieName); err == nil {
		sessionID = cookie.Value
	}
	return s.sessions.GetOrCreate(s.brokerFn(), sessionID)
}

func (s *Server) observePayload(payload []byte) {
	frame := &meshtasticpb.FromRadio{}
	if err := proto.Unmarshal(payload, frame); err != nil {
		return
	}
	s.ObserveFromRadio(frame)
}

func setProtobufHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("X-Protobuf-Schema", protobufSchema)
}

func setCORS(w http.ResponseWriter, methods string) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", methods)
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func setSessionCookie(w http.ResponseWriter, sessionID string) {
	if sessionID == "" {
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(sessionTTL.Seconds()),
	})
}

func (s *Server) BrokerObserver() broker.FromRadioObserver {
	return func(frame *meshtasticpb.FromRadio, _ []byte) {
		s.ObserveFromRadio(frame)
	}
}
