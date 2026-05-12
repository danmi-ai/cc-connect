package infoflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// wsClient manages the Infoflow WebSocket long-connection.
type wsClient struct {
	p      *Platform
	mu     sync.Mutex
	seqID  int
	conn   *websocket.Conn
	cancel context.CancelFunc
}

func newWSClient(p *Platform) *wsClient {
	return &wsClient{p: p}
}

func (w *wsClient) nextSeqID() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seqID++
	return w.seqID
}

// runLoop keeps the WebSocket alive, reconnecting on disconnect.
func (w *wsClient) runLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := w.connectAndServe(ctx); err != nil {
			slog.Warn("infoflow: WS connection lost, retrying in 5s", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// connectAndServe establishes one WS session and processes messages until disconnection.
func (w *wsClient) connectAndServe(ctx context.Context) error {
	// Phase 1: get WS URL
	wsURL, connID, err := w.getWSEndpoint(ctx)
	if err != nil {
		return fmt.Errorf("get endpoint: %w", err)
	}

	// Phase 2: connect using wsConnectDomain (replace hostname from Phase 1 URL)
	connectURL := wsURL
	if w.p.wsConnectDomain != "" {
		// Replace host in URL: ws://original-host/ws?... -> ws://wsConnectDomain/ws?...
		if idx := strings.Index(connectURL, "://"); idx >= 0 {
			rest := connectURL[idx+3:]
			if slashIdx := strings.Index(rest, "/"); slashIdx >= 0 {
				connectURL = connectURL[:idx+3] + w.p.wsConnectDomain + rest[slashIdx:]
			}
		}
	}

	slog.Info("infoflow: connecting WS", "url", connectURL, "connection_id", connID)

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, connectURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	w.mu.Lock()
	w.conn = conn
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		conn.Close()
		w.conn = nil
		w.mu.Unlock()
	}()

	slog.Info("infoflow: WS connected", "connection_id", connID)

	// Start heartbeat
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go w.heartbeatLoop(hbCtx)

	// Read loop
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		frame, err := DecodeFrame(msgBytes)
		if err != nil {
			slog.Error("infoflow: frame decode error", "error", err)
			continue
		}

		w.handleFrame(frame)
	}
}

// getWSEndpoint calls Phase 1 to obtain the WS URL.
func (w *wsClient) getWSEndpoint(ctx context.Context) (wsURL, connID string, err error) {
	body, _ := json.Marshal(map[string]string{
		"app_key":    w.p.appKey,
		"app_secret": w.p.appSecret,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://"+w.p.wsGateway+"/open/ws/endpoint", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.p.httpClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	var result struct {
		Code int `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			URL          string `json:"url"`
			ClientConfig struct {
				PingInterval int `json:"ping_interval"`
			} `json:"client_config"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", err
	}
	if result.Code != 0 {
		return "", "", fmt.Errorf("endpoint API code=%d msg=%s", result.Code, result.Msg)
	}

	wsURL = result.Data.URL

	// Extract connection_id
	if idx := strings.Index(wsURL, "connection_id="); idx >= 0 {
		connID = wsURL[idx+len("connection_id="):]
		if amp := strings.Index(connID, "&"); amp >= 0 {
			connID = connID[:amp]
		}
	}
	return wsURL, connID, nil
}

// handleFrame processes a decoded frame.
func (w *wsClient) handleFrame(frame *Frame) {
	switch frame.Method {
	case FrameMethodControl:
		// Heartbeat response (pong) — just log
		slog.Debug("infoflow: CONTROL frame", "seqId", frame.SeqID)

	case FrameMethodData:
		// Actual message event — ACK then dispatch
		w.sendEventACK(frame)
		payload, err := ParseFramePayload(frame)
		if err != nil || payload == nil {
			return
		}
		if isRecallEvent(payload) {
			w.p.onRecallEvent(payload)
		} else {
			w.p.onIncomingMessage(payload)
		}

	case FrameMethodResponse:
		// Response to our requests — currently unused
		slog.Debug("infoflow: RESPONSE frame", "seqId", frame.SeqID)
	}
}

// sendEventACK acknowledges a DATA frame.
func (w *wsClient) sendEventACK(originalFrame *Frame) {
	ackFrame := CreateFrame(
		w.nextSeqID(),
		FrameMethodData,
		[]Header{{Key: "type", Value: "event_ack"}},
		map[string]any{
			"ackSeqId": originalFrame.SeqID,
			"code":     0,
			"message":  "ok",
		},
	)
	w.sendFrame(ackFrame)
}

// heartbeatLoop sends periodic ping frames.
func (w *wsClient) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.sendHeartbeat()
		}
	}
}

func (w *wsClient) sendHeartbeat() {
	frame := CreateFrame(
		w.nextSeqID(),
		FrameMethodControl,
		[]Header{{Key: "type", Value: "ping"}},
		map[string]any{},
	)
	w.sendFrame(frame)
}

func (w *wsClient) sendFrame(f *Frame) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn == nil {
		return
	}
	data := EncodeFrame(f)
	if err := w.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		slog.Warn("infoflow: send frame failed", "error", err)
	}
}
