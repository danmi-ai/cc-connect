package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	remoteAgentDefaultPort    = 9900
	remoteAgentPingInterval   = 30 * time.Second
	remoteAgentPongTimeout    = 10 * time.Second
	remoteAgentWriteTimeout   = 10 * time.Second
	remoteAgentReadLimit      = 1 << 20 // 1MB
	remoteAgentHeartbeatStale = 90 * time.Second
)

// --- Protocol Messages ---

type RemoteMessageType string

const (
	RemoteMsgRegister  RemoteMessageType = "register"
	RemoteMsgTask      RemoteMessageType = "task"
	RemoteMsgOutput    RemoteMessageType = "output"
	RemoteMsgHeartbeat RemoteMessageType = "heartbeat"
	RemoteMsgError     RemoteMessageType = "error"
)

type RemoteMessage struct {
	Type      RemoteMessageType `json:"type"`
	ID        string            `json:"id,omitempty"`
	Payload   json.RawMessage   `json:"payload,omitempty"`
	Timestamp int64             `json:"ts,omitempty"`
}

type RegisterPayload struct {
	Name         string   `json:"name"`
	Capabilities []string `json:"capabilities,omitempty"`
	Model        string   `json:"model,omitempty"`
	Version      string   `json:"version,omitempty"`
}

type TaskPayload struct {
	TaskID    string           `json:"task_id"`
	SessionID string           `json:"session_id"`
	Prompt    string           `json:"prompt"`
	Images    []ImageAttachment `json:"images,omitempty"`
	Files     []FileAttachment  `json:"files,omitempty"`
}

type OutputPayload struct {
	TaskID    string    `json:"task_id"`
	SessionID string    `json:"session_id"`
	EventType EventType `json:"event_type"`
	Content   string    `json:"content,omitempty"`
	ToolName  string    `json:"tool_name,omitempty"`
	ToolInput string    `json:"tool_input,omitempty"`
	Done      bool      `json:"done,omitempty"`
	Error     string    `json:"error,omitempty"`
}

type HeartbeatPayload struct {
	ActiveTasks int    `json:"active_tasks"`
	Status      string `json:"status,omitempty"`
}

// --- Remote Agent Node (one per WS connection) ---

type RemoteAgentNode struct {
	Name         string
	Capabilities []string
	Model        string
	Version      string
	conn         *websocket.Conn
	lastSeen     time.Time
	mu           sync.Mutex
	sessions     map[string]*RemoteAgentSession
}

func (n *RemoteAgentNode) sendMessage(msg *RemoteMessage) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}
	n.conn.SetWriteDeadline(time.Now().Add(remoteAgentWriteTimeout))
	return n.conn.WriteJSON(msg)
}

func (n *RemoteAgentNode) isAlive() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return time.Since(n.lastSeen) < remoteAgentHeartbeatStale
}

func (n *RemoteAgentNode) touch() {
	n.mu.Lock()
	n.lastSeen = time.Now()
	n.mu.Unlock()
}

// --- Remote Agent Session ---

type RemoteAgentSession struct {
	id        string
	node      *RemoteAgentNode
	events    chan Event
	taskID    string
	closed    bool
	closeMu   sync.Mutex
}

func newRemoteAgentSession(id string, node *RemoteAgentNode) *RemoteAgentSession {
	return &RemoteAgentSession{
		id:     id,
		node:   node,
		events: make(chan Event, 64),
	}
}

func (s *RemoteAgentSession) Send(prompt string, images []ImageAttachment, files []FileAttachment) error {
	taskID := fmt.Sprintf("task_%d", time.Now().UnixNano())
	s.taskID = taskID

	payload, _ := json.Marshal(TaskPayload{
		TaskID:    taskID,
		SessionID: s.id,
		Prompt:    prompt,
		Images:    images,
		Files:     files,
	})

	return s.node.sendMessage(&RemoteMessage{
		Type:    RemoteMsgTask,
		ID:      taskID,
		Payload: payload,
	})
}

func (s *RemoteAgentSession) RespondPermission(requestID string, result PermissionResult) error {
	return nil
}

func (s *RemoteAgentSession) Events() <-chan Event {
	return s.events
}

func (s *RemoteAgentSession) CurrentSessionID() string {
	return s.id
}

func (s *RemoteAgentSession) Alive() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	return !s.closed && s.node.isAlive()
}

func (s *RemoteAgentSession) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.events)
	}
	return nil
}

func (s *RemoteAgentSession) emitEvent(e Event) {
	s.closeMu.Lock()
	closed := s.closed
	s.closeMu.Unlock()
	if closed {
		return
	}
	select {
	case s.events <- e:
	default:
		slog.Warn("remote_agent: event channel full, dropping event", "session", s.id)
	}
}

// --- Remote Agent Bridge (WS server + agent registry) ---

type RemoteAgentBridge struct {
	mu       sync.RWMutex
	nodes    map[string]*RemoteAgentNode // name → node
	server   *http.Server
	upgrader websocket.Upgrader
	port     int
}

func NewRemoteAgentBridge(port int) *RemoteAgentBridge {
	if port == 0 {
		port = remoteAgentDefaultPort
	}
	return &RemoteAgentBridge{
		nodes: make(map[string]*RemoteAgentNode),
		port:  port,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (b *RemoteAgentBridge) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", b.handleWS)
	mux.HandleFunc("/agents", b.handleListAgents)

	b.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", b.port),
		Handler: mux,
	}

	slog.Info("remote_agent: bridge starting", "port", b.port)
	go func() {
		if err := b.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("remote_agent: server error", "error", err)
		}
	}()
	return nil
}

func (b *RemoteAgentBridge) Stop() error {
	if b.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return b.server.Shutdown(ctx)
	}
	return nil
}

func (b *RemoteAgentBridge) handleListAgents(w http.ResponseWriter, r *http.Request) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	type agentInfo struct {
		Name         string   `json:"name"`
		Capabilities []string `json:"capabilities,omitempty"`
		Model        string   `json:"model,omitempty"`
		Alive        bool     `json:"alive"`
	}

	agents := make([]agentInfo, 0, len(b.nodes))
	for _, n := range b.nodes {
		agents = append(agents, agentInfo{
			Name:         n.Name,
			Capabilities: n.Capabilities,
			Model:        n.Model,
			Alive:        n.isAlive(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(agents)
}

func (b *RemoteAgentBridge) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := b.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("remote_agent: ws upgrade failed", "error", err)
		return
	}
	conn.SetReadLimit(remoteAgentReadLimit)

	slog.Info("remote_agent: new connection", "remote", conn.RemoteAddr())

	go b.serveConnection(conn)
}

func (b *RemoteAgentBridge) serveConnection(conn *websocket.Conn) {
	defer conn.Close()

	var node *RemoteAgentNode
	registered := false

	// Ping/pong keepalive
	conn.SetPongHandler(func(string) error {
		if node != nil {
			node.touch()
		}
		return nil
	})

	// Start ping ticker
	pingDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(remoteAgentPingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-pingDone:
				return
			case <-ticker.C:
				conn.SetWriteDeadline(time.Now().Add(remoteAgentWriteTimeout))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}()
	defer close(pingDone)

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				slog.Warn("remote_agent: connection error", "error", err)
			}
			break
		}

		var msg RemoteMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			slog.Warn("remote_agent: invalid message", "error", err)
			continue
		}

		switch msg.Type {
		case RemoteMsgRegister:
			if registered {
				slog.Warn("remote_agent: duplicate register", "node", node.Name)
				continue
			}
			var payload RegisterPayload
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				slog.Warn("remote_agent: invalid register payload", "error", err)
				continue
			}
			if payload.Name == "" {
				slog.Warn("remote_agent: register with empty name")
				continue
			}

			node = &RemoteAgentNode{
				Name:         payload.Name,
				Capabilities: payload.Capabilities,
				Model:        payload.Model,
				Version:      payload.Version,
				conn:         conn,
				lastSeen:     time.Now(),
				sessions:     make(map[string]*RemoteAgentSession),
			}
			b.registerNode(node)
			registered = true
			slog.Info("remote_agent: agent registered", "name", payload.Name, "model", payload.Model)

		case RemoteMsgOutput:
			if node == nil {
				continue
			}
			var payload OutputPayload
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				slog.Warn("remote_agent: invalid output payload", "error", err)
				continue
			}
			b.handleOutput(node, &payload)

		case RemoteMsgHeartbeat:
			if node != nil {
				node.touch()
			}

		default:
			slog.Debug("remote_agent: unknown message type", "type", msg.Type)
		}
	}

	// Cleanup on disconnect
	if node != nil {
		b.unregisterNode(node)
		slog.Info("remote_agent: agent disconnected", "name", node.Name)
	}
}

func (b *RemoteAgentBridge) registerNode(node *RemoteAgentNode) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// If existing node with same name, close old sessions
	if old, ok := b.nodes[node.Name]; ok {
		for _, s := range old.sessions {
			s.Close()
		}
	}
	b.nodes[node.Name] = node
}

func (b *RemoteAgentBridge) unregisterNode(node *RemoteAgentNode) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if current, ok := b.nodes[node.Name]; ok && current == node {
		delete(b.nodes, node.Name)
	}
	// Close all sessions
	node.mu.Lock()
	for _, s := range node.sessions {
		s.Close()
	}
	node.mu.Unlock()
}

func (b *RemoteAgentBridge) handleOutput(node *RemoteAgentNode, payload *OutputPayload) {
	node.mu.Lock()
	session, ok := node.sessions[payload.SessionID]
	node.mu.Unlock()

	if !ok {
		slog.Warn("remote_agent: output for unknown session", "session", payload.SessionID, "node", node.Name)
		return
	}

	event := Event{
		Type:     payload.EventType,
		Content:  payload.Content,
		ToolName: payload.ToolName,
		Done:     payload.Done,
	}
	if payload.Error != "" {
		event.Type = EventError
		event.Error = fmt.Errorf("%s", payload.Error)
	}
	if event.Type == "" {
		event.Type = EventText
	}

	session.emitEvent(event)
}

// GetNode returns a registered remote agent node by name.
func (b *RemoteAgentBridge) GetNode(name string) *RemoteAgentNode {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.nodes[name]
}

// ListNodes returns all registered remote agent nodes.
func (b *RemoteAgentBridge) ListNodes() []*RemoteAgentNode {
	b.mu.RLock()
	defer b.mu.RUnlock()
	nodes := make([]*RemoteAgentNode, 0, len(b.nodes))
	for _, n := range b.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// StartSession creates a new remote agent session on the specified node.
func (b *RemoteAgentBridge) StartSession(nodeName, sessionID string) (*RemoteAgentSession, error) {
	node := b.GetNode(nodeName)
	if node == nil {
		return nil, fmt.Errorf("remote_agent: node %q not connected", nodeName)
	}
	if !node.isAlive() {
		return nil, fmt.Errorf("remote_agent: node %q heartbeat stale", nodeName)
	}

	session := newRemoteAgentSession(sessionID, node)
	node.mu.Lock()
	node.sessions[sessionID] = session
	node.mu.Unlock()

	return session, nil
}

// --- RemoteAgent implements core.Agent interface ---

type RemoteAgent struct {
	bridge   *RemoteAgentBridge
	nodeName string
}

func NewRemoteAgent(bridge *RemoteAgentBridge, nodeName string) *RemoteAgent {
	return &RemoteAgent{
		bridge:   bridge,
		nodeName: nodeName,
	}
}

func (a *RemoteAgent) Name() string {
	return "remote:" + a.nodeName
}

func (a *RemoteAgent) StartSession(ctx context.Context, sessionID string) (AgentSession, error) {
	return a.bridge.StartSession(a.nodeName, sessionID)
}

func (a *RemoteAgent) ListSessions(ctx context.Context) ([]AgentSessionInfo, error) {
	node := a.bridge.GetNode(a.nodeName)
	if node == nil {
		return nil, nil
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	infos := make([]AgentSessionInfo, 0, len(node.sessions))
	for id, s := range node.sessions {
		if s.Alive() {
			infos = append(infos, AgentSessionInfo{
				ID:      id,
				Summary: "remote:" + a.nodeName,
			})
		}
	}
	return infos, nil
}

func (a *RemoteAgent) Stop() error {
	return nil
}
