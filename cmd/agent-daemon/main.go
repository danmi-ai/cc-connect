package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultBridgeURL = "ws://127.0.0.1:9900/ws"
	reconnectDelay   = 5 * time.Second
	heartbeatInterval = 30 * time.Second
	readTimeout       = 60 * time.Second
)

// Protocol message types (mirrors core/remote_agent.go)
type MessageType string

const (
	MsgRegister  MessageType = "register"
	MsgTask      MessageType = "task"
	MsgOutput    MessageType = "output"
	MsgHeartbeat MessageType = "heartbeat"
)

type Message struct {
	Type      MessageType     `json:"type"`
	ID        string          `json:"id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp int64           `json:"ts,omitempty"`
}

type RegisterPayload struct {
	Name         string   `json:"name"`
	Capabilities []string `json:"capabilities,omitempty"`
	Model        string   `json:"model,omitempty"`
	Version      string   `json:"version,omitempty"`
}

type TaskPayload struct {
	TaskID    string `json:"task_id"`
	SessionID string `json:"session_id"`
	Prompt    string `json:"prompt"`
}

type OutputPayload struct {
	TaskID    string `json:"task_id"`
	SessionID string `json:"session_id"`
	EventType string `json:"event_type"`
	Content   string `json:"content,omitempty"`
	Done      bool   `json:"done,omitempty"`
	Error     string `json:"error,omitempty"`
}

type HeartbeatPayload struct {
	ActiveTasks int    `json:"active_tasks"`
	Status      string `json:"status,omitempty"`
}

type Daemon struct {
	bridgeURL string
	name      string
	model     string
	claudeCmd string
	workDir   string
	conn      *websocket.Conn
	connMu    sync.Mutex
	tasks     sync.WaitGroup
	taskCount int
	taskMu    sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
}

func main() {
	bridgeURL := flag.String("bridge", defaultBridgeURL, "Bridge WebSocket URL")
	name := flag.String("name", "", "Agent name to register (required)")
	model := flag.String("model", "claude-sonnet-4-6", "Model name to report")
	claudeCmd := flag.String("cmd", "claude", "Claude CLI command")
	workDir := flag.String("work-dir", ".", "Working directory for claude")
	logLevel := flag.String("log-level", "info", "Log level (debug/info/warn/error)")
	flag.Parse()

	if *name == "" {
		fmt.Fprintf(os.Stderr, "Error: -name is required\n")
		flag.Usage()
		os.Exit(1)
	}

	// Setup logging
	var level slog.Level
	switch strings.ToLower(*logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := &Daemon{
		bridgeURL: *bridgeURL,
		name:      *name,
		model:     *model,
		claudeCmd: *claudeCmd,
		workDir:   *workDir,
		ctx:       ctx,
		cancel:    cancel,
	}

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("shutting down...")
		cancel()
	}()

	d.run()
}

func (d *Daemon) run() {
	for {
		select {
		case <-d.ctx.Done():
			return
		default:
		}

		if err := d.connect(); err != nil {
			slog.Error("connection failed", "error", err)
			select {
			case <-d.ctx.Done():
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		d.readLoop()
		slog.Info("disconnected, reconnecting...")

		select {
		case <-d.ctx.Done():
			return
		case <-time.After(reconnectDelay):
		}
	}
}

func (d *Daemon) connect() error {
	slog.Info("connecting to bridge", "url", d.bridgeURL)
	conn, _, err := websocket.DefaultDialer.DialContext(d.ctx, d.bridgeURL, nil)
	if err != nil {
		return fmt.Errorf("ws dial: %w", err)
	}

	d.connMu.Lock()
	d.conn = conn
	d.connMu.Unlock()

	// Send register
	payload, _ := json.Marshal(RegisterPayload{
		Name:    d.name,
		Model:   d.model,
		Version: "0.1.0",
	})
	msg := Message{
		Type:      MsgRegister,
		Payload:   payload,
		Timestamp: time.Now().UnixMilli(),
	}
	if err := conn.WriteJSON(msg); err != nil {
		conn.Close()
		return fmt.Errorf("register: %w", err)
	}
	slog.Info("registered with bridge", "name", d.name)

	// Start heartbeat
	go d.heartbeatLoop(conn)

	return nil
}

func (d *Daemon) heartbeatLoop(conn *websocket.Conn) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			d.taskMu.Lock()
			count := d.taskCount
			d.taskMu.Unlock()

			payload, _ := json.Marshal(HeartbeatPayload{
				ActiveTasks: count,
				Status:      "ready",
			})
			msg := Message{
				Type:      MsgHeartbeat,
				Payload:   payload,
				Timestamp: time.Now().UnixMilli(),
			}

			d.connMu.Lock()
			err := conn.WriteJSON(msg)
			d.connMu.Unlock()

			if err != nil {
				slog.Debug("heartbeat send failed", "error", err)
				return
			}
		}
	}
}

func (d *Daemon) readLoop() {
	conn := d.conn
	conn.SetReadLimit(1 << 20)

	for {
		select {
		case <-d.ctx.Done():
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(readTimeout))
		_, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				slog.Warn("read error", "error", err)
			}
			return
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			slog.Warn("invalid message", "error", err)
			continue
		}

		switch msg.Type {
		case MsgTask:
			var task TaskPayload
			if err := json.Unmarshal(msg.Payload, &task); err != nil {
				slog.Warn("invalid task payload", "error", err)
				continue
			}
			go d.handleTask(&task)
		default:
			slog.Debug("ignoring message", "type", msg.Type)
		}
	}
}

func (d *Daemon) handleTask(task *TaskPayload) {
	d.taskMu.Lock()
	d.taskCount++
	d.taskMu.Unlock()
	defer func() {
		d.taskMu.Lock()
		d.taskCount--
		d.taskMu.Unlock()
	}()

	slog.Info("task received", "task_id", task.TaskID, "session", task.SessionID, "prompt_len", len(task.Prompt))

	// Spawn claude process
	args := []string{
		"--print",
		"--output-format", "text",
	}
	if d.workDir != "" && d.workDir != "." {
		args = append(args, "--cwd", d.workDir)
	}
	args = append(args, task.Prompt)

	ctx, cancel := context.WithTimeout(d.ctx, 10*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, d.claudeCmd, args...)
	cmd.Dir = d.workDir

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		d.sendOutput(task, "", true, fmt.Sprintf("stdout pipe: %v", err))
		return
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		d.sendOutput(task, "", true, fmt.Sprintf("start: %v", err))
		return
	}

	// Stream output line by line
	buf := make([]byte, 4096)
	for {
		n, readErr := stdout.Read(buf)
		if n > 0 {
			d.sendOutput(task, string(buf[:n]), false, "")
		}
		if readErr != nil {
			if readErr != io.EOF {
				slog.Warn("read error", "task", task.TaskID, "error", readErr)
			}
			break
		}
	}

	if err := cmd.Wait(); err != nil {
		d.sendOutput(task, "", true, fmt.Sprintf("process exited: %v", err))
		return
	}

	// Send done
	d.sendOutput(task, "", true, "")
	slog.Info("task completed", "task_id", task.TaskID)
}

func (d *Daemon) sendOutput(task *TaskPayload, content string, done bool, errMsg string) {
	eventType := "text"
	if done && errMsg == "" {
		eventType = "result"
	} else if errMsg != "" {
		eventType = "error"
	}

	payload, _ := json.Marshal(OutputPayload{
		TaskID:    task.TaskID,
		SessionID: task.SessionID,
		EventType: eventType,
		Content:   content,
		Done:      done,
		Error:     errMsg,
	})

	msg := Message{
		Type:      MsgOutput,
		ID:        task.TaskID,
		Payload:   payload,
		Timestamp: time.Now().UnixMilli(),
	}

	d.connMu.Lock()
	defer d.connMu.Unlock()
	if d.conn != nil {
		if err := d.conn.WriteJSON(msg); err != nil {
			slog.Warn("send output failed", "task", task.TaskID, "error", err)
		}
	}
}
