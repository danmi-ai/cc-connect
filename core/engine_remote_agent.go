package core

import (
	"fmt"
	"strings"
	"sync"
)

// AgentBinding maps a user (session key) to a specific agent name.
// When set, messages from that user are routed to the bound agent
// instead of the default engine agent.
type AgentBindingManager struct {
	mu       sync.RWMutex
	bindings map[string]string // sessionKey → agent name (e.g. "remote:myagent")
}

func NewAgentBindingManager() *AgentBindingManager {
	return &AgentBindingManager{
		bindings: make(map[string]string),
	}
}

func (m *AgentBindingManager) Bind(sessionKey, agentName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if agentName == "" {
		delete(m.bindings, sessionKey)
	} else {
		m.bindings[sessionKey] = agentName
	}
}

func (m *AgentBindingManager) Get(sessionKey string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.bindings[sessionKey]
}

func (m *AgentBindingManager) Unbind(sessionKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.bindings, sessionKey)
}

// SetRemoteBridge attaches a RemoteAgentBridge to the engine.
func (e *Engine) SetRemoteBridge(bridge *RemoteAgentBridge) {
	e.remoteBridge = bridge
}

// GetRemoteBridge returns the engine's remote agent bridge (may be nil).
func (e *Engine) GetRemoteBridge() *RemoteAgentBridge {
	return e.remoteBridge
}

// SetAgentBindings attaches the per-user agent binding manager.
func (e *Engine) SetAgentBindings(m *AgentBindingManager) {
	e.agentBindings = m
}

// GetBoundAgent returns the agent name bound to a session key, or "".
func (e *Engine) GetBoundAgent(sessionKey string) string {
	if e.agentBindings == nil {
		return ""
	}
	return e.agentBindings.Get(sessionKey)
}

// resolveRemoteAgent looks up a remote agent by its binding name (e.g. "remote:myagent").
func (e *Engine) resolveRemoteAgent(boundName string) Agent {
	nodeName := strings.TrimPrefix(boundName, "remote:")
	node := e.remoteBridge.GetNode(nodeName)
	if node == nil || !node.isAlive() {
		return nil
	}
	return NewRemoteAgent(e.remoteBridge, nodeName)
}

// cmdAgent handles /agent [list|<name>|unbind]
func (e *Engine) cmdAgent(p Platform, msg *Message, args []string) {
	if e.remoteBridge == nil {
		e.reply(p, msg.ReplyCtx, "Remote agent bridge is not enabled.")
		return
	}

	sub := ""
	if len(args) > 0 {
		sub = strings.ToLower(args[0])
	}

	switch sub {
	case "", "list":
		nodes := e.remoteBridge.ListNodes()
		if len(nodes) == 0 {
			e.reply(p, msg.ReplyCtx, "No remote agents connected.")
			return
		}
		var sb strings.Builder
		sb.WriteString("**Remote Agents:**\n")
		for _, n := range nodes {
			status := "alive"
			if !n.isAlive() {
				status = "stale"
			}
			model := n.Model
			if model == "" {
				model = "unknown"
			}
			sb.WriteString(fmt.Sprintf("- **%s** (model: %s, status: %s)\n", n.Name, model, status))
		}
		// Show current binding
		if e.agentBindings != nil {
			bound := e.agentBindings.Get(msg.SessionKey)
			if bound != "" {
				sb.WriteString(fmt.Sprintf("\nCurrent binding: **%s**", bound))
			} else {
				sb.WriteString(fmt.Sprintf("\nCurrent: default agent (%s)", e.agent.Name()))
			}
		}
		e.reply(p, msg.ReplyCtx, sb.String())

	case "unbind", "default", "local":
		if e.agentBindings != nil {
			e.agentBindings.Unbind(msg.SessionKey)
		}
		e.reply(p, msg.ReplyCtx, fmt.Sprintf("Switched back to default agent: **%s**", e.agent.Name()))

	default:
		// Try to bind to a remote agent
		nodeName := sub
		node := e.remoteBridge.GetNode(nodeName)
		if node == nil {
			// Try case-insensitive match
			for _, n := range e.remoteBridge.ListNodes() {
				if strings.EqualFold(n.Name, nodeName) {
					node = n
					nodeName = n.Name
					break
				}
			}
		}
		if node == nil {
			e.reply(p, msg.ReplyCtx, fmt.Sprintf("Agent %q not found. Use `/agent list` to see available agents.", nodeName))
			return
		}
		if !node.isAlive() {
			e.reply(p, msg.ReplyCtx, fmt.Sprintf("Agent %q is not responding (heartbeat stale).", nodeName))
			return
		}
		if e.agentBindings == nil {
			e.agentBindings = NewAgentBindingManager()
		}
		e.agentBindings.Bind(msg.SessionKey, "remote:"+nodeName)
		e.reply(p, msg.ReplyCtx, fmt.Sprintf("Bound to remote agent: **%s** (model: %s)", nodeName, node.Model))
	}
}
