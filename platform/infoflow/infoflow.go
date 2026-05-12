package infoflow

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

func init() {
	core.RegisterPlatform("infoflow", New)
}

// replyContext holds enough info to reply or send a proactive message.
type replyContext struct {
	groupID   int64
	userID    string
	messageID string
	isGroup   bool
	proactive bool
}

// Platform implements core.Platform for Baidu Infoflow (如流).
type Platform struct {
	appKey                string
	appSecret             string
	agentID               int64
	robotImID             int64
	allowFrom             string
	shareSessionInChannel bool
	baseURL               string
	wsGateway             string
	wsConnectDomain       string

	// Card/progress configuration
	progressStyle   string // "legacy" | "compact" | "card"
	reactionEmoji   string // emoji code for typing indicator, "" to disable
	doneEmoji       string // emoji code for completion, "" to disable
	cardThrottleMs   int    // minimum ms between card updates
	cardDegradeUntil time.Time

	handler     core.MessageHandler
	mu          sync.Mutex
	accessToken string
	tokenExpiry time.Time

	httpClient *http.Client
	wsConn     *wsClient
	cancel     context.CancelFunc
	dedup      *core.MessageDedup

	// Card degradation state

	// Message recall tracking
	recalledMsgs     *sync.Map
	recalledMsgsOnce sync.Once

	// Unique message ID counter for clientmsgid
	msgIDCounter atomic.Int64

	// Per-session first-reply tracking for @-dedup
	replyCounters sync.Map // sessionKey -> *atomic.Int32
}

// Compile-time interface checks.
var (
	_ core.Platform                    = (*Platform)(nil)
	_ core.ReplyContextReconstructor   = (*Platform)(nil)
	_ core.StreamingCardPlatform       = (*Platform)(nil)
	_ core.FormattingInstructionProvider = (*Platform)(nil)
)

// New creates a new Infoflow platform from config options.
func New(opts map[string]any) (core.Platform, error) {
	appKey, _ := opts["app_key"].(string)
	appSecret, _ := opts["app_secret"].(string)
	if appKey == "" || appSecret == "" {
		return nil, fmt.Errorf("infoflow: app_key and app_secret are required")
	}

	var agentID int64
	switch v := opts["agent_id"].(type) {
	case float64:
		agentID = int64(v)
	case int64:
		agentID = v
	case int:
		agentID = int64(v)
	}

	var robotImID int64
	switch v := opts["robot_im_id"].(type) {
	case float64:
		robotImID = int64(v)
	case int64:
		robotImID = v
	case int:
		robotImID = int64(v)
	}

	allowFrom, _ := opts["allow_from"].(string)
	core.CheckAllowFrom("infoflow", allowFrom)
	shareSessionInChannel, _ := opts["share_session_in_channel"].(bool)

	baseURL, _ := opts["base_url"].(string)
	if baseURL == "" {
		baseURL = "http://apiin.im.baidu.com/api/v1"
	}
	wsGateway, _ := opts["ws_gateway"].(string)
	if wsGateway == "" {
		wsGateway = "infoflow-open-gateway.weiyun.baidu.com"
	}
	wsConnectDomain, _ := opts["ws_connect_domain"].(string)
	if wsConnectDomain == "" {
		wsConnectDomain = "infoflow-open-gateway.weiyun.baidu.com:8869"
	}

	progressStyle, _ := opts["progress_style"].(string)
	if progressStyle == "" {
		progressStyle = "card"
	}
	reactionEmoji, _ := opts["reaction_emoji"].(string)
	if reactionEmoji == "" {
		reactionEmoji = "d18"
	}
	if reactionEmoji == "none" {
		reactionEmoji = ""
	}
	doneEmoji, _ := opts["done_emoji"].(string)
	if doneEmoji == "" {
		doneEmoji = "d01"
	}
	if doneEmoji == "none" {
		doneEmoji = ""
	}
	cardThrottleMs := 1500
	if v, ok := opts["card_throttle_ms"].(float64); ok && v > 0 {
		cardThrottleMs = int(v)
	}

	return &Platform{
		appKey:                appKey,
		appSecret:             appSecret,
		agentID:               agentID,
		robotImID:             robotImID,
		allowFrom:             allowFrom,
		shareSessionInChannel: shareSessionInChannel,
		baseURL:               baseURL,
		wsGateway:             wsGateway,
		wsConnectDomain:       wsConnectDomain,
		progressStyle:         progressStyle,
		reactionEmoji:         reactionEmoji,
		doneEmoji:             doneEmoji,
		cardThrottleMs:        cardThrottleMs,
		httpClient:            &http.Client{Timeout: 30 * time.Second},
		dedup:                 &core.MessageDedup{},
	}, nil
}

func (p *Platform) Name() string { return "infoflow" }

// FormattingInstructions returns platform-specific formatting guidance for the agent.
func (p *Platform) FormattingInstructions() string {
	return `You are replying on Infoflow (如流). Use standard Markdown. Keep code blocks under 100 lines to avoid truncation. Use headings (##) to organize long responses.`
}

// ─── Start / Stop ─────────────────────────────────────────────────────────────

func (p *Platform) Start(handler core.MessageHandler) error {
	p.handler = handler
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	if err := p.fetchRobotImID(ctx); err != nil {
		slog.Warn("infoflow: failed to fetch robot profile; @ detection may be impaired", "error", err)
	}

	// Initialize recall tracker with context-bound cleanup
	p.initRecallTracker()
	go p.recallCleanupLoop(ctx)

	p.wsConn = newWSClient(p)
	go p.wsConn.runLoop(ctx)

	slog.Info("infoflow: platform started", "agentID", p.agentID, "robotImID", p.robotImID)
	return nil
}

func (p *Platform) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}
	return nil
}

// ─── Message Dispatch ──────────────────────────────────────────────────────────

func (p *Platform) onIncomingMessage(raw map[string]any) {
	eventType, _ := raw["eventtype"].(string)
	slog.Info("infoflow: raw message received", "eventType", eventType, "keys", mapKeys(raw))

	var msg *core.Message
	var rctx replyContext

	switch eventType {
	case "MESSAGE_RECEIVE":
		groupID := toInt64(raw["groupid"])
		message, _ := raw["message"].(map[string]any)
		header, _ := message["header"].(map[string]any)
		body, _ := message["body"].([]any)

		senderID, _ := header["fromuserid"].(string)
		if senderID == "" {
			// Bot-sent messages may not have fromuserid; use fromid as fallback
			senderID = fmt.Sprintf("%d", toInt64(raw["fromid"]))
		}
		msgID, _ := header["messageid"].(string)

		slog.Info("infoflow: MESSAGE_RECEIVE", "senderID", senderID, "msgID", msgID, "fromid", raw["fromid"], "targetAgentId", raw["targetAgentId"], "agentID", p.agentID)

		if !p.isBotMentioned(header, body, raw) {
			slog.Info("infoflow: not mentioned, skipping")
			return
		}
		// allow_from check: skip for bot-originated messages (no fromuserid)
		humanSender, _ := header["fromuserid"].(string)
		if humanSender != "" && !core.AllowList(p.allowFrom, humanSender) {
			slog.Debug("infoflow: message from unauthorized user", "user", humanSender)
			return
		}
		if p.dedup.IsDuplicate(msgID) {
			return
		}

		text := extractGroupText(body)
		text = removeBotMention(text)

		rctx = replyContext{
			groupID:   groupID,
			userID:    senderID,
			messageID: msgID,
			isGroup:   true,
		}
		sessionKey := p.sessionKey(rctx)
		// Reset reply counter for this session on new incoming message
		p.resetReplyCounter(sessionKey)
		msg = &core.Message{
			Platform:   "infoflow",
			SessionKey: sessionKey,
			MessageID:  msgID,
			UserID:     senderID,
			Content:    text,
			ReplyCtx:   rctx,
		}

	default:
		fromUserID, _ := raw["FromUserId"].(string)
		msgID, _ := raw["MsgId"].(string)
		msgType, _ := raw["MsgType"].(string)

		if strings.ToLower(msgType) == "event" {
			return
		}
		if !core.AllowList(p.allowFrom, fromUserID) {
			return
		}
		if p.dedup.IsDuplicate(msgID) {
			return
		}

		text := extractPrivateText(raw)
		rctx = replyContext{
			userID:    fromUserID,
			messageID: msgID,
			isGroup:   false,
		}
		sessionKey := p.sessionKey(rctx)
		p.resetReplyCounter(sessionKey)
		msg = &core.Message{
			Platform:   "infoflow",
			SessionKey: sessionKey,
			MessageID:  msgID,
			UserID:     fromUserID,
			Content:    text,
			ReplyCtx:   rctx,
		}
	}

	if msg == nil || strings.TrimSpace(msg.Content) == "" {
		return
	}

	go p.handler(p, msg)
}

// ─── Session Key ───────────────────────────────────────────────────────────────

func (p *Platform) sessionKey(rctx replyContext) string {
	if rctx.isGroup {
		if p.shareSessionInChannel {
			return fmt.Sprintf("group:%d", rctx.groupID)
		}
		return fmt.Sprintf("group:%d:user:%s", rctx.groupID, rctx.userID)
	}
	return fmt.Sprintf("dm:%s", rctx.userID)
}

func (p *Platform) ReconstructReplyCtx(sessionKey string) (any, error) {
	if strings.HasPrefix(sessionKey, "group:") {
		parts := strings.SplitN(strings.TrimPrefix(sessionKey, "group:"), ":user:", 2)
		groupID := toInt64FromStr(parts[0])
		userID := ""
		if len(parts) == 2 {
			userID = parts[1]
		}
		return replyContext{groupID: groupID, userID: userID, isGroup: true, proactive: true}, nil
	}
	if strings.HasPrefix(sessionKey, "dm:") {
		userID := strings.TrimPrefix(sessionKey, "dm:")
		return replyContext{userID: userID, isGroup: false, proactive: true}, nil
	}
	return nil, fmt.Errorf("infoflow: cannot reconstruct reply context from key=%q", sessionKey)
}

// ─── Reply / Send ──────────────────────────────────────────────────────────────

func (p *Platform) Reply(ctx context.Context, replyCtxAny any, content string) error {
	return p.Send(ctx, replyCtxAny, content)
}

const infoflowMaxMessageLen = 4000

func (p *Platform) Send(ctx context.Context, replyCtxAny any, content string) error {
	rctx, ok := replyCtxAny.(replyContext)
	if !ok {
		return fmt.Errorf("infoflow: unexpected reply context type %T", replyCtxAny)
	}
	chunks := core.SplitMessageCodeFenceAware(content, infoflowMaxMessageLen)
	for _, chunk := range chunks {
		var err error
		if rctx.isGroup {
			err = p.sendToGroup(ctx, rctx, chunk)
		} else {
			err = p.sendToDM(ctx, rctx.userID, chunk)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Platform) sendToGroup(ctx context.Context, rctx replyContext, content string) error {
	atUserID := ""
	sessionKey := p.sessionKey(rctx)
	if p.isFirstReply(sessionKey) {
		atUserID = rctx.userID
	}

	body := map[string]any{
		"message": map[string]any{
			"header": map[string]any{
				"toid":        rctx.groupID,
				"totype":      "GROUP",
				"msgtype":     "MD",
				"clientmsgid": p.nextMsgID(),
				"role":        "robot",
			},
			"body": buildGroupMDBody(atUserID, content),
		},
	}
	return p.doPost(ctx, "/robot/msg/groupmsgsend", body)
}

func (p *Platform) sendToDM(ctx context.Context, userID, content string) error {
	body := map[string]any{
		"message": map[string]any{
			"header": map[string]any{
				"toid":        userID,
				"totype":      "USER",
				"msgtype":     "MD",
				"clientmsgid": p.nextMsgID(),
				"role":        "robot",
			},
			"body": []map[string]any{
				{"type": "MD", "content": content},
			},
		},
	}
	return p.doPost(ctx, "/robot/msg/singlemsgsend", body)
}

func (p *Platform) nextMsgID() int64 {
	return time.Now().UnixMilli()*1000 + p.msgIDCounter.Add(1)%1000
}

func buildGroupMDBody(userID, content string) []map[string]any {
	body := []map[string]any{}
	if userID != "" {
		body = append(body, map[string]any{
			"type":      "AT",
			"atall":     false,
			"atuserids": []string{userID},
		})
		content = "@" + userID + "\n\n" + content
	}
	body = append(body, map[string]any{
		"type":    "MD",
		"content": content,
	})
	return body
}

// ─── @-dedup (only first reply per turn mentions the user) ────────────────────

func (p *Platform) resetReplyCounter(sessionKey string) {
	p.replyCounters.Store(sessionKey, &atomic.Int32{})
}

func (p *Platform) isFirstReply(sessionKey string) bool {
	v, ok := p.replyCounters.Load(sessionKey)
	if !ok {
		return true
	}
	counter := v.(*atomic.Int32)
	return counter.Add(1) == 1
}

// ─── Card degradation ─────────────────────────────────────────────────────────

// ─── @ Detection ───────────────────────────────────────────────────────────────

func (p *Platform) isBotMentioned(header map[string]any, body []any, raw map[string]any) bool {
	// Check targetAgentId (most reliable for bot-to-bot @)
	if targetAgent := toInt64(raw["targetAgentId"]); targetAgent == p.agentID && targetAgent != 0 {
		return true
	}
	if toList, ok := header["tolist"].([]any); ok {
		agentIDStr := fmt.Sprintf("%d", p.agentID)
		for _, v := range toList {
			if fmt.Sprintf("%v", v) == agentIDStr {
				return true
			}
		}
	}
	if at, ok := header["at"].(map[string]any); ok {
		if robotIDs, ok := at["atrobotids"].([]any); ok {
			for _, v := range robotIDs {
				id := toInt64(v)
				if id == p.robotImID {
					return true
				}
			}
		}
	}
	if p.robotImID != 0 {
		for _, block := range body {
			b, _ := block.(map[string]any)
			if strings.ToUpper(fmt.Sprintf("%v", b["type"])) == "AT" {
				if toInt64(b["robotid"]) == p.robotImID {
					return true
				}
			}
		}
	}
	return false
}

// ─── Text Extraction ───────────────────────────────────────────────────────────

func extractGroupText(body []any) string {
	var parts []string
	for _, block := range body {
		b, _ := block.(map[string]any)
		switch strings.ToUpper(fmt.Sprintf("%v", b["type"])) {
		case "TEXT", "MD":
			if s, ok := b["content"].(string); ok {
				parts = append(parts, s)
			}
		case "AT":
			// skip
		}
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

func extractPrivateText(raw map[string]any) string {
	if content, ok := raw["Content"].(string); ok {
		return strings.TrimSpace(content)
	}
	return ""
}

func removeBotMention(text string) string {
	for strings.HasPrefix(text, "@") {
		idx := strings.IndexByte(text, ' ')
		if idx < 0 {
			return ""
		}
		text = strings.TrimSpace(text[idx+1:])
	}
	return text
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
