package infoflow

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

func init() {
	core.RegisterPlatform("infoflow", New)
}

// replyContext holds enough info to reply or send a proactive message.
type replyContext struct {
	groupID   int64  // non-zero for group chat
	userID    string // sender's uuapName
	messageID string // original message ID (for quoting)
	isGroup   bool
	proactive bool   // true when constructed by ReconstructReplyCtx (no incoming message)
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

	handler     core.MessageHandler
	mu          sync.Mutex
	accessToken string
	tokenExpiry time.Time

	httpClient *http.Client
	wsConn     *wsClient // WebSocket long-connection
	cancel     context.CancelFunc
	dedup      *core.MessageDedup
}

// New creates a new Infoflow platform from config options.
// Required: app_key, app_secret, agent_id
// Optional: robot_im_id, allow_from, base_url, ws_gateway, ws_connect_domain
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
		httpClient:            &http.Client{Timeout: 30 * time.Second},
		dedup:                 &core.MessageDedup{},
	}, nil
}

func (p *Platform) Name() string { return "infoflow" }

// ─── Token Management ─────────────────────────────────────────────────────────

func md5hex(s string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(s)))
}

func (p *Platform) fetchToken(ctx context.Context) (string, error) {
	body, _ := json.Marshal(map[string]string{
		"app_key":    p.appKey,
		"app_secret": md5hex(p.appSecret),
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/auth/app_access_token", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("infoflow: token request failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Code string `json:"code"`
		Data struct {
			AppAccessToken string `json:"app_access_token"`
			Expire         int    `json:"expire"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("infoflow: token decode failed: %w", err)
	}
	if result.Code != "ok" {
		return "", fmt.Errorf("infoflow: token API returned code=%s", result.Code)
	}
	return result.Data.AppAccessToken, nil
}

func (p *Platform) getToken(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.accessToken != "" && time.Now().Before(p.tokenExpiry) {
		return p.accessToken, nil
	}
	token, err := p.fetchToken(ctx)
	if err != nil {
		return "", err
	}
	p.accessToken = token
	p.tokenExpiry = time.Now().Add(110 * time.Minute) // expire slightly before actual 7200s
	return token, nil
}

// ─── Robot Profile (fetch robotImID if not configured) ─────────────────────────

func (p *Platform) fetchRobotImID(ctx context.Context) error {
	if p.robotImID != 0 {
		return nil
	}
	token, err := p.getToken(ctx)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]any{})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/imRobot/detail", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer-"+token)
	req.Header.Set("X-LogId", fmt.Sprintf("%d", time.Now().UnixMilli()))

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("infoflow: robot detail request failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Code string `json:"code"`
		Data struct {
			Data struct {
				ImID int64 `json:"imId"`
			} `json:"data"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("infoflow: robot detail decode failed: %w", err)
	}
	if result.Code != "ok" {
		return fmt.Errorf("infoflow: robot detail returned code=%s", result.Code)
	}
	p.robotImID = result.Data.Data.ImID
	slog.Info("infoflow: robot profile fetched", "robotImID", p.robotImID)
	return nil
}

// ─── Start / Stop ─────────────────────────────────────────────────────────────

func (p *Platform) Start(handler core.MessageHandler) error {
	p.handler = handler
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	// Fetch robotImID so @ detection works
	if err := p.fetchRobotImID(ctx); err != nil {
		slog.Warn("infoflow: failed to fetch robot profile; @ detection may be impaired", "error", err)
	}

	// Start WebSocket long-connection loop with reconnect
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

// onIncomingMessage is called by wsClient for each valid inbound message.
func (p *Platform) onIncomingMessage(raw map[string]any) {
	eventType, _ := raw["eventtype"].(string)

	var msg *core.Message
	var rctx replyContext

	switch eventType {
	case "MESSAGE_RECEIVE":
		// Group @-message
		groupID := toInt64(raw["groupid"])
		message, _ := raw["message"].(map[string]any)
		header, _ := message["header"].(map[string]any)
		body, _ := message["body"].([]any)

		senderID, _ := header["fromuserid"].(string)
		msgID, _ := header["messageid"].(string)

		if !p.isBotMentioned(header, body) {
			return
		}
		if !core.AllowList(p.allowFrom, senderID) {
			slog.Debug("infoflow: message from unauthorized user", "user", senderID)
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
		msg = &core.Message{
			Platform:   "infoflow",
			SessionKey: sessionKey,
			MessageID:  msgID,
			UserID:     senderID,
			Content:    text,
			ReplyCtx:   rctx,
		}

	default:
		// Private (single chat) message
		fromUserID, _ := raw["FromUserId"].(string)
		msgID, _ := raw["MsgId"].(string)
		msgType, _ := raw["MsgType"].(string)

		if strings.ToLower(msgType) == "event" {
			return // ignore subscribe/entry events
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

// ReconstructReplyCtx lets cron/proactive sends work by parsing a session key.
func (p *Platform) ReconstructReplyCtx(sessionKey string) (any, error) {
	if strings.HasPrefix(sessionKey, "group:") {
		parts := strings.SplitN(strings.TrimPrefix(sessionKey, "group:"), ":user:", 2)
		groupID := toInt64FromStr(parts[0])
		userID := ""
		if len(parts) == 2 {
			userID = parts[1]
		}
		return replyContext{groupID: groupID, userID: userID, isGroup: true}, nil
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

func (p *Platform) Send(ctx context.Context, replyCtxAny any, content string) error {
	rctx, ok := replyCtxAny.(replyContext)
	if !ok {
		return fmt.Errorf("infoflow: unexpected reply context type %T", replyCtxAny)
	}
	token, err := p.getToken(ctx)
	if err != nil {
		return err
	}
	if rctx.isGroup {
		return p.sendToGroup(ctx, token, rctx.groupID, rctx.userID, content)
	}
	return p.sendToDM(ctx, token, rctx.userID, content)
}

// sendToGroup sends a Markdown message to a group, optionally @-mentioning the user.
func (p *Platform) sendToGroup(ctx context.Context, token string, groupID int64, userID, content string) error {
	// 如流群消息 API body 格式
	body := map[string]any{
		"message": map[string]any{
			"header": map[string]any{
				"toid":         groupID,
				"totype":       "GROUP",
				"msgtype":      "MD",
				"clientmsgid":  time.Now().UnixMilli(),
				"role":         "robot",
			},
			"body": buildGroupMDBody(userID, content),
		},
	}
	return p.doPost(ctx, token, "/robot/msg/groupmsgsend", body)
}

// sendToDM sends a Markdown message to a single user.
func (p *Platform) sendToDM(ctx context.Context, token string, userID, content string) error {
	body := map[string]any{
		"message": map[string]any{
			"header": map[string]any{
				"toid":        userID,
				"totype":      "USER",
				"msgtype":     "MD",
				"clientmsgid": time.Now().UnixMilli(),
				"role":        "robot",
			},
			"body": []map[string]any{
				{"type": "MD", "content": content},
			},
		},
	}
	return p.doPost(ctx, token, "/robot/msg/singlemsgsend", body)
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

// ─── HTTP Helper ───────────────────────────────────────────────────────────────

func (p *Platform) doPost(ctx context.Context, token, path string, body any) error {
	data, _ := json.Marshal(body)
	slog.Info("infoflow: POST request", "path", path, "body_len", len(data), "body", string(data))
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+path, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer-"+token)
	req.Header.Set("X-LogId", fmt.Sprintf("%d", time.Now().UnixMilli()))

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("infoflow: POST %s failed: %w", path, err)
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	slog.Info("infoflow: POST response", "path", path, "status", resp.StatusCode, "body", string(b))

	var result struct {
		Code    any    `json:"code"`
		Msg     string `json:"msg"`
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.Unmarshal(b, &result); err != nil {
		return fmt.Errorf("infoflow: POST %s response decode failed: %w", path, err)
	}
	// Check multiple error formats
	if result.ErrCode != 0 {
		return fmt.Errorf("infoflow: POST %s errcode=%d errmsg=%s", path, result.ErrCode, result.ErrMsg)
	}
	switch c := result.Code.(type) {
	case string:
		if c != "ok" && c != "" {
			return fmt.Errorf("infoflow: POST %s code=%s msg=%s", path, c, result.Msg)
		}
	case float64:
		if c != 0 {
			return fmt.Errorf("infoflow: POST %s code=%v msg=%s", path, c, result.Msg)
		}
	}
	return nil
}

// ─── @ Detection ───────────────────────────────────────────────────────────────

func (p *Platform) isBotMentioned(header map[string]any, body []any) bool {
	// Check tolist (agentID as string)
	if toList, ok := header["tolist"].([]any); ok {
		agentIDStr := fmt.Sprintf("%d", p.agentID)
		for _, v := range toList {
			if fmt.Sprintf("%v", v) == agentIDStr {
				return true
			}
		}
	}
	// Check at.atrobotids
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
		// Check body AT blocks
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
		case "TEXT":
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
	// Remove leading @botname patterns like "@my-bot " or "@机器人名 "
	for strings.HasPrefix(text, "@") {
		idx := strings.IndexByte(text, ' ')
		if idx < 0 {
			return ""
		}
		text = strings.TrimSpace(text[idx+1:])
	}
	return text
}

// ─── Helpers ───────────────────────────────────────────────────────────────────

func toInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}

func toInt64FromStr(s string) int64 {
	var n int64
	fmt.Sscanf(s, "%d", &n)
	return n
}
