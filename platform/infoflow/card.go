package infoflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// Compile-time check: streamingCard implements core.StreamingCard.
var _ core.StreamingCard = (*streamingCard)(nil)

// Streaming card API constants (from openclaw-infoflow-plugin).
const (
	streamingCreatePath        = "/api/v1/msg/sender/interactivity_msg"
	streamingUpdateGroupPath   = "/api/v1/msg/modifier/dynamic_content"
	streamingUpdatePersonalPath = "/api/v1/msg/modifier/interactivity_personal_msg_content"
	streamingTemplateName      = "streaming_render"
	streamingTemplateVersion   = 30
	streamingGroupVersionStart = 111
	streamingExpireSeconds     = 7776000 // 90 days
)

// streamingCard aggregates an entire agent turn into one updatable interactive card.
type streamingCard struct {
	platform    *Platform
	groupID     int64
	userID      string
	isGroup     bool
	modifyToken string
	messageID   string

	mu                 sync.Mutex
	state              string // "processing" | "finished" | "failed"
	groupUpdateVersion int
	lastSentContent    string
	lastSentAt         time.Time
	failed             bool

	// Throttle: single-flight + latest-wins
	throttleMs     int
	pendingContent string
	timer          *time.Timer
	inFlight       bool
}

// CreateStreamingCard implements core.StreamingCardPlatform.
func (p *Platform) CreateStreamingCard(ctx context.Context, replyCtxAny any) (core.StreamingCard, error) {
	if p.isCardDegraded() {
		return nil, fmt.Errorf("infoflow: card API degraded")
	}

	rctx, ok := replyCtxAny.(replyContext)
	if !ok {
		return nil, fmt.Errorf("infoflow: invalid reply context for card")
	}

	// Build initial card content
	initialContent := buildCardJSON("⏳ 处理中...")

	modifyToken, messageID, err := p.createStreamingCard(ctx, rctx, initialContent)
	if err != nil {
		slog.Error("infoflow: streaming card create failed", "error", err)
		p.activateCardDegrade(err.Error())
		return nil, err
	}

	card := &streamingCard{
		platform:           p,
		groupID:            rctx.groupID,
		userID:             rctx.userID,
		isGroup:            rctx.isGroup,
		modifyToken:        modifyToken,
		messageID:          messageID,
		state:              "processing",
		groupUpdateVersion: streamingGroupVersionStart,
		throttleMs:         300,
	}

	slog.Info("infoflow: streaming card created", "messageID", messageID, "isGroup", rctx.isGroup)
	return card, nil
}

// Update replaces the card content. Throttled.
func (c *streamingCard) Update(ctx context.Context, content string) error {
	c.mu.Lock()
	if c.state == "finished" || c.state == "failed" {
		c.mu.Unlock()
		return nil
	}
	c.pendingContent = content

	// If nothing in flight and throttle window passed, send immediately
	if !c.inFlight && time.Since(c.lastSentAt) >= time.Duration(c.throttleMs)*time.Millisecond {
		c.inFlight = true
		c.mu.Unlock()
		go c.flush(context.Background())
		return nil
	}

	// Otherwise schedule a flush
	if c.timer == nil {
		c.timer = time.AfterFunc(time.Duration(c.throttleMs)*time.Millisecond, func() {
			c.mu.Lock()
			c.timer = nil
			if c.state == "finished" || c.state == "failed" || c.pendingContent == "" {
				c.mu.Unlock()
				return
			}
			c.inFlight = true
			c.mu.Unlock()
			c.flush(context.Background())
		})
	}
	c.mu.Unlock()
	return nil
}

// Finalize sends the final content and marks the card as complete.
func (c *streamingCard) Finalize(ctx context.Context, content string) error {
	c.mu.Lock()
	if c.state == "finished" || c.state == "failed" {
		c.mu.Unlock()
		return nil
	}
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.state = "finished"
	c.pendingContent = ""
	c.mu.Unlock()

	// Send final update
	cardContent := buildCardJSON(content)
	return c.platform.updateStreamingCard(ctx, c, cardContent)
}

// Failed returns true if the card has entered a failed state.
func (c *streamingCard) Failed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failed
}

func (c *streamingCard) flush(ctx context.Context) {
	c.mu.Lock()
	content := c.pendingContent
	c.pendingContent = ""
	c.mu.Unlock()

	if content == "" || content == c.lastSentContent {
		c.mu.Lock()
		c.inFlight = false
		c.mu.Unlock()
		return
	}

	cardContent := buildCardJSON(content)
	err := c.platform.updateStreamingCard(ctx, c, cardContent)

	c.mu.Lock()
	c.inFlight = false
	if err != nil {
		slog.Warn("infoflow: streaming card update failed", "error", err)
		c.failed = true
		c.state = "failed"
	} else {
		c.lastSentContent = content
		c.lastSentAt = time.Now()
	}
	c.mu.Unlock()
}

// ─── API calls ─────────────────────────────────────────────────────────────────

func (p *Platform) createStreamingCard(ctx context.Context, rctx replyContext, content string) (modifyToken, messageID string, err error) {
	timestamp := time.Now().UnixMilli()

	var receiverID string
	var receiverType string
	if rctx.isGroup {
		receiverID = fmt.Sprintf("%d", rctx.groupID)
		receiverType = "group"
	} else {
		receiverID = rctx.userID
		receiverType = "user"
	}

	payload := map[string]any{
		"contents":      content,
		"type":          1,
		"content_id":    fmt.Sprintf("%d", timestamp),
		"receiver_id":   receiverID,
		"receiver_type": receiverType,
		"scene":         "IM-server",
		"user_msg":      false,
		"meta": map[string]any{
			"client_msg_id":        timestamp,
			"client_send_time":     timestamp,
			"interactivity_expire": streamingExpireSeconds,
			"interactivity_mode":   "normal",
			"modify_expire":        streamingExpireSeconds,
			"offline_notify_txt":   "你收到一条卡片消息",
			"template": map[string]any{
				"name":    streamingTemplateName,
				"version": streamingTemplateVersion,
			},
		},
	}

	respBody, err := p.doPostWithResponse(ctx, streamingCreatePath, payload)
	if err != nil {
		return "", "", fmt.Errorf("create card request: %w", err)
	}

	// Parse response — extract modify_token and messageid
	var result struct {
		Code string `json:"code"`
		Data struct {
			Data struct {
				ModifyToken string `json:"modify_token"`
				MessageID   any    `json:"messageid"`
				Receivers   []struct {
					MsgID       any    `json:"msg_id"`
					ModifyToken string `json:"modify_token"`
				} `json:"receivers"`
			} `json:"data"`
			ModifyToken string `json:"modify_token"`
			MessageID   any    `json:"messageid"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", "", fmt.Errorf("create card decode: %w", err)
	}

	// Extract modify_token (multiple possible locations)
	modifyToken = result.Data.Data.ModifyToken
	if modifyToken == "" {
		modifyToken = result.Data.ModifyToken
	}
	if modifyToken == "" && len(result.Data.Data.Receivers) > 0 {
		modifyToken = result.Data.Data.Receivers[0].ModifyToken
	}
	if modifyToken == "" {
		return "", "", fmt.Errorf("create card: no modify_token in response: %s", string(respBody[:min(len(respBody), 500)]))
	}

	// Extract messageID
	messageID = fmt.Sprintf("%v", result.Data.Data.MessageID)
	if messageID == "" || messageID == "<nil>" {
		messageID = fmt.Sprintf("%v", result.Data.MessageID)
	}

	return modifyToken, messageID, nil
}

func (p *Platform) updateStreamingCard(ctx context.Context, card *streamingCard, content string) error {
	var payload map[string]any
	var path string

	if card.isGroup {
		card.mu.Lock()
		card.groupUpdateVersion++
		version := card.groupUpdateVersion
		card.mu.Unlock()

		payload = map[string]any{
			"modify_token":            card.modifyToken,
			"new_dynamic_msg_content": content,
			"version":                 version,
			"notify_list":             []map[string]any{{"to_type": 2, "to_ids": []int64{card.groupID}}},
		}
		path = streamingUpdateGroupPath
	} else {
		payload = map[string]any{
			"modify_token": card.modifyToken,
			"new_personal_msg_content": []map[string]any{{
				"user_ids":            []string{card.userID},
				"personal_msg_content": content,
				"notify_msg_content":  nil,
			}},
		}
		path = streamingUpdatePersonalPath
	}

	_, err := p.doPostWithResponse(ctx, path, payload)
	return err
}

// ─── Card content builder ──────────────────────────────────────────────────────

func buildCardJSON(markdownText string) string {
	// The streaming_render template expects a JSON with ai_markdown key
	card := map[string]any{
		"ai_markdown": markdownText,
	}
	b, _ := json.Marshal(card)
	return string(b)
}

// ─── Degradation ───────────────────────────────────────────────────────────────

func (p *Platform) isCardDegraded() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return time.Now().Before(p.cardDegradeUntil)
}

func (p *Platform) activateCardDegrade(reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cardDegradeUntil = time.Now().Add(30 * time.Minute)
	slog.Warn("infoflow: card API degraded for 30min", "reason", reason)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
