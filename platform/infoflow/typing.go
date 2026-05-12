package infoflow

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/chenhg5/cc-connect/core"
)

// Compile-time interface checks for typing indicator.
var (
	_ core.TypingIndicator     = (*Platform)(nil)
	_ core.TypingIndicatorDone = (*Platform)(nil)
	_ core.ProgressStyleProvider = (*Platform)(nil)
)

// StartTyping adds an emoji reaction to the user's message to signal processing.
// Returns a stop function that removes the reaction.
func (p *Platform) StartTyping(ctx context.Context, rctxAny any) (stop func()) {
	if p.reactionEmoji == "" {
		return func() {}
	}
	rctx, ok := rctxAny.(replyContext)
	if !ok || rctx.messageID == "" {
		return func() {}
	}

	reactionID := p.addEmojiReaction(ctx, rctx.messageID, p.reactionEmoji)
	return func() {
		if reactionID != "" {
			go p.removeEmojiReaction(context.Background(), rctx.messageID, reactionID)
		}
	}
}

// AddDoneReaction adds a completion emoji to the original message.
func (p *Platform) AddDoneReaction(rctxAny any) {
	if p.doneEmoji == "" {
		return
	}
	rctx, ok := rctxAny.(replyContext)
	if !ok || rctx.messageID == "" {
		return
	}
	go p.addEmojiReaction(context.Background(), rctx.messageID, p.doneEmoji)
}

// ProgressStyle returns the configured progress rendering style.
func (p *Platform) ProgressStyle() string {
	return p.progressStyle
}

// ─── Emoji API helpers ─────────────────────────────────────────────────────────

func (p *Platform) addEmojiReaction(ctx context.Context, messageID, emojiCode string) string {
	payload := map[string]any{
		"message_id": messageID,
		"emoji_code": emojiCode,
	}
	respBody, err := p.doPostWithResponse(ctx, "/im/message/emoji/add", payload)
	if err != nil {
		slog.Debug("infoflow: add emoji failed", "error", err, "msgID", messageID)
		return ""
	}

	var result struct {
		Data struct {
			ReactionID string `json:"reaction_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return ""
	}
	return result.Data.ReactionID
}

func (p *Platform) removeEmojiReaction(ctx context.Context, messageID, reactionID string) {
	payload := map[string]any{
		"message_id":  messageID,
		"reaction_id": reactionID,
	}
	if err := p.doPost(ctx, "/im/message/emoji/del", payload); err != nil {
		slog.Debug("infoflow: remove emoji failed", "error", err, "msgID", messageID)
	}
}
