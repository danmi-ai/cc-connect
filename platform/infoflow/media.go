package infoflow

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// Compile-time interface checks for media and recall.
var (
	_ core.ImageSender          = (*Platform)(nil)
	_ core.FileSender           = (*Platform)(nil)
	_ core.MessageRecallDetector = (*Platform)(nil)
)

// SendImage sends an image to the conversation.
func (p *Platform) SendImage(ctx context.Context, replyCtxAny any, img core.ImageAttachment) error {
	rctx, ok := replyCtxAny.(replyContext)
	if !ok {
		return fmt.Errorf("infoflow: unexpected reply context type %T", replyCtxAny)
	}

	b64 := base64.StdEncoding.EncodeToString(img.Data)
	dataURI := fmt.Sprintf("data:%s;base64,%s", img.MimeType, b64)

	content := fmt.Sprintf("![%s](%s)", img.FileName, dataURI)

	if rctx.isGroup {
		return p.sendToGroup(ctx, rctx, content)
	}
	return p.sendToDM(ctx, rctx.userID, content)
}

// SendFile sends a file to the conversation as a markdown link with metadata.
func (p *Platform) SendFile(ctx context.Context, replyCtxAny any, file core.FileAttachment) error {
	rctx, ok := replyCtxAny.(replyContext)
	if !ok {
		return fmt.Errorf("infoflow: unexpected reply context type %T", replyCtxAny)
	}

	sizeKB := len(file.Data) / 1024
	content := fmt.Sprintf("📎 **%s** (%s, %dKB)\n\n_File sent via cc-connect. Use `cc-connect send --file` to download._",
		file.FileName, file.MimeType, sizeKB)

	if rctx.isGroup {
		return p.sendToGroup(ctx, rctx, content)
	}
	return p.sendToDM(ctx, rctx.userID, content)
}

// ─── Message Recall Detection ─────────────────────────────────────────────────

const recallTTL = 10 * time.Minute

// recalledMessages tracks message IDs that have been recalled.
func (p *Platform) initRecallTracker() {
	p.recalledMsgsOnce.Do(func() {
		p.recalledMsgs = &sync.Map{}
	})
}

// markRecalled stores a message ID as recalled with a TTL.
func (p *Platform) markRecalled(msgID string) {
	if msgID == "" {
		return
	}
	p.initRecallTracker()
	p.recalledMsgs.Store(msgID, time.Now())
	slog.Debug("infoflow: message recalled", "msgID", msgID)
}

// IsMessageRecalled checks if the message targeted by a reply context was recalled.
func (p *Platform) IsMessageRecalled(ctx context.Context, replyCtxAny any) (bool, error) {
	rctx, ok := replyCtxAny.(replyContext)
	if !ok {
		return false, nil
	}
	if rctx.messageID == "" {
		return false, nil
	}
	p.initRecallTracker()
	_, exists := p.recalledMsgs.Load(rctx.messageID)
	return exists, nil
}

// recallCleanupLoop periodically purges expired recall entries.
func (p *Platform) recallCleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			p.recalledMsgs.Range(func(key, value any) bool {
				if ts, ok := value.(time.Time); ok && now.Sub(ts) > recallTTL {
					p.recalledMsgs.Delete(key)
				}
				return true
			})
		}
	}
}

// onRecallEvent handles a message recall event from WebSocket.
func (p *Platform) onRecallEvent(raw map[string]any) {
	msgID, _ := raw["messageid"].(string)
	if msgID == "" {
		msgID, _ = raw["MsgId"].(string)
	}
	if msgID == "" {
		return
	}
	p.markRecalled(msgID)

	// Also dispatch to engine as a recalled message
	fromUserID, _ := raw["FromUserId"].(string)
	if fromUserID == "" {
		if header, ok := raw["header"].(map[string]any); ok {
			fromUserID, _ = header["fromuserid"].(string)
		}
	}

	sessionKey := ""
	groupID := toInt64(raw["groupid"])
	if groupID != 0 {
		rctx := replyContext{groupID: groupID, userID: fromUserID, isGroup: true}
		sessionKey = p.sessionKey(rctx)
	} else if fromUserID != "" {
		sessionKey = fmt.Sprintf("dm:%s", fromUserID)
	}

	if sessionKey != "" && p.handler != nil {
		msg := &core.Message{
			Platform:   "infoflow",
			SessionKey: sessionKey,
			MessageID:  msgID,
			UserID:     fromUserID,
			Recalled:   true,
			ReplyCtx:   replyContext{messageID: msgID},
		}
		go p.handler(p, msg)
	}
}

// isRecallEvent checks if a raw WS payload is a recall event.
func isRecallEvent(raw map[string]any) bool {
	eventType, _ := raw["eventtype"].(string)
	return strings.ToUpper(eventType) == "MESSAGE_RECALL" ||
		strings.ToUpper(eventType) == "MSG_RECALL"
}
