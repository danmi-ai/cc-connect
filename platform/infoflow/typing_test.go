package infoflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartTyping(t *testing.T) {
	var addCount, delCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/auth/app_access_token":
			json.NewEncoder(w).Encode(map[string]any{
				"code": "ok",
				"data": map[string]any{"app_access_token": "test-token", "expire": 7200},
			})
		case r.URL.Path == "/api/v1/im/message/emoji/add":
			addCount.Add(1)
			json.NewEncoder(w).Encode(map[string]any{
				"code": "ok",
				"data": map[string]any{"reaction_id": "react-001"},
			})
		case r.URL.Path == "/api/v1/im/message/emoji/del":
			delCount.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"code": "ok"})
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()

	p := &Platform{
		appKey:        "test-key",
		appSecret:     "test-secret",
		baseURL:       server.URL + "/api/v1",
		reactionEmoji: "d18",
		httpClient:    &http.Client{Timeout: 5 * time.Second},
	}

	ctx := context.Background()
	rctx := replyContext{groupID: 1001, userID: "user1", messageID: "msg-001", isGroup: true}

	stop := p.StartTyping(ctx, rctx)
	if addCount.Load() != 1 {
		t.Fatalf("expected 1 add emoji call, got %d", addCount.Load())
	}

	stop()
	time.Sleep(50 * time.Millisecond) // give goroutine time to complete
	if delCount.Load() != 1 {
		t.Fatalf("expected 1 del emoji call, got %d", delCount.Load())
	}
}

func TestStartTypingDisabled(t *testing.T) {
	p := &Platform{
		reactionEmoji: "", // disabled
	}

	ctx := context.Background()
	rctx := replyContext{groupID: 1001, userID: "user1", messageID: "msg-001", isGroup: true}

	stop := p.StartTyping(ctx, rctx)
	stop() // should not panic
}

func TestAddDoneReaction(t *testing.T) {
	var addCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/auth/app_access_token":
			json.NewEncoder(w).Encode(map[string]any{
				"code": "ok",
				"data": map[string]any{"app_access_token": "test-token", "expire": 7200},
			})
		case r.URL.Path == "/api/v1/im/message/emoji/add":
			addCount.Add(1)
			json.NewEncoder(w).Encode(map[string]any{
				"code": "ok",
				"data": map[string]any{"reaction_id": "react-002"},
			})
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()

	p := &Platform{
		appKey:    "test-key",
		appSecret: "test-secret",
		baseURL:   server.URL + "/api/v1",
		doneEmoji: "d01",
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	rctx := replyContext{groupID: 1001, userID: "user1", messageID: "msg-001", isGroup: true}
	p.AddDoneReaction(rctx)
	time.Sleep(50 * time.Millisecond)

	if addCount.Load() != 1 {
		t.Fatalf("expected 1 done emoji call, got %d", addCount.Load())
	}
}

func TestProgressStyle(t *testing.T) {
	p := &Platform{progressStyle: "card"}
	if p.ProgressStyle() != "card" {
		t.Fatalf("expected 'card', got %q", p.ProgressStyle())
	}
}
