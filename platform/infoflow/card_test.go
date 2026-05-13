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

func TestStreamingCardLifecycle(t *testing.T) {
	var createCount, updateCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/auth/app_access_token":
			json.NewEncoder(w).Encode(map[string]any{
				"code": "ok",
				"data": map[string]any{"app_access_token": "test-token", "expire": 7200},
			})
		case r.URL.Path == "/api/v1/msg/sender/interactivity_msg":
			createCount.Add(1)
			json.NewEncoder(w).Encode(map[string]any{
				"code": "ok",
				"data": map[string]any{
					"receivers": []map[string]any{
						{"modify_token": "tok-abc123", "msg_id": "msg-123"},
					},
				},
			})
		case r.URL.Path == "/api/v1/msg/modifier/dynamic_content":
			updateCount.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"code": "ok"})
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()

	p := &Platform{
		appKey:         "test-key",
		appSecret:      "test-secret",
		baseURL:        server.URL + "/api/v1",
		cardThrottleMs: 100,
		httpClient:     &http.Client{Timeout: 5 * time.Second},
	}

	ctx := context.Background()
	rctx := replyContext{groupID: 1001, userID: "user1", isGroup: true}

	card, err := p.CreateStreamingCard(ctx, rctx)
	if err != nil {
		t.Fatalf("CreateStreamingCard: %v", err)
	}
	if createCount.Load() != 1 {
		t.Fatalf("expected 1 create call, got %d", createCount.Load())
	}

	// Rapid updates should be throttled
	for i := 0; i < 10; i++ {
		card.Update(ctx, "content-"+string(rune('0'+i)))
	}
	time.Sleep(200 * time.Millisecond)

	// Should have far fewer than 10 API calls due to throttling
	updates := updateCount.Load()
	if updates >= 10 {
		t.Fatalf("throttle failed: expected <10 update calls, got %d", updates)
	}
	if updates == 0 {
		t.Fatalf("no updates were sent")
	}

	// Finalize
	err = card.Finalize(ctx, "final content")
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if card.Failed() {
		t.Fatal("card should not be in failed state")
	}
}

func TestStreamingCardDegradation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/auth/app_access_token":
			json.NewEncoder(w).Encode(map[string]any{
				"code": "ok",
				"data": map[string]any{"app_access_token": "test-token", "expire": 7200},
			})
		case r.URL.Path == "/api/v1/msg/sender/interactivity_msg":
			w.WriteHeader(503)
			w.Write([]byte(`{"code":"error","msg":"service unavailable"}`))
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()

	p := &Platform{
		appKey:         "test-key",
		appSecret:      "test-secret",
		baseURL:        server.URL + "/api/v1",
		cardThrottleMs: 100,
		httpClient:     &http.Client{Timeout: 5 * time.Second},
	}

	ctx := context.Background()
	rctx := replyContext{groupID: 1001, userID: "user1", isGroup: true}

	_, err := p.CreateStreamingCard(ctx, rctx)
	if err == nil {
		t.Fatal("expected error from 503 response")
	}

	// Should now be degraded
	if !p.isCardDegraded() {
		t.Fatal("card should be degraded after 503")
	}

	// Second attempt should fail immediately due to degradation
	_, err = p.CreateStreamingCard(ctx, rctx)
	if err == nil {
		t.Fatal("expected degradation error")
	}
}

func TestStreamingCardFinalizeSendsLastContent(t *testing.T) {
	var lastContent string
	var updateCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/auth/app_access_token":
			json.NewEncoder(w).Encode(map[string]any{
				"code": "ok",
				"data": map[string]any{"app_access_token": "test-token", "expire": 7200},
			})
		case r.URL.Path == "/api/v1/msg/sender/interactivity_msg":
			json.NewEncoder(w).Encode(map[string]any{
				"code": "ok",
				"data": map[string]any{
					"receivers": []map[string]any{
						{"modify_token": "tok-abc456", "msg_id": "msg-456"},
					},
				},
			})
		case r.URL.Path == "/api/v1/msg/modifier/dynamic_content":
			updateCalls.Add(1)
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if dmc, ok := body["new_dynamic_msg_content"].(map[string]any); ok {
				if md, ok := dmc["ai_markdown"].(map[string]any); ok {
					lastContent, _ = md["content"].(string)
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"code": "ok"})
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()

	p := &Platform{
		appKey:         "test-key",
		appSecret:      "test-secret",
		baseURL:        server.URL + "/api/v1",
		cardThrottleMs: 50,
		httpClient:     &http.Client{Timeout: 5 * time.Second},
	}

	ctx := context.Background()
	rctx := replyContext{groupID: 1001, userID: "user1", isGroup: true}

	card, err := p.CreateStreamingCard(ctx, rctx)
	if err != nil {
		t.Fatalf("CreateStreamingCard: %v", err)
	}

	err = card.Finalize(ctx, "## Final Answer\nHello world!")
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	if lastContent != "## Final Answer\nHello world!" {
		t.Fatalf("expected final content, got: %q", lastContent)
	}
}

func TestAtDedup(t *testing.T) {
	p := &Platform{}

	sessionKey := "group:123:user:alice"
	p.resetReplyCounter(sessionKey)

	if !p.isFirstReply(sessionKey) {
		t.Fatal("first reply should be true")
	}
	if p.isFirstReply(sessionKey) {
		t.Fatal("second reply should be false")
	}
	if p.isFirstReply(sessionKey) {
		t.Fatal("third reply should be false")
	}

	// Reset and first should be true again
	p.resetReplyCounter(sessionKey)
	if !p.isFirstReply(sessionKey) {
		t.Fatal("after reset, first reply should be true")
	}
}
