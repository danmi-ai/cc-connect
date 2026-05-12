package infoflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"
)

// doPost sends a POST request with automatic token management and retry.
func (p *Platform) doPost(ctx context.Context, path string, body any) error {
	_, err := p.doPostWithResponse(ctx, path, body)
	return err
}

// doPostWithResponse sends a POST and returns the raw response body.
func (p *Platform) doPostWithResponse(ctx context.Context, path string, body any) ([]byte, error) {
	var respBody []byte
	err := p.withTransientRetry(ctx, path, func() error {
		return p.withFreshTokenRetry(ctx, path, func(token string) error {
			data, err := json.Marshal(body)
			if err != nil {
				return fmt.Errorf("infoflow: marshal body for %s: %w", path, err)
			}
			slog.Debug("infoflow: POST request", "path", path, "body_len", len(data))
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+path, bytes.NewReader(data))
			if err != nil {
				return fmt.Errorf("infoflow: create request for %s: %w", path, err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer-"+token)
			req.Header.Set("X-LogId", fmt.Sprintf("%d", time.Now().UnixMilli()))

			resp, err := p.httpClient.Do(req)
			if err != nil {
				return fmt.Errorf("infoflow: POST %s failed: %w", path, err)
			}
			defer resp.Body.Close()

			b, _ := io.ReadAll(resp.Body)
			slog.Debug("infoflow: POST response", "path", path, "status", resp.StatusCode, "body_len", len(b))

			if resp.StatusCode == 503 || resp.StatusCode == 401 {
				return fmt.Errorf("infoflow: POST %s: 503/unauthorized status=%d", path, resp.StatusCode)
			}

			var result struct {
				Code    any    `json:"code"`
				Msg     string `json:"msg"`
				ErrCode int    `json:"errcode"`
				ErrMsg  string `json:"errmsg"`
			}
			if err := json.Unmarshal(b, &result); err != nil {
				return fmt.Errorf("infoflow: POST %s response decode failed: %w", path, err)
			}
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
			respBody = b
			return nil
		})
	})
	return respBody, err
}

// doPostRawToken sends a POST with an explicit token (no auto-refresh).
// Used when the caller already manages the token lifecycle.
func (p *Platform) doPostRawToken(ctx context.Context, token, path string, body any) ([]byte, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("infoflow: marshal body for %s: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("infoflow: create request for %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer-"+token)
	req.Header.Set("X-LogId", fmt.Sprintf("%d", time.Now().UnixMilli()))

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("infoflow: POST %s failed: %w", path, err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// withTransientRetry retries fn up to 3 times on transient network errors.
func (p *Platform) withTransientRetry(ctx context.Context, op string, fn func() error) error {
	const maxRetries = 3
	delay := 500 * time.Millisecond
	maxDelay := 5 * time.Second

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			jitter := time.Duration(float64(delay) * (1.0 + 0.25*rand.Float64()))
			if jitter > maxDelay {
				jitter = maxDelay
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(jitter):
			}
			delay *= 2
		}
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isTransientError(lastErr) {
			return lastErr
		}
		slog.Warn("infoflow: transient error, retrying", "op", op, "attempt", attempt+1, "error", lastErr)
	}
	return lastErr
}

func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "i/o timeout")
}

// Helpers for parsing number types from JSON.
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
