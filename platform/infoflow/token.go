package infoflow

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

func md5hex(s string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(s)))
}

func (p *Platform) fetchToken(ctx context.Context) (string, int, error) {
	body, _ := json.Marshal(map[string]string{
		"app_key":    p.appKey,
		"app_secret": md5hex(p.appSecret),
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/auth/app_access_token", bytes.NewReader(body))
	if err != nil {
		return "", 0, fmt.Errorf("infoflow: create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("infoflow: token request failed: %w", err)
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
		return "", 0, fmt.Errorf("infoflow: token decode failed: %w", err)
	}
	if result.Code != "ok" {
		return "", 0, fmt.Errorf("infoflow: token API returned code=%s", result.Code)
	}
	return result.Data.AppAccessToken, result.Data.Expire, nil
}

func (p *Platform) getToken(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.accessToken != "" && time.Now().Before(p.tokenExpiry) {
		return p.accessToken, nil
	}
	token, expireSec, err := p.fetchToken(ctx)
	if err != nil {
		return "", err
	}
	p.accessToken = token
	expiry := time.Duration(expireSec) * time.Second
	if expiry <= 0 {
		expiry = 110 * time.Minute
	}
	p.tokenExpiry = time.Now().Add(expiry - 5*time.Minute)
	return token, nil
}

func (p *Platform) invalidateToken() {
	p.mu.Lock()
	p.accessToken = ""
	p.tokenExpiry = time.Time{}
	p.mu.Unlock()
}

// withFreshTokenRetry executes fn with a token; if the token is expired,
// invalidates and retries once with a fresh token.
func (p *Platform) withFreshTokenRetry(ctx context.Context, op string, fn func(token string) error) error {
	token, err := p.getToken(ctx)
	if err != nil {
		return fmt.Errorf("infoflow: %s: get token: %w", op, err)
	}
	err = fn(token)
	if err == nil || !isTokenExpiredError(err) {
		return err
	}
	p.invalidateToken()
	token, err = p.getToken(ctx)
	if err != nil {
		return fmt.Errorf("infoflow: %s: refresh token: %w", op, err)
	}
	slog.Warn("infoflow: retrying with fresh token", "operation", op)
	return fn(token)
}

func isTokenExpiredError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "503") ||
		strings.Contains(msg, "token expired") ||
		strings.Contains(msg, "invalid token") ||
		strings.Contains(msg, "unauthorized")
}

// fetchRobotImID queries the robot profile to determine its IM ID.
func (p *Platform) fetchRobotImID(ctx context.Context) error {
	if p.robotImID != 0 {
		return nil
	}
	token, err := p.getToken(ctx)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]any{})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/imRobot/detail", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("infoflow: create robot detail request: %w", err)
	}
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
