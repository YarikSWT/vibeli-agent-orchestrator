// Package telegram delivers AO notifications to a Telegram chat and accepts a
// small set of control commands back.
//
// It exists because the operator is not sitting in front of the dashboard: the
// daemon runs on a remote box, and the two events worth interrupting a human
// for — a session blocked on a permission prompt, and a PR waiting for a merge
// decision — need to reach a phone.
//
// The whole package is inert without AO_TELEGRAM_BOT_TOKEN, so a stock install
// never talks to Telegram.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	// EnvBotToken and EnvChatID are the only required configuration. Both must
	// be set for the notifier to come up.
	EnvBotToken = "AO_TELEGRAM_BOT_TOKEN"
	EnvChatID   = "AO_TELEGRAM_CHAT_ID"
	// EnvAPIBase overrides the Telegram API root; tests point it at httptest.
	EnvAPIBase = "AO_TELEGRAM_API_BASE"

	defaultAPIBase = "https://api.telegram.org"
	// requestTimeout bounds a single sendMessage. Long polling sets its own.
	requestTimeout = 15 * time.Second
)

// Client is a minimal Telegram Bot API client: send a message, read updates.
type Client struct {
	http    *http.Client
	apiBase string
	token   string
	chatID  string
}

// Config configures a Client. Empty fields fall back to the environment.
type Config struct {
	Token      string
	ChatID     string
	APIBase    string
	HTTPClient *http.Client
}

// NewFromEnv returns a client, or ok=false when the deployment did not
// configure Telegram. A missing token is the normal case, not an error.
func NewFromEnv() (*Client, bool) {
	token := strings.TrimSpace(os.Getenv(EnvBotToken))
	chatID := strings.TrimSpace(os.Getenv(EnvChatID))
	if token == "" || chatID == "" {
		return nil, false
	}
	return New(Config{Token: token, ChatID: chatID, APIBase: os.Getenv(EnvAPIBase)}), true
}

// New builds a client from an explicit config.
func New(cfg Config) *Client {
	c := &Client{
		http:    cfg.HTTPClient,
		apiBase: strings.TrimRight(strings.TrimSpace(cfg.APIBase), "/"),
		token:   strings.TrimSpace(cfg.Token),
		chatID:  strings.TrimSpace(cfg.ChatID),
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: requestTimeout}
	}
	if c.apiBase == "" {
		c.apiBase = defaultAPIBase
	}
	return c
}

// ChatID reports the chat this client talks to. Updates from any other chat are
// ignored, so a leaked bot username cannot drive the conveyor.
func (c *Client) ChatID() string { return c.chatID }

// Send posts one message. Text is sent as-is (no parse mode), so issue titles
// and agent output cannot break formatting or be interpreted as markup.
func (c *Client) Send(ctx context.Context, text string) error {
	payload := map[string]any{
		"chat_id":                  c.chatID,
		"text":                     text,
		"disable_web_page_preview": true,
	}
	var resp struct {
		OK          bool   `json:"ok"`
		Description string `json:"description"`
	}
	if err := c.call(ctx, "sendMessage", payload, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("telegram: sendMessage rejected: %s", resp.Description)
	}
	return nil
}

// Update is the subset of a Telegram update AO acts on: a text message and the
// chat it came from.
type Update struct {
	ID     int64
	ChatID string
	Text   string
}

// GetUpdates long-polls for messages after offset. timeout is the server-side
// long-poll window; the HTTP deadline is that plus a margin, so a healthy poll
// never trips the client timeout.
func (c *Client) GetUpdates(ctx context.Context, offset int64, timeout time.Duration) ([]Update, error) {
	seconds := int(timeout.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	payload := map[string]any{
		"timeout":         seconds,
		"allowed_updates": []string{"message"},
	}
	if offset > 0 {
		payload["offset"] = offset
	}
	var resp struct {
		OK     bool `json:"ok"`
		Result []struct {
			UpdateID int64 `json:"update_id"`
			Message  *struct {
				Text string `json:"text"`
				Chat struct {
					ID json.Number `json:"id"`
				} `json:"chat"`
			} `json:"message"`
		} `json:"result"`
		Description string `json:"description"`
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout+requestTimeout)
	defer cancel()
	if err := c.callWithClient(callCtx, &http.Client{Timeout: timeout + requestTimeout}, "getUpdates", payload, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("telegram: getUpdates rejected: %s", resp.Description)
	}
	updates := make([]Update, 0, len(resp.Result))
	for _, item := range resp.Result {
		if item.Message == nil {
			// Still surface the id: the offset must advance past updates AO
			// does not act on, or the same batch is re-read forever.
			updates = append(updates, Update{ID: item.UpdateID})
			continue
		}
		updates = append(updates, Update{
			ID:     item.UpdateID,
			ChatID: item.Message.Chat.ID.String(),
			Text:   strings.TrimSpace(item.Message.Text),
		})
	}
	return updates, nil
}

func (c *Client) call(ctx context.Context, method string, payload any, out any) error {
	return c.callWithClient(ctx, c.http, method, payload, out)
}

func (c *Client) callWithClient(ctx context.Context, client *http.Client, method string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoint := c.apiBase + "/bot" + url.PathEscape(c.token) + "/" + method
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		// The token is in the URL, never in the message: errors are logged.
		return fmt.Errorf("telegram: %s http %d: %s", method, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}
