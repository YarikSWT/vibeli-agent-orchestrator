package controllers

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apispec"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
)

// maxAnnounceLen bounds one chat message. Telegram rejects anything longer than
// 4096 characters, and the chat transport prefixes the sender, so the text
// itself is capped a little below that.
const maxAnnounceLen = 4000

// maxAnnounceSessionLen bounds the sender label.
const maxAnnounceSessionLen = 128

// maxAnnounceBodyBytes bounds the request body. The text itself is capped at
// maxAnnounceLen runes; this only keeps a runaway client from streaming.
const maxAnnounceBodyBytes = 1 << 20

// ErrChatUnavailable reports that no chat transport is configured, so the
// message has nowhere to go. An announcer returns it rather than pretending to
// have delivered.
var ErrChatUnavailable = errors.New("chat notifier is not configured")

// ChatAnnouncer posts one message to the operator chat.
//
// It exists so an agent can speak to the humans watching the conveyor without
// holding the bot token: the daemon owns the transport and stays the single
// place chat messages are formed, the agent only supplies the words.
type ChatAnnouncer interface {
	// Announce posts text to the chat. session names the agent session it came
	// from, or is empty for the daemon's own voice; the transport uses it to
	// label the message and to overwrite the message that promised an answer.
	Announce(ctx context.Context, text, session string) error
}

// AnnounceController owns POST /announce — the write half of the chat
// side-channel, mirroring the read half the chat bot already serves.
type AnnounceController struct {
	// Chat is nil on a build without a chat transport; the route then answers
	// 501 instead of pretending to have sent anything.
	Chat ChatAnnouncer
}

// Register mounts the announce route on the supplied router.
func (c *AnnounceController) Register(r chi.Router) {
	r.Post("/announce", c.announce)
}

func (c *AnnounceController) announce(w http.ResponseWriter, r *http.Request) {
	if c.Chat == nil {
		apispec.NotImplemented(w, r, "POST", "/api/v1/announce")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxAnnounceBodyBytes)
	var in AnnounceRequest
	if err := decodeJSON(r, &in); err != nil {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_JSON", "Invalid JSON body", nil)
		return
	}
	text := domain.SanitizeControlChars(in.Text)
	if strings.TrimSpace(text) == "" {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "TEXT_REQUIRED", "Text is required", nil)
		return
	}
	if len([]rune(text)) > maxAnnounceLen {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "TEXT_TOO_LONG", "Text is too long", nil)
		return
	}
	session := strings.TrimSpace(domain.SanitizeControlChars(in.Session))
	if len([]rune(session)) > maxAnnounceSessionLen {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "SESSION_TOO_LONG", "Session is too long", nil)
		return
	}
	if err := c.Chat.Announce(r.Context(), text, session); err != nil {
		envelope.WriteAPIError(w, r, http.StatusServiceUnavailable, "unavailable", "CHAT_UNAVAILABLE", err.Error(), nil)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, AnnounceResponse{OK: true, Text: text})
}

// AnnounceRequest is the body of POST /api/v1/announce.
type AnnounceRequest struct {
	Text string `json:"text" minLength:"1" maxLength:"4000"`
	// Session is the agent session speaking. Omitted for the daemon's own
	// voice; `ao announce` fills it from AO_SESSION_ID.
	Session string `json:"session,omitempty" maxLength:"128"`
}

// AnnounceResponse is the body of POST /api/v1/announce. Text echoes what was
// queued after sanitisation, so a caller can see what the chat will show.
type AnnounceResponse struct {
	OK   bool   `json:"ok"`
	Text string `json:"text"`
}
