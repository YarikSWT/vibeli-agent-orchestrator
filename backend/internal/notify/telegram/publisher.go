package telegram

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// queueDepth bounds the pending-message buffer. Telegram is a best-effort
// side-channel: when it is unreachable and the buffer fills, messages are
// dropped with a log line rather than backing pressure into the notification
// write path, which runs inside lifecycle transitions.
const queueDepth = 128

// Publisher delivers notification events to Telegram. It satisfies the
// notify.Publisher contract and is meant to sit alongside the dashboard hub,
// never to replace it.
//
// Publish never blocks on the network: events go onto a buffered queue drained
// by one worker goroutine. A lifecycle transition must not wait on an external
// API, and ordering only matters per-chat, so a single worker is enough.
type Publisher struct {
	client *Client
	logger *slog.Logger

	queue chan outgoing
	once  sync.Once
	done  chan struct{}
}

// NewPublisher wraps a client. Start must be called before events arrive.
func NewPublisher(client *Client, logger *slog.Logger) *Publisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Publisher{
		client: client,
		logger: logger,
		queue:  make(chan outgoing, queueDepth),
		done:   make(chan struct{}),
	}
}

// Start launches the delivery worker and returns a channel closed once it has
// stopped, matching how the daemon waits on its other loops.
func (p *Publisher) Start(ctx context.Context) <-chan struct{} {
	p.once.Do(func() {
		go func() {
			defer close(p.done)
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-p.queue:
					p.deliver(ctx, msg)
				}
			}
		}()
	})
	return p.done
}

// Publish enqueues a notification. Only creations are sent: a resolution means
// the human already acted, and echoing it back would be noise on their phone.
func (p *Publisher) Publish(_ context.Context, event domain.NotificationEvent) error {
	if p == nil || event.Kind != domain.NotificationCreated {
		return nil
	}
	p.Announce(formatRecord(event.Record))
	return nil
}

// outgoing is one queued message. A non-zero replaces names a message the bot
// already sent, whose text this one overwrites.
type outgoing struct {
	text     string
	replaces int64
}

// Announce queues an arbitrary message — used for conveyor events that are not
// stored notifications, such as a card being claimed off the board.
//
// It returns no error on a full queue: the caller is a lifecycle path that must
// not fail because a chat is unreachable.
func (p *Publisher) Announce(text string) {
	p.enqueue(outgoing{text: text})
}

// Replace overwrites a message the bot sent earlier instead of adding another
// one. It is how an answer lands on the very message that promised it, so a
// question costs the chat one line rather than two.
func (p *Publisher) Replace(messageID int64, text string) {
	p.enqueue(outgoing{text: text, replaces: messageID})
}

func (p *Publisher) enqueue(msg outgoing) {
	if p == nil || strings.TrimSpace(msg.text) == "" {
		return
	}
	select {
	case p.queue <- msg:
	default:
		p.logger.Warn("telegram: queue full, dropping message", "text", firstLine(msg.text))
	}
}

// deliver writes one queued message. An edit that Telegram refuses (too old, or
// the message was deleted) falls back to a fresh message: an extra line in the
// chat is a smaller loss than a swallowed answer.
func (p *Publisher) deliver(ctx context.Context, msg outgoing) {
	if msg.replaces != 0 {
		err := p.client.Edit(ctx, msg.replaces, msg.text)
		if err == nil || ctx.Err() != nil {
			return
		}
		p.logger.Warn("telegram: edit failed, sending a new message", "message", msg.replaces, "err", err)
	}
	if err := p.client.Send(ctx, msg.text); err != nil && ctx.Err() == nil {
		p.logger.Warn("telegram: send failed", "err", err)
	}
}

// formatRecord renders a stored notification as a chat message: what happened,
// which session, and the link a human needs to act on it.
func formatRecord(rec domain.NotificationRecord) string {
	var b strings.Builder
	b.WriteString(icon(rec.Type))
	b.WriteString(" ")
	b.WriteString(rec.Title)
	if body := strings.TrimSpace(rec.Body); body != "" {
		b.WriteString("\n")
		b.WriteString(body)
	}
	b.WriteString(fmt.Sprintf("\n\nсессия: %s · проект: %s", rec.SessionID, rec.ProjectID))
	if url := strings.TrimSpace(rec.PRURL); url != "" {
		b.WriteString("\n")
		b.WriteString(url)
	}
	if rec.Type == domain.NotificationNeedsInput {
		b.WriteString(fmt.Sprintf("\n\n/kill %s — снять задачу", rec.SessionID))
	}
	return b.String()
}

func icon(t domain.NotificationType) string {
	switch t {
	case domain.NotificationNeedsInput:
		return "⏸"
	case domain.NotificationReadyToMerge:
		return "✅"
	case domain.NotificationPRMerged:
		return "🎉"
	case domain.NotificationPRClosedUnmerged:
		return "🚫"
	default:
		return "•"
	}
}

func firstLine(text string) string {
	if idx := strings.IndexByte(text, '\n'); idx >= 0 {
		return text[:idx]
	}
	return text
}
