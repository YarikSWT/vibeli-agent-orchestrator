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

	queue chan string
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
		queue:  make(chan string, queueDepth),
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
				case text := <-p.queue:
					if err := p.client.Send(ctx, text); err != nil && ctx.Err() == nil {
						p.logger.Warn("telegram: send failed", "err", err)
					}
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

// Announce queues an arbitrary message — used for conveyor events that are not
// stored notifications, such as a card being claimed off the board.
//
// It returns no error on a full queue: the caller is a lifecycle path that must
// not fail because a chat is unreachable.
func (p *Publisher) Announce(text string) {
	if p == nil || strings.TrimSpace(text) == "" {
		return
	}
	select {
	case p.queue <- text:
	default:
		p.logger.Warn("telegram: queue full, dropping message", "text", firstLine(text))
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
