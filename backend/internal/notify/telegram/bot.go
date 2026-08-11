package telegram

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

const (
	// pollTimeout is the server-side long-poll window. Long enough that an idle
	// conveyor makes ~1 request/minute, short enough that a shutdown is not
	// held up for long.
	pollTimeout = 50 * time.Second
	// errorBackoff throttles retries when Telegram is unreachable, so an outage
	// does not turn into a hot loop.
	errorBackoff = 30 * time.Second
)

// SessionLister is the read surface /status needs.
type SessionLister interface {
	ListAllSessions(ctx context.Context) ([]domain.SessionRecord, error)
}

// Killer terminates a session on /kill.
type Killer interface {
	Kill(ctx context.Context, id domain.SessionID) error
}

// Gate is the intake pause switch shared with the usage-limit backoff.
type Gate interface {
	Pause()
	Resume()
	Paused() bool
}

// Bot answers control commands from the configured chat. It is deliberately
// tiny: status, pause, resume, kill. Anything that changes code or opens a PR
// stays in the agent's hands.
type Bot struct {
	client   *Client
	sessions SessionLister
	killer   Killer
	gate     Gate
	logger   *slog.Logger
}

// NewBot wires a command bot. Any of sessions/killer/gate may be nil; the
// matching command then reports that it is unavailable instead of panicking.
func NewBot(client *Client, sessions SessionLister, killer Killer, gate Gate, logger *slog.Logger) *Bot {
	if logger == nil {
		logger = slog.Default()
	}
	return &Bot{client: client, sessions: sessions, killer: killer, gate: gate, logger: logger}
}

// Start runs the long-poll loop until ctx is done and returns a channel closed
// when it has stopped.
func (b *Bot) Start(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		var offset int64
		for {
			if ctx.Err() != nil {
				return
			}
			updates, err := b.client.GetUpdates(ctx, offset, pollTimeout)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				b.logger.Warn("telegram: poll failed", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(errorBackoff):
				}
				continue
			}
			for _, update := range updates {
				if update.ID >= offset {
					offset = update.ID + 1
				}
				b.handle(ctx, update)
			}
		}
	}()
	return done
}

// handle runs one command. Updates from any chat other than the configured one
// are dropped without a reply: the bot's username is discoverable, its chat is
// the authorization boundary.
func (b *Bot) handle(ctx context.Context, update Update) {
	if update.Text == "" {
		return
	}
	if update.ChatID != b.client.ChatID() {
		b.logger.Warn("telegram: ignoring command from unknown chat", "chat", update.ChatID)
		return
	}
	command, arg := splitCommand(update.Text)
	var reply string
	switch command {
	case "/status":
		reply = b.status(ctx)
	case "/pause":
		reply = b.pause()
	case "/resume":
		reply = b.resume()
	case "/kill":
		reply = b.kill(ctx, arg)
	case "/help", "/start":
		reply = "/status — сессии и состояние очереди\n/pause — не брать новые карточки\n/resume — снова брать\n/kill <id> — снять сессию"
	default:
		return
	}
	if err := b.client.Send(ctx, reply); err != nil {
		b.logger.Warn("telegram: reply failed", "command", command, "err", err)
	}
}

func (b *Bot) status(ctx context.Context) string {
	var out strings.Builder
	if b.gate != nil && b.gate.Paused() {
		out.WriteString("⏸ приём карточек на паузе\n\n")
	} else {
		out.WriteString("▶ приём карточек включён\n\n")
	}
	if b.sessions == nil {
		out.WriteString("список сессий недоступен")
		return out.String()
	}
	sessions, err := b.sessions.ListAllSessions(ctx)
	if err != nil {
		return out.String() + "не смог прочитать сессии: " + err.Error()
	}
	var live int
	for _, session := range sessions {
		if session.IsTerminated {
			continue
		}
		live++
		out.WriteString(fmt.Sprintf("• %s [%s] %s", session.ID, session.Activity.State, session.IssueID))
		out.WriteString("\n")
	}
	if live == 0 {
		out.WriteString("живых сессий нет")
	}
	return strings.TrimRight(out.String(), "\n")
}

func (b *Bot) pause() string {
	if b.gate == nil {
		return "пауза недоступна"
	}
	b.gate.Pause()
	return "⏸ новые карточки не берём. Живые сессии продолжают работать."
}

func (b *Bot) resume() string {
	if b.gate == nil {
		return "пауза недоступна"
	}
	b.gate.Resume()
	return "▶ снова берём карточки из очереди."
}

func (b *Bot) kill(ctx context.Context, arg string) string {
	id := strings.TrimSpace(arg)
	if id == "" {
		return "нужен id сессии: /kill vibeli-3"
	}
	if b.killer == nil {
		return "kill недоступен"
	}
	if err := b.killer.Kill(ctx, domain.SessionID(id)); err != nil {
		return fmt.Sprintf("не смог снять %s: %v", id, err)
	}
	return fmt.Sprintf("сессия %s снята", id)
}

// splitCommand parses "/kill vibeli-3" and the "/kill@bot_name vibeli-3" form
// Telegram uses in groups.
func splitCommand(text string) (command, arg string) {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return "", ""
	}
	command = fields[0]
	if at := strings.IndexByte(command, '@'); at > 0 {
		command = command[:at]
	}
	return strings.ToLower(command), strings.Join(fields[1:], " ")
}
