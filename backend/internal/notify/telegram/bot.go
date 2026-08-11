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

// QueueItem is one card waiting to be claimed.
type QueueItem struct {
	Project string
	Issue   string
	Title   string
}

// ClaimResult reports the session a manual claim started.
type ClaimResult struct {
	SessionID string
	Issue     string
	Title     string
}

// Conveyor exposes the backlog to the chat: what is queued, and "start this one
// now". It is an interface so the bot stays free of intake internals.
type Conveyor interface {
	Queue(ctx context.Context) ([]QueueItem, error)
	Claim(ctx context.Context, ref string) (ClaimResult, error)
}

// Bot answers control commands from the configured chat: what is running, what
// is queued, start this card, pause claiming, drop a session. Anything that
// writes code or opens a PR stays in the agent's hands.
type Bot struct {
	client   *Client
	sessions SessionLister
	killer   Killer
	gate     Gate
	conveyor Conveyor
	logger   *slog.Logger
}

// NewBot wires a command bot. Any dependency may be nil; the matching command
// then reports that it is unavailable instead of panicking.
func NewBot(client *Client, sessions SessionLister, killer Killer, gate Gate, conveyor Conveyor, logger *slog.Logger) *Bot {
	if logger == nil {
		logger = slog.Default()
	}
	return &Bot{client: client, sessions: sessions, killer: killer, gate: gate, conveyor: conveyor, logger: logger}
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
	case "/queue":
		reply = b.queue(ctx)
	case "/take":
		reply = b.take(ctx, arg)
	case "/help", "/start":
		reply = strings.Join([]string{
			"/status — сессии и состояние очереди",
			"/queue — что лежит в Ready, по порядку",
			"/take <номер> — взять конкретную задачу сейчас",
			"/pause — не брать новые карточки",
			"/resume — снова брать",
			"/kill <id> — снять сессию",
		}, "\n")
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

// queue shows the backlog in claim order, so /take can name a card without
// opening the board in a browser.
func (b *Bot) queue(ctx context.Context) string {
	if b.conveyor == nil {
		return "очередь недоступна"
	}
	items, err := b.conveyor.Queue(ctx)
	if err != nil {
		return "не смог прочитать очередь: " + err.Error()
	}
	if len(items) == 0 {
		return "очередь пуста — в Ready ничего нет"
	}
	var out strings.Builder
	out.WriteString("в очереди (в порядке, в котором будут взяты):\n")
	for i, item := range items {
		out.WriteString(fmt.Sprintf("%d. %s — %s\n", i+1, item.Issue, truncate(item.Title, 60)))
	}
	out.WriteString("\n/take <номер issue> — взять сейчас")
	return strings.TrimRight(out.String(), "\n")
}

// take claims one specific card immediately, ahead of the queue.
func (b *Bot) take(ctx context.Context, arg string) string {
	ref := strings.TrimSpace(arg)
	if ref == "" {
		return "нужен номер issue: /take 53"
	}
	if b.conveyor == nil {
		return "запуск задач недоступен"
	}
	result, err := b.conveyor.Claim(ctx, ref)
	if err != nil {
		return "не смог взять " + ref + ": " + err.Error()
	}
	return fmt.Sprintf("🤖 взял %s — %s\n\nсессия: %s", result.Issue, truncate(result.Title, 60), result.SessionID)
}

func truncate(text string, limit int) string {
	runes := []rune(strings.TrimSpace(text))
	if len(runes) <= limit {
		return string(runes)
	}
	return string(runes[:limit]) + "…"
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
