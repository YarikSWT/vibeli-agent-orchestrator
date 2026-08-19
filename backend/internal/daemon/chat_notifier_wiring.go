package daemon

import (
	"context"
	"log/slog"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
	"github.com/aoagents/agent-orchestrator/backend/internal/notify"
	"github.com/aoagents/agent-orchestrator/backend/internal/notify/telegram"
	trackerintake "github.com/aoagents/agent-orchestrator/backend/internal/observe/trackerintake"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// chatNotifier bundles the opt-in chat side-channel: notifications out, control
// commands in, and the pause gate both share with intake.
//
// A disabled notifier (no token configured) is a live zero value rather than
// nil, so call sites stay branch-free: the publisher is nil, Announce is a
// no-op, and the gate still works for other pause sources.
type chatNotifier struct {
	client   *telegram.Client
	pub      *telegram.Publisher
	gate     *trackerintake.Gate
	conveyor telegram.Conveyor
	logger   *slog.Logger
}

// startChatNotifier builds the notifier from the environment and starts its
// delivery worker. Without AO_TELEGRAM_BOT_TOKEN it returns an inert notifier
// and nothing ever leaves the machine.
func startChatNotifier(ctx context.Context, logger *slog.Logger) *chatNotifier {
	notifier := &chatNotifier{gate: &trackerintake.Gate{}, logger: logger}
	client, ok := telegram.NewFromEnv()
	if !ok {
		return notifier
	}
	notifier.client = client
	notifier.pub = telegram.NewPublisher(client, logger)
	notifier.pub.Start(ctx)
	logger.Info("chat notifier enabled", "transport", "telegram")
	return notifier
}

// publisher returns the notify.Publisher to fan out to, or nil when disabled.
// A typed nil would satisfy the interface while claiming to be non-nil, so the
// nil case is returned explicitly.
func (c *chatNotifier) publisher() notify.Publisher {
	if c == nil || c.pub == nil {
		return nil
	}
	return c.pub
}

// Announce implements trackerintake.Announcer. Safe on a disabled notifier.
func (c *chatNotifier) Announce(text string) {
	if c == nil || c.pub == nil {
		return
	}
	c.pub.Announce(text)
}

// intakeGate hands intake its pause switch.
func (c *chatNotifier) intakeGate() *trackerintake.Gate {
	if c == nil {
		return nil
	}
	return c.gate
}

// attachConveyor hands the bot the backlog surface. Intake is built after the
// notifier (the notifier owns the pause gate intake needs), so the dependency
// arrives in a second step rather than through the constructor.
func (c *chatNotifier) attachConveyor(observer *trackerintake.Observer) {
	if c == nil || observer == nil {
		return
	}
	c.conveyor = intakeConveyor{observer: observer}
}

// intakeConveyor translates intake's types into the chat-facing ones.
type intakeConveyor struct {
	observer *trackerintake.Observer
}

func (i intakeConveyor) Queue(ctx context.Context) ([]telegram.QueueItem, error) {
	queued, err := i.observer.Queue(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]telegram.QueueItem, 0, len(queued))
	for _, item := range queued {
		items = append(items, telegram.QueueItem{Project: item.ProjectID, Issue: item.Issue, Title: item.Title})
	}
	return items, nil
}

func (i intakeConveyor) Claim(ctx context.Context, ref string) (telegram.ClaimResult, error) {
	result, err := i.observer.Claim(ctx, ref)
	if err != nil {
		return telegram.ClaimResult{}, err
	}
	return telegram.ClaimResult{SessionID: result.SessionID, Issue: result.Issue, Title: result.Title}, nil
}

// startBot launches the command loop. It runs after the store and session
// service exist, since /status and /kill need both. Returns nil when chat is
// not configured, which the daemon's shutdown path treats as "nothing to wait
// for".
func (c *chatNotifier) startBot(ctx context.Context, store *sqlite.Store, sessions *sessionsvc.Service, duty *orchestratorEscalator) <-chan struct{} {
	if c == nil || c.client == nil {
		return nil
	}
	// A nil escalator must stay a nil interface: wrapped in one, the bot would
	// believe it has somewhere to send questions.
	var desk telegram.Duty
	if duty != nil {
		desk = duty
	}
	bot := telegram.NewBot(c.client, store, chatSessionKiller{sessions: sessions}, c.gate, c.conveyor, desk, c.logger)
	return bot.Start(ctx)
}

// chatAnnounceAPI is the write half of the chat side-channel, mounted at
// POST /api/v1/announce and reached by `ao announce`.
//
// Agents run with the bot token in their environment and could call Telegram
// directly. They must not: routing through the daemon keeps message formation
// in one place and keeps the right to speak as the bot a daemon capability
// rather than an ambient one.
type chatAnnounceAPI struct {
	notifier *chatNotifier
}

func (a chatAnnounceAPI) Announce(_ context.Context, text string) error {
	if a.notifier == nil || a.notifier.pub == nil {
		return controllers.ErrChatUnavailable
	}
	a.notifier.pub.Announce(text)
	return nil
}

// chatSessionKiller adapts the session service to the bot's Killer surface.
type chatSessionKiller struct {
	sessions *sessionsvc.Service
}

func (k chatSessionKiller) Kill(ctx context.Context, id domain.SessionID) error {
	_, err := k.sessions.Kill(ctx, id)
	return err
}
