package daemon

import (
	"context"
	"log/slog"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
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
	client *telegram.Client
	pub    *telegram.Publisher
	gate   *trackerintake.Gate
	logger *slog.Logger
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

// startBot launches the command loop. It runs after the store and session
// service exist, since /status and /kill need both. Returns nil when chat is
// not configured, which the daemon's shutdown path treats as "nothing to wait
// for".
func (c *chatNotifier) startBot(ctx context.Context, store *sqlite.Store, sessions *sessionsvc.Service) <-chan struct{} {
	if c == nil || c.client == nil {
		return nil
	}
	bot := telegram.NewBot(c.client, store, chatSessionKiller{sessions: sessions}, c.gate, c.logger)
	return bot.Start(ctx)
}

// chatSessionKiller adapts the session service to the bot's Killer surface.
type chatSessionKiller struct {
	sessions *sessionsvc.Service
}

func (k chatSessionKiller) Kill(ctx context.Context, id domain.SessionID) error {
	_, err := k.sessions.Kill(ctx, id)
	return err
}
