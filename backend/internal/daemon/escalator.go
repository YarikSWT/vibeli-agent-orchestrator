package daemon

import (
	"context"
	"log/slog"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// orchestratorEscalator hands a stalled worker to the project's orchestrator
// session — the agent on duty.
//
// It exists because the alternative is waking a human for every agent that
// stops mid-task, and most stalls are dull: a context compaction that lost the
// thread, a command waiting on input that never came. The orchestrator can look
// at the pane and decide; the chat message still goes out in parallel, so a
// missing or wedged orchestrator never swallows the signal.
type orchestratorEscalator struct {
	store    *sqlite.Store
	sessions *sessionsvc.Service
	logger   *slog.Logger
}

func newOrchestratorEscalator(store *sqlite.Store, sessions *sessionsvc.Service, logger *slog.Logger) *orchestratorEscalator {
	if store == nil || sessions == nil {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &orchestratorEscalator{store: store, sessions: sessions, logger: logger}
}

// Escalate sends the stall report to the project's live orchestrator. Doing
// nothing is a valid outcome: a project without an orchestrator simply relies on
// the chat notification.
func (e *orchestratorEscalator) Escalate(ctx context.Context, project domain.ProjectID, stalled domain.SessionID, text string) {
	if e == nil {
		return
	}
	target, ok := e.onDuty(ctx, project, stalled)
	if !ok {
		return
	}
	if err := e.sessions.Send(ctx, target, text, nil); err != nil {
		// Best effort by design: the operator was told in the same breath.
		e.logger.Warn("escalation to orchestrator failed", "orchestrator", target, "stalled", stalled, "err", err)
		return
	}
	e.logger.Info("stalled session escalated to orchestrator", "orchestrator", target, "stalled", stalled)
}

// onDuty picks the project's live orchestrator. With several, the most recently
// updated one wins: that is the session someone is actually working with.
func (e *orchestratorEscalator) onDuty(ctx context.Context, project domain.ProjectID, stalled domain.SessionID) (domain.SessionID, bool) {
	sessions, err := e.store.ListSessions(ctx, project)
	if err != nil {
		e.logger.Warn("could not resolve orchestrator for escalation", "project", project, "err", err)
		return "", false
	}
	var best domain.SessionRecord
	found := false
	for _, s := range sessions {
		if s.Kind != domain.KindOrchestrator || s.IsTerminated || s.ID == stalled {
			continue
		}
		if strings.TrimSpace(string(s.ID)) == "" {
			continue
		}
		if !found || s.UpdatedAt.After(best.UpdatedAt) {
			best = s
			found = true
		}
	}
	return best.ID, found
}
