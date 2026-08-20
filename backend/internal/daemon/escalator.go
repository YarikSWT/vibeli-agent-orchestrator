package daemon

import (
	"context"
	"log/slog"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/notify/telegram"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// orchestratorEscalator hands a stalled worker to the project's orchestrator
// session — the agent on duty.
//
// It exists because the alternative is waking a human for every agent that
// stops mid-task, and most stalls are dull: a context compaction that lost the
// thread, a command waiting on input that never came. The orchestrator can look
// at the pane and decide, and the human hears about the stall only when nobody
// on duty took it — so the signal is never swallowed, but it also stops being
// noise.
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

// Escalate sends the stall report to the project's live orchestrator and
// reports whether it landed. A false answer — no orchestrator, or one that
// would not take the message — is what puts the stall in front of a human
// instead.
func (e *orchestratorEscalator) Escalate(ctx context.Context, project domain.ProjectID, stalled domain.SessionID, text string) bool {
	if e == nil {
		return false
	}
	target, ok := e.onDuty(ctx, project, stalled)
	if !ok {
		return false
	}
	if err := e.sessions.Send(ctx, target, text, nil); err != nil {
		e.logger.Warn("escalation to orchestrator failed", "orchestrator", target, "stalled", stalled, "err", err)
		return false
	}
	e.logger.Info("stalled session escalated to orchestrator", "orchestrator", target, "stalled", stalled)
	return true
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

// Ask delivers a human's chat message to the agent on duty and reports
// which session took it.
//
// It is the inbound half of the chat side-channel: the outbound half already
// tells the chat what the conveyor is doing, and a human who reads that has
// follow-up questions. Unlike Escalate, this one cannot fail quietly — the
// person is waiting for an answer — so it returns ErrNoDutyAgent instead of
// doing nothing.
//
// The question is not project-scoped (a chat message names no board), so the
// whole fleet is considered rather than one project's sessions.
func (e *orchestratorEscalator) Ask(ctx context.Context, text string) (string, error) {
	if e == nil {
		return "", telegram.ErrNoDutyAgent
	}
	sessions, err := e.store.ListAllSessions(ctx)
	if err != nil {
		return "", err
	}
	target, ok := mostRecentOrchestrator(sessions)
	if !ok {
		return "", telegram.ErrNoDutyAgent
	}
	if err := e.sessions.Send(ctx, target, dutyQuestion(text), nil); err != nil {
		return "", err
	}
	e.logger.Info("question from chat handed to orchestrator", "orchestrator", target)
	return string(target), nil
}

// mostRecentOrchestrator picks the live orchestrator someone is actually
// working with, on the same "most recently updated wins" rule onDuty uses.
func mostRecentOrchestrator(sessions []domain.SessionRecord) (domain.SessionID, bool) {
	var best domain.SessionRecord
	found := false
	for _, s := range sessions {
		if s.Kind != domain.KindOrchestrator || s.IsTerminated {
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

// dutyQuestion frames the message for the agent. Without the frame, a bare line
// of Russian arriving in a pane reads like a task brief; the agent needs to know
// a human is waiting in the chat and how to answer them.
func dutyQuestion(text string) string {
	return "[вопрос от человека из чата конвейера]\n" + text +
		"\n\nЭто не задача на код, а вопрос дежурному. Ответь коротко в тот же чат: " +
		"ao announce --message \"...\" — одним сообщением: первый announce перезапишет " +
		"в чате строку «спросил дежурного», следующие лягут отдельными сообщениями."
}
