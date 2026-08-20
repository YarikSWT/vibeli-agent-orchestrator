package scm

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type fakeEscalator struct {
	targets  []domain.SessionID
	texts    []string
	declined bool
}

func (f *fakeEscalator) Escalate(_ context.Context, _ domain.ProjectID, stalled domain.SessionID, text string) bool {
	f.targets = append(f.targets, stalled)
	f.texts = append(f.texts, text)
	return !f.declined
}

func TestStall_IsHandedToTheOrchestratorOnDuty(t *testing.T) {
	now := time.Now().UTC()
	announcer := &fakeAnnouncer{}
	escalator := &fakeEscalator{}
	o := New(&fakeProvider{}, &fakeStore{}, &fakeLifecycle{}, Config{
		Logger: quietSlog(), CacheMax: 32, Announcer: announcer, Escalator: escalator,
		StallAfter: 30 * time.Minute,
	})

	o.reportStalledSessions(context.Background(), map[string]*subject{
		"k": stallSubject("vibeli-16", 90*time.Minute, now, domain.PullRequest{}, false),
	}, now)

	// The agent on duty takes it, and the chat stays quiet: a stall somebody is
	// already looking at is not news for a human.
	if len(escalator.targets) != 1 || escalator.targets[0] != "vibeli-16" {
		t.Fatalf("escalated = %v, want [vibeli-16]", escalator.targets)
	}
	if !strings.Contains(escalator.texts[0], "vibeli-16") {
		t.Errorf("escalation should name the session: %s", escalator.texts[0])
	}
	if len(announcer.texts) != 0 {
		t.Fatalf("a stall the agent on duty took must not reach the chat: %v", announcer.texts)
	}
}

// An orchestrator that is missing or wedged must not swallow the signal.
func TestStall_ReachesTheChatWhenNobodyTakesIt(t *testing.T) {
	now := time.Now().UTC()
	announcer := &fakeAnnouncer{}
	escalator := &fakeEscalator{declined: true}
	o := New(&fakeProvider{}, &fakeStore{}, &fakeLifecycle{}, Config{
		Logger: quietSlog(), CacheMax: 32, Announcer: announcer, Escalator: escalator,
		StallAfter: 30 * time.Minute,
	})

	o.reportStalledSessions(context.Background(), map[string]*subject{
		"k": stallSubject("vibeli-16", 90*time.Minute, now, domain.PullRequest{}, false),
	}, now)

	if len(announcer.texts) != 1 {
		t.Fatalf("an untaken stall must reach the chat: %v", announcer.texts)
	}
	if !strings.Contains(announcer.texts[0], "дежурного нет") {
		t.Errorf("the chat should say why it is being bothered: %s", announcer.texts[0])
	}
}

func TestStall_WithoutAnOrchestratorOnlyTheChatIsUsed(t *testing.T) {
	now := time.Now().UTC()
	announcer := &fakeAnnouncer{}
	o := stallObserver(announcer)

	o.reportStalledSessions(context.Background(), map[string]*subject{
		"k": stallSubject("vibeli-16", 90*time.Minute, now, domain.PullRequest{}, false),
	}, now)

	if len(announcer.texts) != 1 {
		t.Fatalf("a project with no orchestrator still reports to chat: %v", announcer.texts)
	}
}
