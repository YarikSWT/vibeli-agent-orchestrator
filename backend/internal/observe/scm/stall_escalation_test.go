package scm

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type fakeEscalator struct {
	targets []domain.SessionID
	texts   []string
}

func (f *fakeEscalator) Escalate(_ context.Context, _ domain.ProjectID, stalled domain.SessionID, text string) {
	f.targets = append(f.targets, stalled)
	f.texts = append(f.texts, text)
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

	// Both channels fire: the orchestrator can look at it right away, and the
	// operator still hears about it in case the orchestrator is itself wedged.
	if len(escalator.targets) != 1 || escalator.targets[0] != "vibeli-16" {
		t.Fatalf("escalated = %v, want [vibeli-16]", escalator.targets)
	}
	if !strings.Contains(escalator.texts[0], "vibeli-16") {
		t.Errorf("escalation should name the session: %s", escalator.texts[0])
	}
	if len(announcer.texts) != 1 {
		t.Fatalf("the operator must still be told: %v", announcer.texts)
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
