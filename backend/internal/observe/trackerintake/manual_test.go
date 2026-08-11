package trackerintake

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func manualProject(id, origin string) domain.ProjectRecord {
	return domain.ProjectRecord{
		ID:            id,
		RepoOriginURL: origin,
		Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{
			Enabled:  true,
			Provider: domain.TrackerProviderGitHub,
			Assignee: "*",
		}},
	}
}

func manualTracker() *fakeTracker {
	return &fakeTracker{issues: []domain.Issue{
		{
			ID:        domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#53"},
			Title:     "падает загрузка картинки",
			State:     domain.IssueOpen,
			Assignees: []string{"octocat"},
		},
	}}
}

func TestQueueListsReadyCards(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{manualProject("proj", "https://github.com/acme/demo.git")}}
	observer := New(singleResolver(manualTracker()), store, &fakeSpawner{}, Config{Logger: discardLogger()})

	queued, err := observer.Queue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 1 || queued[0].Issue != "acme/demo#53" {
		t.Fatalf("queued = %#v, want the single ready card", queued)
	}
	if queued[0].Title != "падает загрузка картинки" {
		t.Fatalf("title = %q", queued[0].Title)
	}
}

func TestClaimByBareNumber(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{manualProject("proj", "https://github.com/acme/demo.git")}}
	spawner := &fakeSpawner{}
	observer := New(singleResolver(manualTracker()), store, spawner, Config{Logger: discardLogger()})

	result, err := observer.Claim(context.Background(), "53")
	if err != nil {
		t.Fatal(err)
	}
	if result.Issue != "acme/demo#53" {
		t.Fatalf("issue = %q, want acme/demo#53", result.Issue)
	}
	if len(spawner.calls) != 1 {
		t.Fatalf("spawned %d sessions, want 1", len(spawner.calls))
	}
	if spawner.calls[0].IssueID != "github:acme/demo#53" {
		t.Fatalf("session issue id = %q, want the canonical form", spawner.calls[0].IssueID)
	}
	if !strings.Contains(spawner.calls[0].Prompt, "падает загрузка картинки") {
		t.Fatalf("prompt must carry the issue context:\n%s", spawner.calls[0].Prompt)
	}
}

func TestClaimAcceptsFullReferenceAndHash(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{manualProject("proj", "https://github.com/acme/demo.git")}}
	for _, ref := range []string{"#53", "acme/demo#53", "ACME/Demo#53"} {
		spawner := &fakeSpawner{}
		observer := New(singleResolver(manualTracker()), store, spawner, Config{Logger: discardLogger()})
		if _, err := observer.Claim(context.Background(), ref); err != nil {
			t.Fatalf("Claim(%q): %v", ref, err)
		}
		if len(spawner.calls) != 1 {
			t.Fatalf("Claim(%q) spawned %d sessions", ref, len(spawner.calls))
		}
	}
}

func TestClaimIgnoresTheConcurrencyCapAndPause(t *testing.T) {
	project := manualProject("proj", "https://github.com/acme/demo.git")
	project.Config.TrackerIntake.MaxConcurrent = 1
	store := &fakeStore{
		projects: []domain.ProjectRecord{project},
		sessions: []domain.SessionRecord{{ID: "proj-1", ProjectID: "proj", IssueID: "github:acme/demo#1"}},
	}
	spawner := &fakeSpawner{}
	gate := &Gate{}
	gate.Pause()
	observer := New(singleResolver(manualTracker()), store, spawner, Config{Logger: discardLogger(), Gate: gate})

	// A human naming a card outranks both the sweep's cap and its pause switch.
	if _, err := observer.Claim(context.Background(), "53"); err != nil {
		t.Fatal(err)
	}
	if len(spawner.calls) != 1 {
		t.Fatalf("manual claim must not be blocked, spawned %d", len(spawner.calls))
	}
}

func TestClaimRefusesAnIssueAlreadyInFlight(t *testing.T) {
	store := &fakeStore{
		projects: []domain.ProjectRecord{manualProject("proj", "https://github.com/acme/demo.git")},
		// Bare spelling, as `ao spawn --issue` writes it.
		sessions: []domain.SessionRecord{{ID: "proj-7", ProjectID: "proj", IssueID: "acme/demo#53"}},
	}
	spawner := &fakeSpawner{}
	observer := New(singleResolver(manualTracker()), store, spawner, Config{Logger: discardLogger()})

	_, err := observer.Claim(context.Background(), "53")
	if err == nil {
		t.Fatal("claiming an issue that already has a live session must fail")
	}
	if !strings.Contains(err.Error(), "proj-7") {
		t.Fatalf("error should name the session holding it: %v", err)
	}
	if len(spawner.calls) != 0 {
		t.Fatalf("nothing should be spawned, got %d", len(spawner.calls))
	}
}

func TestClaimRejectsAmbiguousBareNumber(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{
		manualProject("one", "https://github.com/acme/demo.git"),
		manualProject("two", "https://github.com/acme/other.git"),
	}}
	observer := New(singleResolver(manualTracker()), store, &fakeSpawner{}, Config{Logger: discardLogger()})

	_, err := observer.Claim(context.Background(), "53")
	if err == nil || !strings.Contains(err.Error(), "owner/repo#53") {
		t.Fatalf("a bare number with several projects must ask for the full form, got %v", err)
	}
}

func TestClaimRejectsGarbage(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{manualProject("proj", "https://github.com/acme/demo.git")}}
	observer := New(singleResolver(manualTracker()), store, &fakeSpawner{}, Config{Logger: discardLogger()})

	for _, ref := range []string{"", "не-номер", "acme/demo"} {
		if _, err := observer.Claim(context.Background(), ref); err == nil {
			t.Fatalf("Claim(%q) must fail", ref)
		}
	}
}

func TestClaimRefusesWhenTheIssueDoesNotExist(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{manualProject("proj", "https://github.com/acme/demo.git")}}
	spawner := &fakeSpawner{}
	observer := New(singleResolver(manualTracker()), store, spawner, Config{Logger: discardLogger()})

	_, err := observer.Claim(context.Background(), "404")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err = %v, want a not-found error", err)
	}
	if len(spawner.calls) != 0 {
		t.Fatalf("an unknown issue must not start a session, spawned %d", len(spawner.calls))
	}
}

// --- concurrency slots ------------------------------------------------------

func capProject() domain.ProjectRecord {
	project := manualProject("proj", "https://github.com/acme/demo.git")
	project.Config.TrackerIntake.MaxConcurrent = 1
	return project
}

func busySession() domain.SessionRecord {
	return domain.SessionRecord{ID: "proj-1", ProjectID: "proj", IssueID: "github:acme/demo#1"}
}

func TestOpenPRFreesTheConcurrencySlot(t *testing.T) {
	store := &fakeStore{
		projects: []domain.ProjectRecord{capProject()},
		sessions: []domain.SessionRecord{busySession()},
		// The session opened its PR and is now waiting on a human reviewer.
		prs: map[domain.SessionID][]domain.PullRequest{
			"proj-1": {{URL: "https://github.com/acme/demo/pull/9", Number: 9}},
		},
	}
	spawner := &fakeSpawner{}
	observer := New(singleResolver(manualTracker()), store, spawner, Config{Logger: discardLogger()})

	if err := observer.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(spawner.calls) != 1 {
		t.Fatalf("spawned %d sessions, want 1: a PR waiting on review must not hold the slot", len(spawner.calls))
	}
}

func TestSessionWithoutPRStillHoldsTheSlot(t *testing.T) {
	store := &fakeStore{
		projects: []domain.ProjectRecord{capProject()},
		sessions: []domain.SessionRecord{busySession()},
	}
	spawner := &fakeSpawner{}
	observer := New(singleResolver(manualTracker()), store, spawner, Config{Logger: discardLogger()})

	if err := observer.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(spawner.calls) != 0 {
		t.Fatalf("spawned %d sessions, want 0: an agent still working occupies the cap", len(spawner.calls))
	}
}

func TestMergedPRDoesNotFreeTheSlot(t *testing.T) {
	store := &fakeStore{
		projects: []domain.ProjectRecord{capProject()},
		sessions: []domain.SessionRecord{busySession()},
		prs: map[domain.SessionID][]domain.PullRequest{
			"proj-1": {{URL: "https://github.com/acme/demo/pull/9", Number: 9, Merged: true}},
		},
	}
	spawner := &fakeSpawner{}
	observer := New(singleResolver(manualTracker()), store, spawner, Config{Logger: discardLogger()})

	if err := observer.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(spawner.calls) != 0 {
		t.Fatalf("spawned %d, want 0: a merged PR means teardown is imminent, not a free slot", len(spawner.calls))
	}
}

func TestUnreadablePRsCountAgainstTheCap(t *testing.T) {
	store := &fakeStore{
		projects: []domain.ProjectRecord{capProject()},
		sessions: []domain.SessionRecord{busySession()},
		prsErr:   errors.New("db is down"),
	}
	spawner := &fakeSpawner{}
	observer := New(singleResolver(manualTracker()), store, spawner, Config{Logger: discardLogger()})

	if err := observer.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(spawner.calls) != 0 {
		t.Fatalf("spawned %d, want 0: over-counting only delays a claim, under-counting breaks the cap", len(spawner.calls))
	}
}
