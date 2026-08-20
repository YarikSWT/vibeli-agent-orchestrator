package projectsync

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	trackerprojects "github.com/aoagents/agent-orchestrator/backend/internal/adapters/tracker/githubprojects"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeBoard struct {
	status map[domain.IssueID]string
	writes []write
	err    error
}

type write struct {
	issue  domain.IssueID
	status string
}

func (f *fakeBoard) Status(_ context.Context, id domain.IssueID) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	status, ok := f.status[id]
	if !ok {
		return "", trackerprojects.ErrNotFound
	}
	return status, nil
}

func (f *fakeBoard) SetStatus(_ context.Context, id domain.IssueID, status string) error {
	f.writes = append(f.writes, write{issue: id, status: status})
	if f.status == nil {
		f.status = map[domain.IssueID]string{}
	}
	f.status[id] = status
	return nil
}

type fakeResolver struct{ board Board }

func (f fakeResolver) Resolve(domain.TrackerIntakeConfig) (Board, error) { return f.board, nil }

type fakeStore struct {
	projects []domain.ProjectRecord
	sessions []domain.SessionRecord
	prs      map[domain.SessionID][]domain.PullRequest
}

func (f *fakeStore) ListProjects(context.Context) ([]domain.ProjectRecord, error) {
	return f.projects, nil
}

func (f *fakeStore) ListAllSessions(context.Context) ([]domain.SessionRecord, error) {
	return f.sessions, nil
}

func (f *fakeStore) ListPRsBySession(_ context.Context, id domain.SessionID) ([]domain.PullRequest, error) {
	return f.prs[id], nil
}

type fakeKiller struct{ killed []domain.SessionID }

func (f *fakeKiller) Kill(_ context.Context, id domain.SessionID) error {
	f.killed = append(f.killed, id)
	return nil
}

func boardProject() domain.ProjectRecord {
	return domain.ProjectRecord{
		ID: "vibeli",
		Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{
			Enabled:   true,
			Provider:  domain.TrackerProviderGitHubProjects,
			ProjectID: "PVT_board",
		}},
	}
}

func session(created time.Time) domain.SessionRecord {
	return domain.SessionRecord{
		ID:        "vibeli-1",
		ProjectID: "vibeli",
		IssueID:   "github:acme/demo#74",
		CreatedAt: created,
	}
}

func newObserver(board Board, store Store, killer Killer, now time.Time) *Observer {
	return New(fakeResolver{board}, store, killer, Config{
		Clock:  func() time.Time { return now },
		Logger: discardLogger(),
	})
}

func TestLiveSessionMovesCardToInProgress(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	board := &fakeBoard{status: map[domain.IssueID]string{"github:acme/demo#74": "Ready"}}
	store := &fakeStore{projects: []domain.ProjectRecord{boardProject()}, sessions: []domain.SessionRecord{session(now)}}

	// Created "now": inside the reclaim grace window, so a Ready card is AO
	// lagging behind rather than a human taking the work back.
	if err := newObserver(board, store, &fakeKiller{}, now).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(board.writes) != 1 || board.writes[0].status != "In progress" {
		t.Fatalf("writes = %#v, want one In progress", board.writes)
	}
}

func TestOpenPRMovesCardToInReview(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	board := &fakeBoard{status: map[domain.IssueID]string{"github:acme/demo#74": "In progress"}}
	store := &fakeStore{
		projects: []domain.ProjectRecord{boardProject()},
		sessions: []domain.SessionRecord{session(now)},
		prs:      map[domain.SessionID][]domain.PullRequest{"vibeli-1": {{Number: 76}}},
	}

	if err := newObserver(board, store, &fakeKiller{}, now).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(board.writes) != 1 || board.writes[0].status != "In review" {
		t.Fatalf("writes = %#v, want one In review", board.writes)
	}
}

func TestMergedPRMovesCardToDone(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	board := &fakeBoard{status: map[domain.IssueID]string{"github:acme/demo#74": "In review"}}
	sess := session(now)
	sess.IsTerminated = true
	store := &fakeStore{
		projects: []domain.ProjectRecord{boardProject()},
		sessions: []domain.SessionRecord{sess},
		prs:      map[domain.SessionID][]domain.PullRequest{"vibeli-1": {{Number: 76, Merged: true}}},
	}

	if err := newObserver(board, store, &fakeKiller{}, now).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(board.writes) != 1 || board.writes[0].status != "Done" {
		t.Fatalf("writes = %#v, want one Done", board.writes)
	}
}

func TestFinishedSessionWithUnmergedPRIsLeftAlone(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	board := &fakeBoard{status: map[domain.IssueID]string{"github:acme/demo#74": "In review"}}
	sess := session(now)
	sess.IsTerminated = true
	store := &fakeStore{
		projects: []domain.ProjectRecord{boardProject()},
		sessions: []domain.SessionRecord{sess},
		prs:      map[domain.SessionID][]domain.PullRequest{"vibeli-1": {{Number: 76}}},
	}

	if err := newObserver(board, store, &fakeKiller{}, now).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(board.writes) != 0 {
		t.Fatalf("card already in review must not be rewritten: %#v", board.writes)
	}
}

func TestCardDraggedBackToReadyKillsTheSession(t *testing.T) {
	created := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	now := created.Add(10 * time.Minute)
	board := &fakeBoard{status: map[domain.IssueID]string{"github:acme/demo#74": "Ready"}}
	store := &fakeStore{projects: []domain.ProjectRecord{boardProject()}, sessions: []domain.SessionRecord{session(created)}}
	killer := &fakeKiller{}

	observer := New(fakeResolver{board}, store, killer, Config{
		Clock:  func() time.Time { return now },
		Logger: discardLogger(),
	})
	// The first sweep is the one that stamps the session as seen; the human
	// drag is only distinguishable from "not moved yet" on a later sweep.
	if err := observer.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	board.status["github:acme/demo#74"] = "Ready"
	board.writes = nil
	now = now.Add(10 * time.Minute)
	if err := observer.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(killer.killed) != 1 || killer.killed[0] != "vibeli-1" {
		t.Fatalf("killed = %v, want [vibeli-1]", killer.killed)
	}
	if len(board.writes) != 0 {
		t.Fatalf("a reclaimed card must be left where the human put it: %#v", board.writes)
	}
}

func TestTerminatedSessionOnReadyCardIsNotKilled(t *testing.T) {
	created := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	now := created.Add(10 * time.Minute)
	board := &fakeBoard{status: map[domain.IssueID]string{"github:acme/demo#74": "Ready"}}
	sess := session(created)
	sess.IsTerminated = true
	store := &fakeStore{projects: []domain.ProjectRecord{boardProject()}, sessions: []domain.SessionRecord{sess}}
	killer := &fakeKiller{}

	if err := newObserver(board, store, killer, now).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(killer.killed) != 0 {
		t.Fatalf("an already-terminated session must not be killed again: %v", killer.killed)
	}
}

func TestReopenedTaskOnReadyCardIsNotPushedBackToDone(t *testing.T) {
	created := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	now := created.Add(10 * time.Minute)
	board := &fakeBoard{status: map[domain.IssueID]string{"github:acme/demo#74": "Ready"}}
	sess := session(created)
	sess.IsTerminated = true
	store := &fakeStore{
		projects: []domain.ProjectRecord{boardProject()},
		sessions: []domain.SessionRecord{sess},
		prs:      map[domain.SessionID][]domain.PullRequest{"vibeli-1": {{Number: 76, Merged: true}}},
	}

	if err := newObserver(board, store, &fakeKiller{}, now).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(board.writes) != 0 {
		t.Fatalf("a reopened card must stay in the ready column, got %#v", board.writes)
	}
}

func TestSecondAttemptOwnsTheCardInsteadOfTheMergedOne(t *testing.T) {
	created := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	now := created.Add(2 * time.Hour)
	board := &fakeBoard{status: map[domain.IssueID]string{"github:acme/demo#74": "In review"}}

	merged := session(created)
	merged.ID = "vibeli-1"
	merged.IsTerminated = true

	live := session(now)
	live.ID = "vibeli-2"

	store := &fakeStore{
		projects: []domain.ProjectRecord{boardProject()},
		sessions: []domain.SessionRecord{merged, live},
		prs: map[domain.SessionID][]domain.PullRequest{
			"vibeli-1": {{Number: 76, Merged: true}},
			"vibeli-2": {{Number: 90}},
		},
	}

	if err := newObserver(board, store, &fakeKiller{}, now).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(board.writes) != 0 {
		t.Fatalf("the merged attempt must not drag the card to Done: %#v", board.writes)
	}
}

func TestSweepThatFallsBehindDoesNotKillAFreshSession(t *testing.T) {
	created := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	// The session is already older than the grace window when this loop first
	// gets to it — a rate-limited board makes one sweep take minutes. Its card
	// still reads "Ready" because nothing has moved it yet, which must not be
	// mistaken for a human taking the work back.
	now := created.Add(30 * time.Minute)
	board := &fakeBoard{status: map[domain.IssueID]string{"github:acme/demo#74": "Ready"}}
	store := &fakeStore{projects: []domain.ProjectRecord{boardProject()}, sessions: []domain.SessionRecord{session(created)}}
	killer := &fakeKiller{}

	if err := newObserver(board, store, killer, now).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(killer.killed) != 0 {
		t.Fatalf("killed = %v, want none: the card was never moved off Ready", killer.killed)
	}
	if len(board.writes) != 1 || board.writes[0].status != "In progress" {
		t.Fatalf("writes = %#v, want the card moved to In progress", board.writes)
	}
}

func TestProjectsWithoutBoardIntakeAreIgnored(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	board := &fakeBoard{status: map[domain.IssueID]string{"github:acme/demo#74": "Ready"}}
	plain := domain.ProjectRecord{
		ID: "vibeli",
		Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{
			Enabled:  true,
			Provider: domain.TrackerProviderGitHub,
			Assignee: "octocat",
		}},
	}
	store := &fakeStore{projects: []domain.ProjectRecord{plain}, sessions: []domain.SessionRecord{session(now)}}

	if err := newObserver(board, store, &fakeKiller{}, now).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(board.writes) != 0 {
		t.Fatalf("issue-list intake has no board to write to: %#v", board.writes)
	}
}

func TestSessionWithoutCardIsSkipped(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	board := &fakeBoard{status: map[domain.IssueID]string{}}
	store := &fakeStore{projects: []domain.ProjectRecord{boardProject()}, sessions: []domain.SessionRecord{session(now)}}

	if err := newObserver(board, store, &fakeKiller{}, now).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(board.writes) != 0 {
		t.Fatalf("no card, no write: %#v", board.writes)
	}
}
