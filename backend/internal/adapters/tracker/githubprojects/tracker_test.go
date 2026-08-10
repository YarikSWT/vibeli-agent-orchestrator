package githubprojects

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	trackergithub "github.com/aoagents/agent-orchestrator/backend/internal/adapters/tracker/github"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// boardFixture is a tiny in-memory Projects v2 board: two cards in the ready
// column (one per repository) and one already in progress.
const boardItemsJSON = `{
  "node": {
    "items": {
      "pageInfo": {"hasNextPage": false, "endCursor": ""},
      "nodes": [
        {
          "id": "PVTI_ready",
          "fieldValueByName": {"name": "Ready"},
          "content": {
            "number": 74, "title": "brand leak", "body": "fix it", "url": "https://github.com/acme/demo/issues/74",
            "state": "OPEN", "repository": {"nameWithOwner": "acme/demo"},
            "labels": {"nodes": [{"name": "type:bug"}]},
            "assignees": {"nodes": [{"login": "octocat"}]}
          }
        },
        {
          "id": "PVTI_other_repo",
          "fieldValueByName": {"name": "Ready"},
          "content": {
            "number": 9, "title": "other repo card", "body": "", "url": "https://github.com/acme/other/issues/9",
            "state": "OPEN", "repository": {"nameWithOwner": "acme/other"},
            "labels": {"nodes": []}, "assignees": {"nodes": []}
          }
        },
        {
          "id": "PVTI_busy",
          "fieldValueByName": {"name": "In progress"},
          "content": {
            "number": 75, "title": "already claimed", "body": "", "url": "https://github.com/acme/demo/issues/75",
            "state": "OPEN", "repository": {"nameWithOwner": "acme/demo"},
            "labels": {"nodes": []}, "assignees": {"nodes": []}
          }
        },
        {
          "id": "PVTI_draft",
          "fieldValueByName": {"name": "Ready"},
          "content": null
        }
      ]
    }
  }
}`

const statusFieldJSON = `{
  "node": {
    "field": {
      "id": "PVTSSF_status",
      "options": [
        {"id": "opt_ready", "name": "Ready"},
        {"id": "opt_progress", "name": "In progress"},
        {"id": "opt_done", "name": "Done"}
      ]
    }
  }
}`

// fakeBoard serves the two queries and records mutations.
type fakeBoard struct {
	mutations []map[string]any
	calls     int
}

func (f *fakeBoard) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls++
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("Authorization = %q, want Bearer tok", got)
		}
		var body struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(body.Query, "updateProjectV2ItemFieldValue"):
			f.mutations = append(f.mutations, body.Variables)
			_, _ = w.Write([]byte(`{"data":{"updateProjectV2ItemFieldValue":{"projectV2Item":{"id":"PVTI_ready"}}}}`))
		case strings.Contains(body.Query, "field(name:$field)"):
			_, _ = w.Write([]byte(`{"data":` + statusFieldJSON + `}`))
		default:
			_, _ = w.Write([]byte(`{"data":` + boardItemsJSON + `}`))
		}
	})
}

func newTestTracker(t *testing.T, board *fakeBoard) *Tracker {
	t.Helper()
	srv := httptest.NewServer(board.handler(t))
	t.Cleanup(srv.Close)
	tracker, err := New(Options{
		Token:      trackergithub.StaticTokenSource("tok"),
		ProjectID:  "PVT_board",
		GraphQLURL: srv.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tracker
}

func TestListReturnsOnlyReadyCardsForTheRepo(t *testing.T) {
	tracker := newTestTracker(t, &fakeBoard{})

	issues, err := tracker.List(context.Background(), domain.TrackerRepo{
		Provider: domain.TrackerProviderGitHub,
		Native:   "acme/demo",
	}, domain.ListFilter{State: domain.ListOpen})
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 1 {
		t.Fatalf("got %d issues, want 1: %#v", len(issues), issues)
	}
	got := issues[0]
	if got.ID.Native != "acme/demo#74" {
		t.Errorf("ID.Native = %q, want acme/demo#74", got.ID.Native)
	}
	if got.ID.Provider != domain.TrackerProviderGitHub {
		t.Errorf("ID.Provider = %q, want github: board cards must keep plain github issue identity", got.ID.Provider)
	}
	if got.State != domain.IssueOpen {
		t.Errorf("State = %q, want open", got.State)
	}
	if len(got.Labels) != 1 || got.Labels[0] != "type:bug" {
		t.Errorf("Labels = %v, want [type:bug]", got.Labels)
	}
}

func TestListAcrossAllRepositoriesWhenRepoIsEmpty(t *testing.T) {
	tracker := newTestTracker(t, &fakeBoard{})

	issues, err := tracker.List(context.Background(), domain.TrackerRepo{}, domain.ListFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 2 {
		t.Fatalf("got %d issues, want both ready cards: %#v", len(issues), issues)
	}
}

func TestListHonorsAssigneeFilter(t *testing.T) {
	tracker := newTestTracker(t, &fakeBoard{})

	issues, err := tracker.List(context.Background(), domain.TrackerRepo{}, domain.ListFilter{Assignee: "none"})
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 1 || issues[0].ID.Native != "acme/other#9" {
		t.Fatalf("assignee=none should keep only the unassigned card, got %#v", issues)
	}
}

func TestStatusReportsCurrentColumn(t *testing.T) {
	tracker := newTestTracker(t, &fakeBoard{})

	status, err := tracker.Status(context.Background(), "github:acme/demo#75")
	if err != nil {
		t.Fatal(err)
	}
	if status != "In progress" {
		t.Fatalf("status = %q, want In progress", status)
	}

	if _, err := tracker.Status(context.Background(), "github:acme/demo#404"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown issue error = %v, want ErrNotFound", err)
	}
}

func TestSetStatusMovesCard(t *testing.T) {
	board := &fakeBoard{}
	tracker := newTestTracker(t, board)

	if err := tracker.SetStatus(context.Background(), "github:acme/demo#74", "In progress"); err != nil {
		t.Fatal(err)
	}
	if len(board.mutations) != 1 {
		t.Fatalf("got %d mutations, want 1", len(board.mutations))
	}
	vars := board.mutations[0]
	if vars["item"] != "PVTI_ready" {
		t.Errorf("item = %v, want PVTI_ready", vars["item"])
	}
	if vars["field"] != "PVTSSF_status" {
		t.Errorf("field = %v, want PVTSSF_status", vars["field"])
	}
	if vars["option"] != "opt_progress" {
		t.Errorf("option = %v, want opt_progress", vars["option"])
	}
}

func TestSetStatusIsNoOpWhenAlreadyThere(t *testing.T) {
	board := &fakeBoard{}
	tracker := newTestTracker(t, board)

	if err := tracker.SetStatus(context.Background(), "github:acme/demo#74", "ready"); err != nil {
		t.Fatal(err)
	}
	if len(board.mutations) != 0 {
		t.Fatalf("a card already in the target column must not be written: %#v", board.mutations)
	}
}

func TestSetStatusRejectsUnknownColumn(t *testing.T) {
	board := &fakeBoard{}
	tracker := newTestTracker(t, board)

	err := tracker.SetStatus(context.Background(), "github:acme/demo#74", "Waiting Approval")
	if !errors.Is(err, ErrNoSuchStatus) {
		t.Fatalf("err = %v, want ErrNoSuchStatus", err)
	}
}

func TestGraphQLErrorsSurface(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":null,"errors":[{"type":"FORBIDDEN","message":"missing project scope"}]}`))
	}))
	defer srv.Close()

	tracker, err := New(Options{Token: trackergithub.StaticTokenSource("tok"), ProjectID: "PVT_board", GraphQLURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tracker.List(context.Background(), domain.TrackerRepo{}, domain.ListFilter{}); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("err = %v, want ErrAuthFailed", err)
	}
}

func TestNewRequiresProjectID(t *testing.T) {
	if _, err := New(Options{Token: trackergithub.StaticTokenSource("tok")}); err == nil {
		t.Fatal("New without ProjectID must fail")
	}
	if _, err := New(Options{ProjectID: "PVT_board"}); !errors.Is(err, trackergithub.ErrNoToken) {
		t.Fatal("New without a token must report ErrNoToken")
	}
}
