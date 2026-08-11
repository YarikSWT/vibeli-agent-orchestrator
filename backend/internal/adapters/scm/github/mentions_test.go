package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// timelineServer serves one page of issue comments and records the query it was
// asked for.
func timelineServer(t *testing.T, comments []map[string]any) (*httptest.Server, *string) {
	t.Helper()
	var lastQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/issues/7/comments") {
			http.NotFound(w, r)
			return
		}
		lastQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(comments)
	}))
	t.Cleanup(srv.Close)
	return srv, &lastQuery
}

func mentionProvider(t *testing.T, srv *httptest.Server, trigger string) *Provider {
	t.Helper()
	p, err := NewProvider(ProviderOptions{
		RESTBase:           srv.URL,
		SkipTokenPreflight: true,
		MentionTrigger:     trigger,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func comment(id int64, login, kind, body string, at time.Time) map[string]any {
	return map[string]any{
		"id":         id,
		"body":       body,
		"html_url":   "https://github.com/o/r/pull/7#issuecomment-" + string(rune('0'+id%10)),
		"created_at": at.Format(time.RFC3339),
		"user":       map[string]any{"login": login, "type": kind},
	}
}

func prRef() ports.SCMPRRef {
	return ports.SCMPRRef{Repo: ports.SCMRepo{Owner: "o", Name: "r", Repo: "o/r"}, Number: 7}
}

func TestOnlyTriggeredCommentsBecomeMentions(t *testing.T) {
	now := time.Now().UTC()
	srv, query := timelineServer(t, []map[string]any{
		comment(1, "alice", "User", "выглядит хорошо, мержим?", now),
		comment(2, "bob", "User", "@ao прикрепи скриншот", now.Add(time.Minute)),
		comment(3, "ci-bot", "Bot", "@ao build finished", now.Add(2*time.Minute)),
	})
	p := mentionProvider(t, srv, "")

	got := p.fetchMentions(context.Background(), prRef(), now)

	if len(got) != 1 {
		t.Fatalf("mentions = %+v, want only the human comment carrying the trigger", got)
	}
	if got[0].Author != "bob" || !strings.Contains(got[0].Body, "скриншот") {
		t.Fatalf("mention = %+v", got[0])
	}
	// A bounded window keeps the newest comments on the first page of a long thread.
	if !strings.Contains(*query, "since=") {
		t.Fatalf("query = %q, want a since bound", *query)
	}
}

func TestPeopleTalkingDoesNotWakeTheAgent(t *testing.T) {
	now := time.Now().UTC()
	srv, _ := timelineServer(t, []map[string]any{
		comment(1, "alice", "User", "а тесты гоняли?", now),
		comment(2, "bob", "User", "да, зелёные", now.Add(time.Minute)),
	})
	p := mentionProvider(t, srv, "")

	if got := p.fetchMentions(context.Background(), prRef(), now); len(got) != 0 {
		t.Fatalf("mentions = %+v, want none: a discussion between humans is not an instruction", got)
	}
}

func TestTriggerIsNotMatchedInsideAWord(t *testing.T) {
	now := time.Now().UTC()
	srv, _ := timelineServer(t, []map[string]any{
		comment(1, "alice", "User", "смотри на @aoagents/orchestrator", now),
		comment(2, "bob", "User", "пиши на me@ao.io", now),
	})
	p := mentionProvider(t, srv, "")

	if got := p.fetchMentions(context.Background(), prRef(), now); len(got) != 0 {
		t.Fatalf("mentions = %+v, want none: neither is addressing the agent", got)
	}
}

func TestTriggerIsConfigurableAndCaseInsensitive(t *testing.T) {
	now := time.Now().UTC()
	srv, _ := timelineServer(t, []map[string]any{
		comment(1, "alice", "User", "@Bot почини сборку", now),
		comment(2, "bob", "User", "@ao это уже другой триггер", now),
	})
	p := mentionProvider(t, srv, "@bot")

	got := p.fetchMentions(context.Background(), prRef(), now)

	if len(got) != 1 || got[0].Author != "alice" {
		t.Fatalf("mentions = %+v, want the @Bot comment only", got)
	}
}

func TestUnreadableTimelineIsNotFatal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	p := mentionProvider(t, srv, "")

	// Mentions are an extra channel; losing them must not cost AO its review poll.
	if got := p.fetchMentions(context.Background(), prRef(), time.Now()); got != nil {
		t.Fatalf("mentions = %+v, want nil on a provider error", got)
	}
}
