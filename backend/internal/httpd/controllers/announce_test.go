package controllers_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
)

type fakeAnnouncer struct {
	sent     []string
	sessions []string
	err      error
}

func (f *fakeAnnouncer) Announce(_ context.Context, text, session string) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, text)
	f.sessions = append(f.sessions, session)
	return nil
}

func newAnnounceTestServer(t *testing.T, chat controllers.ChatAnnouncer) *httptest.Server {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(httpd.NewRouterWithControl(config.Config{}, log, nil, httpd.APIDeps{Announce: chat}, httpd.ControlDeps{}))
	t.Cleanup(srv.Close)
	return srv
}

func postAnnounce(t *testing.T, srv *httptest.Server, body string) *http.Response {
	t.Helper()
	res, err := http.Post(srv.URL+"/api/v1/announce", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	return res
}

func TestAnnounce_QueuesTheMessage(t *testing.T) {
	chat := &fakeAnnouncer{}
	srv := newAnnounceTestServer(t, chat)

	res := postAnnounce(t, srv, `{"text":"дежурный: релиз уехал"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if len(chat.sent) != 1 || chat.sent[0] != "дежурный: релиз уехал" {
		t.Fatalf("announced = %#v", chat.sent)
	}
	var out controllers.AnnounceResponse
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.OK || out.Text != "дежурный: релиз уехал" {
		t.Fatalf("response = %+v", out)
	}
}

func TestAnnounce_EmptyTextIsRejected(t *testing.T) {
	chat := &fakeAnnouncer{}
	srv := newAnnounceTestServer(t, chat)

	res := postAnnounce(t, srv, `{"text":"   "}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", res.StatusCode)
	}
	if len(chat.sent) != 0 {
		t.Fatalf("blank text must not reach the chat: %#v", chat.sent)
	}
}

func TestAnnounce_OverlongTextIsRejected(t *testing.T) {
	chat := &fakeAnnouncer{}
	srv := newAnnounceTestServer(t, chat)

	// Cyrillic: a rune-counted cap must not be a byte-counted one.
	res := postAnnounce(t, srv, `{"text":"`+strings.Repeat("я", 4001)+`"}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", res.StatusCode)
	}
	if len(chat.sent) != 0 {
		t.Fatalf("overlong text must not reach the chat: %#v", chat.sent)
	}
}

func TestAnnounce_ChatFailureIsReported(t *testing.T) {
	chat := &fakeAnnouncer{err: controllers.ErrChatUnavailable}
	srv := newAnnounceTestServer(t, chat)

	res := postAnnounce(t, srv, `{"text":"привет"}`)
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", res.StatusCode)
	}
	body, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(body), "CHAT_UNAVAILABLE") {
		t.Fatalf("body did not name the failure: %s", body)
	}
}

// A daemon built without a chat transport still mounts the route; it must say
// so rather than 404 or silently swallow the message.
func TestAnnounce_WithoutChatIsNotImplemented(t *testing.T) {
	srv := newAnnounceTestServer(t, nil)

	res := postAnnounce(t, srv, `{"text":"привет"}`)
	if res.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", res.StatusCode)
	}
}

// The sender travels as its own field: the chat transport labels the message
// with it and uses it to find the message this one answers.
func TestAnnounce_PassesTheSendingSessionThrough(t *testing.T) {
	chat := &fakeAnnouncer{}
	srv := newAnnounceTestServer(t, chat)

	res := postAnnounce(t, srv, `{"text":"всё готово","session":"vibeli-24"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if len(chat.sessions) != 1 || chat.sessions[0] != "vibeli-24" {
		t.Fatalf("sessions = %#v", chat.sessions)
	}
}
