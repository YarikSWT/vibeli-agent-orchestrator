package telegram

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeAPI records sendMessage payloads and serves canned getUpdates batches.
type fakeAPI struct {
	mu       sync.Mutex
	sent     []string
	updates  [][]byte
	updateIx int
}

func (f *fakeAPI) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/sendMessage"):
			var body struct {
				ChatID string `json:"chat_id"`
				Text   string `json:"text"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.mu.Lock()
			f.sent = append(f.sent, body.Text)
			f.mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true}`))
		case strings.HasSuffix(r.URL.Path, "/getUpdates"):
			f.mu.Lock()
			var batch []byte
			if f.updateIx < len(f.updates) {
				batch = f.updates[f.updateIx]
				f.updateIx++
			} else {
				batch = []byte(`{"ok":true,"result":[]}`)
			}
			f.mu.Unlock()
			_, _ = w.Write(batch)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (f *fakeAPI) messages() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sent...)
}

func newTestClient(t *testing.T, api *fakeAPI) *Client {
	t.Helper()
	return New(Config{Token: "tok", ChatID: "42", APIBase: api.server(t).URL})
}

func TestSendPostsToTheConfiguredChat(t *testing.T) {
	api := &fakeAPI{}
	client := newTestClient(t, api)

	if err := client.Send(context.Background(), "привет"); err != nil {
		t.Fatal(err)
	}
	if got := api.messages(); len(got) != 1 || got[0] != "привет" {
		t.Fatalf("sent = %#v, want [привет]", got)
	}
}

func TestNewFromEnvNeedsBothTokenAndChat(t *testing.T) {
	t.Setenv(EnvBotToken, "tok")
	t.Setenv(EnvChatID, "")
	if _, ok := NewFromEnv(); ok {
		t.Fatal("a token without a chat id must not enable the notifier")
	}
	t.Setenv(EnvChatID, "42")
	if _, ok := NewFromEnv(); !ok {
		t.Fatal("token + chat id must enable the notifier")
	}
}

func waitForMessages(t *testing.T, api *fakeAPI, want int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := api.messages(); len(got) >= want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	return api.messages()
}

func TestPublisherSendsCreatedNotifications(t *testing.T) {
	api := &fakeAPI{}
	pub := NewPublisher(newTestClient(t, api), discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pub.Start(ctx)

	record := domain.NotificationRecord{
		ID:        "ntf_1",
		SessionID: "vibeli-3",
		ProjectID: "vibeli",
		PRURL:     "https://github.com/acme/demo/pull/7",
		Type:      domain.NotificationReadyToMerge,
		Title:     "PR #7 is ready to merge",
		CreatedAt: time.Now(),
	}
	if err := pub.Publish(ctx, domain.NotificationEvent{Kind: domain.NotificationCreated, Record: record}); err != nil {
		t.Fatal(err)
	}

	got := waitForMessages(t, api, 1)
	if len(got) != 1 {
		t.Fatalf("sent %d messages, want 1", len(got))
	}
	for _, want := range []string{"ready to merge", "vibeli-3", "https://github.com/acme/demo/pull/7"} {
		if !strings.Contains(got[0], want) {
			t.Errorf("message missing %q:\n%s", want, got[0])
		}
	}
}

func TestPublisherIgnoresResolvedEvents(t *testing.T) {
	api := &fakeAPI{}
	pub := NewPublisher(newTestClient(t, api), discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pub.Start(ctx)

	if err := pub.Publish(ctx, domain.NotificationEvent{
		Kind:   domain.NotificationResolved,
		Record: domain.NotificationRecord{SessionID: "vibeli-3", Type: domain.NotificationNeedsInput, Title: "resolved"},
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := api.messages(); len(got) != 0 {
		t.Fatalf("a resolution means the human already acted; nothing should be sent: %#v", got)
	}
}

// --- bot -------------------------------------------------------------------

type fakeSessions struct{ sessions []domain.SessionRecord }

func (f fakeSessions) ListAllSessions(context.Context) ([]domain.SessionRecord, error) {
	return f.sessions, nil
}

type fakeKiller struct {
	mu     sync.Mutex
	killed []domain.SessionID
}

func (f *fakeKiller) Kill(_ context.Context, id domain.SessionID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.killed = append(f.killed, id)
	return nil
}

func (f *fakeKiller) list() []domain.SessionID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.SessionID(nil), f.killed...)
}

type fakeGate struct{ paused bool }

func (f *fakeGate) Pause()       { f.paused = true }
func (f *fakeGate) Resume()      { f.paused = false }
func (f *fakeGate) Paused() bool { return f.paused }

func updateBatch(id int64, chat, text string) []byte {
	payload := map[string]any{
		"ok": true,
		"result": []map[string]any{{
			"update_id": id,
			"message": map[string]any{
				"text": text,
				"chat": map[string]any{"id": json.RawMessage(chat)},
			},
		}},
	}
	out, _ := json.Marshal(payload)
	return out
}

type fakeConveyor struct {
	items   []QueueItem
	claimed []string
	err     error
}

func (f *fakeConveyor) Queue(context.Context) ([]QueueItem, error) { return f.items, f.err }

func (f *fakeConveyor) Claim(_ context.Context, ref string) (ClaimResult, error) {
	if f.err != nil {
		return ClaimResult{}, f.err
	}
	f.claimed = append(f.claimed, ref)
	return ClaimResult{SessionID: "vibeli-9", Issue: "acme/demo#" + ref, Title: "починить"}, nil
}

func runBot(t *testing.T, api *fakeAPI, sessions SessionLister, killer Killer, gate Gate) {
	t.Helper()
	runBotWithConveyor(t, api, sessions, killer, gate, nil)
}

func runBotWithConveyor(t *testing.T, api *fakeAPI, sessions SessionLister, killer Killer, gate Gate, conveyor Conveyor) {
	t.Helper()
	runBotWithDuty(t, api, sessions, killer, gate, conveyor, nil)
}

func runBotWithDuty(t *testing.T, api *fakeAPI, sessions SessionLister, killer Killer, gate Gate, conveyor Conveyor, duty Duty) {
	t.Helper()
	bot := NewBot(newTestClient(t, api), sessions, killer, gate, conveyor, duty, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done := bot.Start(ctx)
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("bot did not stop after cancellation")
		}
	})
}

func TestBotStatusListsLiveSessions(t *testing.T) {
	api := &fakeAPI{updates: [][]byte{updateBatch(1, "42", "/status")}}
	sessions := fakeSessions{sessions: []domain.SessionRecord{
		{ID: "vibeli-3", IssueID: "github:acme/demo#16", Activity: domain.Activity{State: domain.ActivityActive}},
		{ID: "vibeli-1", IssueID: "github:acme/demo#74", IsTerminated: true},
	}}
	runBot(t, api, sessions, &fakeKiller{}, &fakeGate{})

	got := waitForMessages(t, api, 1)
	if len(got) == 0 {
		t.Fatal("no reply to /status")
	}
	if !strings.Contains(got[0], "vibeli-3") {
		t.Errorf("reply must list the live session:\n%s", got[0])
	}
	if strings.Contains(got[0], "vibeli-1") {
		t.Errorf("reply must not list a terminated session:\n%s", got[0])
	}
}

func TestBotPauseFlipsTheGate(t *testing.T) {
	api := &fakeAPI{updates: [][]byte{updateBatch(1, "42", "/pause")}}
	gate := &fakeGate{}
	runBot(t, api, fakeSessions{}, &fakeKiller{}, gate)

	waitForMessages(t, api, 1)
	if !gate.paused {
		t.Fatal("/pause must suspend claiming")
	}
}

func TestBotKillTerminatesTheNamedSession(t *testing.T) {
	api := &fakeAPI{updates: [][]byte{updateBatch(1, "42", "/kill vibeli-3")}}
	killer := &fakeKiller{}
	runBot(t, api, fakeSessions{}, killer, &fakeGate{})

	waitForMessages(t, api, 1)
	if got := killer.list(); len(got) != 1 || got[0] != "vibeli-3" {
		t.Fatalf("killed = %v, want [vibeli-3]", got)
	}
}

func TestBotIgnoresCommandsFromOtherChats(t *testing.T) {
	api := &fakeAPI{updates: [][]byte{updateBatch(1, "999", "/kill vibeli-3")}}
	killer := &fakeKiller{}
	runBot(t, api, fakeSessions{}, killer, &fakeGate{})

	time.Sleep(100 * time.Millisecond)
	if got := killer.list(); len(got) != 0 {
		t.Fatalf("a command from an unknown chat must be ignored, killed=%v", got)
	}
	if got := api.messages(); len(got) != 0 {
		t.Fatalf("an unknown chat must not even get a reply: %#v", got)
	}
}

func TestSplitCommandHandlesGroupSuffix(t *testing.T) {
	command, arg := splitCommand("/kill@vibeli_ao_bot vibeli-3")
	if command != "/kill" || arg != "vibeli-3" {
		t.Fatalf("splitCommand = (%q, %q), want (/kill, vibeli-3)", command, arg)
	}
}

func TestProxyTransportRoutesThroughTheProxy(t *testing.T) {
	var proxied []string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied = append(proxied, r.Host)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer proxy.Close()

	// http:// target so the proxy sees an absolute-form request rather than
	// CONNECT, which httptest cannot answer.
	client := New(Config{Token: "tok", ChatID: "42", APIBase: "http://api.telegram.invalid", Proxy: proxy.URL})
	if err := client.Send(context.Background(), "ping"); err != nil {
		t.Fatal(err)
	}
	if len(proxied) != 1 || proxied[0] != "api.telegram.invalid" {
		t.Fatalf("proxy saw %v, want one request for api.telegram.invalid", proxied)
	}
}

func TestNoProxyConfiguredMeansDirect(t *testing.T) {
	if got := proxyTransport("   "); got != nil {
		t.Fatalf("empty proxy must mean direct, got %#v", got)
	}
	if got := proxyTransport("::not a url::"); got != nil {
		t.Fatalf("malformed proxy must not construct a transport, got %#v", got)
	}
}

func TestTransportErrorsNeverCarryTheToken(t *testing.T) {
	// Nothing listens on this port, so client.Do fails and net/http quotes the
	// full request URL — which is where the token lives.
	client := New(Config{Token: "8981925447:SECRET", ChatID: "42", APIBase: "http://127.0.0.1:1"})

	err := client.Send(context.Background(), "ping")
	if err == nil {
		t.Fatal("expected a transport error")
	}
	if strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("bot token leaked into the error text: %v", err)
	}
	if !strings.Contains(err.Error(), "<token>") {
		t.Fatalf("token should be redacted in place, got: %v", err)
	}
}

func TestBotQueueListsBacklogInOrder(t *testing.T) {
	api := &fakeAPI{updates: [][]byte{updateBatch(1, "42", "/queue")}}
	conveyor := &fakeConveyor{items: []QueueItem{
		{Project: "vibeli", Issue: "acme/demo#46", Title: "SEO оптимизация"},
		{Project: "vibeli", Issue: "acme/demo#51", Title: "действия под сообщением"},
	}}
	runBotWithConveyor(t, api, fakeSessions{}, &fakeKiller{}, &fakeGate{}, conveyor)

	got := waitForMessages(t, api, 1)
	if len(got) == 0 {
		t.Fatal("no reply to /queue")
	}
	first := strings.Index(got[0], "acme/demo#46")
	second := strings.Index(got[0], "acme/demo#51")
	if first < 0 || second < 0 {
		t.Fatalf("reply must list both cards:\n%s", got[0])
	}
	if first > second {
		t.Fatalf("cards must keep claim order:\n%s", got[0])
	}
}

func TestBotQueueReportsEmptyBacklog(t *testing.T) {
	api := &fakeAPI{updates: [][]byte{updateBatch(1, "42", "/queue")}}
	runBotWithConveyor(t, api, fakeSessions{}, &fakeKiller{}, &fakeGate{}, &fakeConveyor{})

	got := waitForMessages(t, api, 1)
	if len(got) == 0 || !strings.Contains(got[0], "пуста") {
		t.Fatalf("empty backlog must say so, got %#v", got)
	}
}

func TestBotTakeClaimsTheNamedIssue(t *testing.T) {
	api := &fakeAPI{updates: [][]byte{updateBatch(1, "42", "/take 53")}}
	conveyor := &fakeConveyor{}
	runBotWithConveyor(t, api, fakeSessions{}, &fakeKiller{}, &fakeGate{}, conveyor)

	got := waitForMessages(t, api, 1)
	if len(conveyor.claimed) != 1 || conveyor.claimed[0] != "53" {
		t.Fatalf("claimed = %v, want [53]", conveyor.claimed)
	}
	if len(got) == 0 || !strings.Contains(got[0], "vibeli-9") {
		t.Fatalf("reply must name the started session, got %#v", got)
	}
}

func TestBotTakeWithoutArgumentExplainsUsage(t *testing.T) {
	api := &fakeAPI{updates: [][]byte{updateBatch(1, "42", "/take")}}
	conveyor := &fakeConveyor{}
	runBotWithConveyor(t, api, fakeSessions{}, &fakeKiller{}, &fakeGate{}, conveyor)

	got := waitForMessages(t, api, 1)
	if len(conveyor.claimed) != 0 {
		t.Fatalf("nothing should be claimed without an argument: %v", conveyor.claimed)
	}
	if len(got) == 0 || !strings.Contains(got[0], "/take 53") {
		t.Fatalf("reply must show the usage, got %#v", got)
	}
}

func TestTruncateCountsRunesNotBytes(t *testing.T) {
	// Cyrillic is 2 bytes per rune: a byte-based cut would slice a character
	// in half and put a replacement glyph in the chat.
	if got := truncate("почини сборку", 6); got != "почини…" {
		t.Fatalf("truncate = %q, want почини…", got)
	}
	if got := truncate("короткий", 20); got != "короткий" {
		t.Fatalf("short text must pass through unchanged, got %q", got)
	}
}

// --- questions to the agent on duty ----------------------------------------

type fakeDuty struct {
	mu    sync.Mutex
	asked []string
	err   error
}

func (f *fakeDuty) Ask(_ context.Context, text string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", f.err
	}
	f.asked = append(f.asked, text)
	return "vibeli-24", nil
}

func (f *fakeDuty) list() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.asked...)
}

func TestBotRoutesAPlainMessageToTheAgentOnDuty(t *testing.T) {
	api := &fakeAPI{updates: [][]byte{updateBatch(1, "42", "почему vibeli-7 стоит?")}}
	duty := &fakeDuty{}
	runBotWithDuty(t, api, fakeSessions{}, &fakeKiller{}, &fakeGate{}, nil, duty)

	got := waitForMessages(t, api, 1)
	if asked := duty.list(); len(asked) != 1 || asked[0] != "почему vibeli-7 стоит?" {
		t.Fatalf("asked = %#v, want the human's message verbatim", asked)
	}
	if !strings.Contains(got[0], "vibeli-24") {
		t.Errorf("the reply must name the session that took the question:\n%s", got[0])
	}
}

func TestBotAnswersWhenNobodyIsOnDuty(t *testing.T) {
	api := &fakeAPI{updates: [][]byte{updateBatch(1, "42", "живой?")}}
	duty := &fakeDuty{err: ErrNoDutyAgent}
	runBotWithDuty(t, api, fakeSessions{}, &fakeKiller{}, &fakeGate{}, nil, duty)

	got := waitForMessages(t, api, 1)
	if !strings.Contains(got[0], "дежурного") {
		t.Errorf("a question with no one on duty must be answered, not swallowed:\n%s", got[0])
	}
}

func TestBotIgnoresPlainMessagesFromOtherChats(t *testing.T) {
	api := &fakeAPI{updates: [][]byte{updateBatch(1, "999", "кто дежурный?")}}
	duty := &fakeDuty{}
	runBotWithDuty(t, api, fakeSessions{}, &fakeKiller{}, &fakeGate{}, nil, duty)

	time.Sleep(100 * time.Millisecond)
	if asked := duty.list(); len(asked) != 0 {
		t.Fatalf("a message from an unknown chat must not reach the agent: %#v", asked)
	}
	if got := api.messages(); len(got) != 0 {
		t.Fatalf("an unknown chat must not even get a reply: %#v", got)
	}
}
