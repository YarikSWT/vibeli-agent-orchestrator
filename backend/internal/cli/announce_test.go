package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// announceServer captures what the CLI posted to /api/v1/announce.
func announceServer(t *testing.T, status int, respBody string) (*httptest.Server, *sendCapture) {
	t.Helper()
	capture := &sendCapture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/announce" {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		capture.body = string(body)
		capture.path = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, respBody)
	}))
	t.Cleanup(srv.Close)
	return srv, capture
}

func announcedText(t *testing.T, capture *sendCapture) string {
	t.Helper()
	var req struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(capture.body), &req); err != nil {
		t.Fatalf("decode body: %v\nbody=%s", err, capture.body)
	}
	return req.Text
}

func TestAnnounce_Success(t *testing.T) {
	t.Setenv("AO_SESSION_ID", "")
	cfg := setConfigEnv(t)
	srv, capture := announceServer(t, http.StatusOK, `{"ok":true,"text":"готово"}`)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "announce", "--message", "готово")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.path != "/api/v1/announce" {
		t.Errorf("path = %q, want /api/v1/announce", capture.path)
	}
	if got := announcedText(t, capture); got != "готово" {
		t.Errorf("announced text = %q, want %q", got, "готово")
	}
}

func TestAnnounce_LabelsTheSendingSession(t *testing.T) {
	t.Setenv("AO_SESSION_ID", "vibeli-24")
	cfg := setConfigEnv(t)
	srv, capture := announceServer(t, http.StatusOK, `{"ok":true,"text":"x"}`)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "announce", "--message", "PR готов")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	want := "💬 vibeli-24: PR готов"
	if got := announcedText(t, capture); got != want {
		t.Errorf("announced text = %q, want %q", got, want)
	}
}

func TestAnnounce_EmptyMessageIsUsageError(t *testing.T) {
	setConfigEnv(t)
	_, _, err := executeCLI(t, Deps{}, "announce", "--message", "   ")
	if err == nil {
		t.Fatal("expected usage error for empty message")
	}
	if got := ExitCode(err); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if !strings.Contains(err.Error(), "--message is required") {
		t.Fatalf("error missing usage message: %v", err)
	}
}

func TestAnnounce_ChatUnavailableExits1(t *testing.T) {
	t.Setenv("AO_SESSION_ID", "")
	cfg := setConfigEnv(t)
	srv, _ := announceServer(t, http.StatusServiceUnavailable,
		`{"error":"unavailable","code":"CHAT_UNAVAILABLE","message":"chat notifier is not configured"}`)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "announce", "--message", "привет")
	if err == nil {
		t.Fatal("expected runtime error from 503")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit code = %d, want 1", got)
	}
	if !strings.Contains(err.Error(), "CHAT_UNAVAILABLE") && !strings.Contains(errOut, "CHAT_UNAVAILABLE") {
		t.Fatalf("error did not surface the server error envelope: %v\nstderr=%s", err, errOut)
	}
}
