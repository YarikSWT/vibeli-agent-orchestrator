package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	trackergithub "github.com/aoagents/agent-orchestrator/backend/internal/adapters/tracker/github"
	trackerprojects "github.com/aoagents/agent-orchestrator/backend/internal/adapters/tracker/githubprojects"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/observe/projectsync"
	trackerintake "github.com/aoagents/agent-orchestrator/backend/internal/observe/trackerintake"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	aoprocess "github.com/aoagents/agent-orchestrator/backend/internal/process"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// startTrackerIntake wires the opt-in GitHub issue-intake loop. The observer
// always runs — Poll re-reads each project's config on every tick and skips
// projects with intake disabled, so a project enabling intake after daemon
// boot is picked up on the next tick without a restart. The adapter itself
// stays lazy so daemon readiness is not blocked by credential probing or a gh
// CLI call, and no token is resolved until some enabled project is actually
// polled.
// The two loops share one board registry on purpose: it carries the issue ->
// card index and the board's field ids, so reads done by intake spare the
// status-sync writes a lookup and vice versa.
func startTrackerIntake(ctx context.Context, store *sqlite.Store, sessions *sessionsvc.Service, logger *slog.Logger) (intakeDone, syncDone <-chan struct{}) {
	issues := newLazyGitHubTracker(logger)
	boards := newBoardRegistry(issues, logger)
	resolver := &trackerProviderResolver{issues: issues, boards: boards}
	intake := trackerintake.New(resolver, store, sessions, trackerintake.Config{Logger: logger})
	return intake.Start(ctx), startProjectStatusSync(ctx, boards, store, sessions, logger)
}

// startProjectStatusSync wires the write half of board-backed intake: it pushes
// session progress onto the Projects v2 board and kills a session whose card a
// human dragged back to the ready column. Projects that do not use the
// github-projects provider are skipped on every tick, so this loop is inert for
// a stock install.
func startProjectStatusSync(ctx context.Context, boards *boardRegistry, store *sqlite.Store, sessions *sessionsvc.Service, logger *slog.Logger) <-chan struct{} {
	observer := projectsync.New(boardResolver{boards}, store, sessionKiller{sessions: sessions}, projectsync.Config{
		Statuses: projectStatusMapFromEnv(),
		Logger:   logger,
	})
	return observer.Start(ctx)
}

// projectStatusMapFromEnv lets a deployment rename the board columns AO writes
// without a rebuild. Unset values keep the stock Projects v2 column names.
func projectStatusMapFromEnv() projectsync.StatusMap {
	statuses := projectsync.DefaultStatusMap()
	if v := strings.TrimSpace(os.Getenv("AO_PROJECT_STATUS_IN_PROGRESS")); v != "" {
		statuses.InProgress = v
	}
	if v := strings.TrimSpace(os.Getenv("AO_PROJECT_STATUS_IN_REVIEW")); v != "" {
		statuses.InReview = v
	}
	if v := strings.TrimSpace(os.Getenv("AO_PROJECT_STATUS_DONE")); v != "" {
		statuses.Done = v
	}
	return statuses
}

// sessionKiller adapts the session service to the narrow Killer surface. The
// service reports whether the kill changed anything; the loop does not care, so
// the bool is dropped.
type sessionKiller struct {
	sessions *sessionsvc.Service
}

func (k sessionKiller) Kill(ctx context.Context, id domain.SessionID) error {
	_, err := k.sessions.Kill(ctx, id)
	return err
}

// ---------------------------------------------------------------------------
// Provider resolution
// ---------------------------------------------------------------------------

// trackerProviderResolver dispatches a project's intake config to the adapter
// that can serve it: the repository issue list, or a Projects v2 board.
type trackerProviderResolver struct {
	issues ports.Tracker
	boards *boardRegistry
}

func (r *trackerProviderResolver) Resolve(cfg domain.TrackerIntakeConfig) (ports.Tracker, error) {
	switch cfg.WithDefaults().Provider {
	case domain.TrackerProviderGitHub:
		return r.issues, nil
	case domain.TrackerProviderGitHubProjects:
		// Return through a typed variable: handing back a nil *Tracker directly
		// would produce a non-nil ports.Tracker holding a nil pointer.
		board, err := r.boards.Resolve(cfg)
		if err != nil {
			return nil, err
		}
		return board, nil
	default:
		return nil, fmt.Errorf("tracker intake: no adapter for provider %q", cfg.Provider)
	}
}

// boardResolver adapts the concrete registry to the observer's Board-returning
// interface; Go will not do that conversion on the return type for us.
type boardResolver struct{ registry *boardRegistry }

func (b boardResolver) Resolve(cfg domain.TrackerIntakeConfig) (projectsync.Board, error) {
	board, err := b.registry.Resolve(cfg)
	if err != nil {
		return nil, err
	}
	return board, nil
}

// boardRegistry caches one Projects v2 adapter per (board, ready column) pair.
// Caching matters beyond object reuse: the adapter holds the issue -> card index
// and the board's status-field ids, so a shared instance keeps both loops
// (intake reads, status sync writes) off redundant GraphQL calls.
type boardRegistry struct {
	issues ports.Tracker
	tokens *trackerTokenSource
	logger *slog.Logger

	mu     sync.Mutex
	boards map[string]*trackerprojects.Tracker
}

func newBoardRegistry(issues ports.Tracker, logger *slog.Logger) *boardRegistry {
	return &boardRegistry{issues: issues, tokens: &trackerTokenSource{}, logger: logger, boards: map[string]*trackerprojects.Tracker{}}
}

// Resolve returns the adapter for a config's board, constructing it on first
// use. Construction is offline (no token probe), so a daemon can boot with
// GitHub unreachable.
func (r *boardRegistry) Resolve(cfg domain.TrackerIntakeConfig) (*trackerprojects.Tracker, error) {
	cfg = cfg.WithDefaults()
	key := cfg.ProjectID + "\x00" + cfg.ReadyStatus
	r.mu.Lock()
	defer r.mu.Unlock()
	if board, ok := r.boards[key]; ok {
		return board, nil
	}
	board, err := trackerprojects.New(trackerprojects.Options{
		Token:       r.tokens,
		ProjectID:   cfg.ProjectID,
		ReadyStatus: cfg.ReadyStatus,
		Issues:      r.issues,
	})
	if err != nil {
		return nil, err
	}
	r.boards[key] = board
	return board, nil
}

// ---------------------------------------------------------------------------
// GitHub lazy adapter (token sourced from env or gh CLI fallback)
// ---------------------------------------------------------------------------

type lazyGitHubTracker struct {
	logger  *slog.Logger
	tokens  *trackerTokenSource
	mu      sync.Mutex
	tracker ports.Tracker
}

func newLazyGitHubTracker(logger *slog.Logger) *lazyGitHubTracker {
	return &lazyGitHubTracker{logger: logger, tokens: &trackerTokenSource{}}
}

func (t *lazyGitHubTracker) Get(ctx context.Context, id domain.TrackerID) (domain.Issue, error) {
	tracker, err := t.resolve()
	if err != nil {
		return domain.Issue{}, err
	}
	return tracker.Get(ctx, id)
}

func (t *lazyGitHubTracker) List(ctx context.Context, repo domain.TrackerRepo, filter domain.ListFilter) ([]domain.Issue, error) {
	tracker, err := t.resolve()
	if err != nil {
		return nil, err
	}
	return tracker.List(ctx, repo, filter)
}

func (t *lazyGitHubTracker) Preflight(ctx context.Context) error {
	tracker, err := t.resolve()
	if err != nil {
		return err
	}
	return tracker.Preflight(ctx)
}

func (t *lazyGitHubTracker) resolve() (ports.Tracker, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tracker != nil {
		return t.tracker, nil
	}
	tracker, err := trackergithub.New(trackergithub.Options{Token: t.tokens})
	if err != nil {
		if errors.Is(err, trackergithub.ErrNoToken) && t.logger != nil {
			t.logger.Warn("tracker intake disabled: no usable GitHub token", "err", err)
		}
		return nil, err
	}
	t.tracker = tracker
	return tracker, nil
}

const (
	trackerTokenCacheTTL       = 5 * time.Minute
	trackerTokenCommandTimeout = 5 * time.Second
)

// trackerTokenSource mirrors the SCM credential precedence while returning the
// tracker adapter's own ErrNoToken sentinel.
type trackerTokenSource struct {
	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

func (s *trackerTokenSource) Token(ctx context.Context) (string, error) {
	env := trackergithub.EnvTokenSource{EnvVars: []string{"AO_GITHUB_TOKEN"}}
	if tok, err := env.Token(ctx); err == nil {
		return tok, nil
	} else if !errors.Is(err, trackergithub.ErrNoToken) {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.token != "" && now.Before(s.expiresAt) {
		return s.token, nil
	}
	cmdCtx, cancel := context.WithTimeout(ctx, trackerTokenCommandTimeout)
	defer cancel()
	out, err := aoprocess.CommandContext(cmdCtx, "gh", "auth", "token").Output()
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(out))
	if token == "" {
		return "", trackergithub.ErrNoToken
	}
	s.token = token
	s.expiresAt = now.Add(trackerTokenCacheTTL)
	return token, nil
}
