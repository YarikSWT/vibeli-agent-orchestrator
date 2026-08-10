// Package projectsync keeps a GitHub Projects v2 board in step with AO session
// state: the card moves as its session advances, and a card a human drags back
// to the ready column reclaims the work.
//
// The board is authoritative. AO never treats its own session table as the
// truth about what should be worked on — it only reports progress onto the
// board and obeys the board when the two disagree.
package projectsync

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	trackerprojects "github.com/aoagents/agent-orchestrator/backend/internal/adapters/tracker/githubprojects"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/observe"
)

const (
	// DefaultTickInterval matches intake's cadence: this loop is the write half
	// of the same board conversation, and polling faster only burns GraphQL quota.
	DefaultTickInterval = time.Minute
	// DefaultReclaimGrace is how long a fresh session is left alone before a
	// card still sitting in the ready column counts as a human reclaim rather
	// than as this loop not having caught up yet.
	DefaultReclaimGrace = 2 * time.Minute
)

// StatusMap names the board columns AO writes. An empty column name disables
// that transition, which is how a board without, say, a review column opts out
// instead of failing every tick.
type StatusMap struct {
	InProgress string
	InReview   string
	Done       string
}

// DefaultStatusMap matches the columns GitHub creates on a stock Projects v2
// board.
func DefaultStatusMap() StatusMap {
	return StatusMap{InProgress: "In progress", InReview: "In review", Done: "Done"}
}

// Board is the subset of the Projects v2 adapter this loop needs.
type Board interface {
	Status(ctx context.Context, id domain.IssueID) (string, error)
	SetStatus(ctx context.Context, id domain.IssueID, status string) error
}

// BoardResolver hands back the board for a project's intake config. It is an
// interface so the daemon can cache one adapter per board id and tests can
// inject a fake.
type BoardResolver interface {
	Resolve(cfg domain.TrackerIntakeConfig) (Board, error)
}

// Store is the durable read surface.
type Store interface {
	ListProjects(ctx context.Context) ([]domain.ProjectRecord, error)
	ListAllSessions(ctx context.Context) ([]domain.SessionRecord, error)
	ListPRsBySession(ctx context.Context, id domain.SessionID) ([]domain.PullRequest, error)
}

// Killer terminates a session whose card was reclaimed. Kill is expected to be
// idempotent: the loop may observe the reclaim again before the store settles.
type Killer interface {
	Kill(ctx context.Context, id domain.SessionID) error
}

// Config holds optional knobs. Zero values use production defaults.
type Config struct {
	Tick         time.Duration
	ReclaimGrace time.Duration
	Statuses     StatusMap
	Clock        func() time.Time
	Logger       *slog.Logger
}

// Observer pushes session state onto the board and honors human reclaims.
type Observer struct {
	resolver     BoardResolver
	store        Store
	killer       Killer
	tick         time.Duration
	reclaimGrace time.Duration
	statuses     StatusMap
	clock        func() time.Time
	logger       *slog.Logger

	// lastWritten suppresses repeat writes for a card AO already moved. The
	// board read still happens, so an externally changed column is noticed.
	lastWritten map[domain.IssueID]string
	// warnedMissing remembers columns the board does not have, so a board
	// without "In review" logs once instead of on every tick.
	warnedMissing map[string]bool
}

// New constructs an Observer with safe defaults.
func New(resolver BoardResolver, store Store, killer Killer, cfg Config) *Observer {
	o := &Observer{
		resolver:      resolver,
		store:         store,
		killer:        killer,
		tick:          cfg.Tick,
		reclaimGrace:  cfg.ReclaimGrace,
		statuses:      cfg.Statuses,
		clock:         cfg.Clock,
		logger:        cfg.Logger,
		lastWritten:   map[domain.IssueID]string{},
		warnedMissing: map[string]bool{},
	}
	if o.tick <= 0 {
		o.tick = DefaultTickInterval
	}
	if o.reclaimGrace <= 0 {
		o.reclaimGrace = DefaultReclaimGrace
	}
	if o.statuses == (StatusMap{}) {
		o.statuses = DefaultStatusMap()
	}
	if o.clock == nil {
		o.clock = time.Now
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	return o
}

// Start launches the observer loop.
func (o *Observer) Start(ctx context.Context) <-chan struct{} {
	return observe.StartPollLoop(ctx, o.tick, o.Poll, o.logger, "project status sync")
}

// Poll runs one synchronous sync pass. Store failures abort the pass (the world
// is unknown); per-project and per-session failures are logged and skipped so
// one unreachable board cannot stall the daemon.
func (o *Observer) Poll(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.resolver == nil || o.store == nil {
		return nil
	}
	projects, err := o.store.ListProjects(ctx)
	if err != nil {
		return err
	}
	boards := map[domain.ProjectID]Board{}
	for _, project := range projects {
		cfg := project.Config.TrackerIntake.WithDefaults()
		if !cfg.Enabled || cfg.Provider != domain.TrackerProviderGitHubProjects {
			continue
		}
		board, err := o.resolver.Resolve(cfg)
		if err != nil {
			o.logger.Warn("project sync: no board for project", "project", project.ID, "err", err)
			continue
		}
		boards[domain.ProjectID(project.ID)] = board
	}
	if len(boards) == 0 {
		return nil
	}
	sessions, err := o.store.ListAllSessions(ctx)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if err := ctx.Err(); err != nil {
			return err
		}
		board, ok := boards[session.ProjectID]
		if !ok || session.IssueID == "" {
			continue
		}
		o.syncSession(ctx, board, session)
	}
	return nil
}

// syncSession reconciles one session against its card.
func (o *Observer) syncSession(ctx context.Context, board Board, session domain.SessionRecord) {
	issueID := session.IssueID
	current, err := board.Status(ctx, issueID)
	if err != nil {
		if !errors.Is(err, trackerprojects.ErrNotFound) {
			o.logger.Warn("project sync: read card status failed", "session", session.ID, "issue", issueID, "err", err)
		}
		return
	}
	prs, err := o.store.ListPRsBySession(ctx, session.ID)
	if err != nil {
		o.logger.Warn("project sync: read session PRs failed", "session", session.ID, "err", err)
		return
	}

	// The board wins: a live session whose card was dragged back to the ready
	// column is work a human took back. Kill it and let intake re-claim from a
	// clean workspace on the next sweep.
	if o.isReclaim(current, session) {
		if o.killer == nil {
			o.logger.Warn("project sync: card reclaimed but no killer wired", "session", session.ID, "issue", issueID)
			return
		}
		o.logger.Info("project sync: card reclaimed, killing session", "session", session.ID, "issue", issueID, "status", current)
		if err := o.killer.Kill(ctx, session.ID); err != nil {
			o.logger.Error("project sync: kill reclaimed session failed", "session", session.ID, "err", err)
			return
		}
		delete(o.lastWritten, issueID)
		return
	}

	want := o.desiredStatus(session, prs)
	if want == "" || strings.EqualFold(want, current) {
		return
	}
	if err := board.SetStatus(ctx, issueID, want); err != nil {
		if errors.Is(err, trackerprojects.ErrNoSuchStatus) {
			if !o.warnedMissing[want] {
				o.warnedMissing[want] = true
				o.logger.Warn("project sync: board has no such column, skipping this transition from now on", "status", want)
			}
			return
		}
		o.logger.Error("project sync: set card status failed", "session", session.ID, "issue", issueID, "status", want, "err", err)
		return
	}
	o.lastWritten[issueID] = want
	o.logger.Info("project sync: card moved", "session", session.ID, "issue", issueID, "from", current, "to", want)
}

// isReclaim reports whether a card sitting in the ready column means a human
// took the work back. A session younger than the grace window is exempt: right
// after a spawn the card legitimately still reads "Ready" because this loop has
// not moved it yet.
func (o *Observer) isReclaim(current string, session domain.SessionRecord) bool {
	if session.IsTerminated {
		return false
	}
	if !strings.EqualFold(current, domain.DefaultProjectReadyStatus) {
		return false
	}
	return o.clock().UTC().Sub(session.CreatedAt.UTC()) >= o.reclaimGrace
}

// desiredStatus maps session + PR facts onto a board column. An empty result
// means "leave the card alone", which is the right answer for a finished
// session whose PR a human has not merged yet: the card is theirs to move.
func (o *Observer) desiredStatus(session domain.SessionRecord, prs []domain.PullRequest) string {
	var hasOpen, hasMerged bool
	for _, pr := range prs {
		switch {
		case pr.Merged:
			hasMerged = true
		case !pr.Closed:
			hasOpen = true
		}
	}
	switch {
	case hasMerged && !hasOpen:
		return o.statuses.Done
	case hasOpen:
		return o.statuses.InReview
	case !session.IsTerminated:
		return o.statuses.InProgress
	default:
		return ""
	}
}
