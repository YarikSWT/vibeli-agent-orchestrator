package trackerintake

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// QueuedIssue is one card waiting in the ready column, in the order intake
// would claim it.
type QueuedIssue struct {
	ProjectID string
	Issue     string // native id, "owner/repo#123"
	Title     string
}

// ClaimResult reports what a manual claim started.
type ClaimResult struct {
	SessionID string
	ProjectID string
	Issue     string
	Title     string
}

// Queue lists the cards intake would claim next, per project, in provider
// order. It is the read half of "let a human see and steer the backlog"; the
// automatic loop consumes the same list from the top.
func (o *Observer) Queue(ctx context.Context) ([]QueuedIssue, error) {
	projects, err := o.intakeProjects(ctx)
	if err != nil {
		return nil, err
	}
	var queued []QueuedIssue
	for _, project := range projects {
		cfg := project.Config.TrackerIntake.WithDefaults()
		tracker, repo, err := o.trackerFor(project, cfg)
		if err != nil {
			o.logger.Warn("tracker intake: queue skipped a project", "project", project.ID, "err", err)
			continue
		}
		issues, err := tracker.List(ctx, repo, domain.ListFilter{State: domain.ListOpen, Assignee: cfg.Assignee})
		if err != nil {
			o.logger.Warn("tracker intake: queue list failed", "project", project.ID, "err", err)
			continue
		}
		for _, issue := range issues {
			if issue.State != domain.IssueOpen || !issueMatchesConfig(issue, cfg) {
				continue
			}
			queued = append(queued, QueuedIssue{ProjectID: project.ID, Issue: issue.ID.Native, Title: issue.Title})
		}
	}
	return queued, nil
}

// Claim starts a session for one specific issue, bypassing both the queue order
// and the concurrency cap: a human naming a card outranks the backlog sweep.
// The pause gate is likewise ignored — /pause stops the automatic loop, not the
// operator.
//
// ref accepts "123", "#123" or "owner/repo#123". The short forms resolve against
// the only intake-enabled project; with several such projects the full form is
// required, since a bare number is ambiguous.
func (o *Observer) Claim(ctx context.Context, ref string) (ClaimResult, error) {
	projects, err := o.intakeProjects(ctx)
	if err != nil {
		return ClaimResult{}, err
	}
	if len(projects) == 0 {
		return ClaimResult{}, fmt.Errorf("no project has tracker intake enabled")
	}
	project, native, err := resolveRef(projects, ref)
	if err != nil {
		return ClaimResult{}, err
	}
	sessions, err := o.store.ListAllSessions(ctx)
	if err != nil {
		return ClaimResult{}, err
	}
	for _, session := range sessions {
		if !session.IsTerminated && sameIssue(session.IssueID, native) {
			return ClaimResult{}, fmt.Errorf("%s is already being worked on by session %s", native, session.ID)
		}
	}
	cfg := project.Config.TrackerIntake.WithDefaults()
	tracker, _, err := o.trackerFor(project, cfg)
	if err != nil {
		return ClaimResult{}, err
	}
	issue, err := tracker.Get(ctx, domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: native})
	if err != nil {
		return ClaimResult{}, fmt.Errorf("read %s: %w", native, err)
	}
	// An adapter that answers with a blank issue found nothing. Spawning on that
	// would start an agent with no task in its prompt.
	if strings.TrimSpace(issue.ID.Native) == "" {
		return ClaimResult{}, fmt.Errorf("%s not found", native)
	}
	session, _, _, err := o.spawner.Spawn(ctx, ports.SpawnConfig{
		ProjectID: domain.ProjectID(project.ID),
		IssueID:   CanonicalIssueID(issue.ID),
		Kind:      domain.KindWorker,
		Prompt:    BuildIssuePrompt(issue),
	})
	if err != nil {
		return ClaimResult{}, fmt.Errorf("spawn %s: %w", native, err)
	}
	o.logger.Info("tracker intake: manual claim", "project", project.ID, "issue", native, "session", session.ID)
	return ClaimResult{
		SessionID: string(session.ID),
		ProjectID: project.ID,
		Issue:     native,
		Title:     issue.Title,
	}, nil
}

// intakeProjects returns the projects with intake enabled and a valid config.
func (o *Observer) intakeProjects(ctx context.Context) ([]domain.ProjectRecord, error) {
	if o.store == nil || o.resolver == nil || o.spawner == nil {
		return nil, fmt.Errorf("tracker intake is not wired")
	}
	all, err := o.store.ListProjects(ctx)
	if err != nil {
		return nil, err
	}
	enabled := make([]domain.ProjectRecord, 0, len(all))
	for _, project := range all {
		cfg := project.Config.TrackerIntake.WithDefaults()
		if !cfg.Enabled {
			continue
		}
		if err := cfg.Validate(); err != nil {
			o.logger.Warn("tracker intake: skipping project with invalid config", "project", project.ID, "err", err)
			continue
		}
		enabled = append(enabled, project)
	}
	return enabled, nil
}

// trackerFor resolves a project's adapter and repository scope in one step,
// since every caller needs both together.
func (o *Observer) trackerFor(project domain.ProjectRecord, cfg domain.TrackerIntakeConfig) (ports.Tracker, domain.TrackerRepo, error) {
	repo, ok := trackerRepo(project, cfg)
	if !ok {
		return nil, domain.TrackerRepo{}, fmt.Errorf("project %s has no tracker scope", project.ID)
	}
	tracker, err := o.resolver.Resolve(cfg)
	if err != nil {
		return nil, domain.TrackerRepo{}, err
	}
	return tracker, repo, nil
}

// resolveRef turns a user-typed reference into a project plus a native issue id.
func resolveRef(projects []domain.ProjectRecord, ref string) (domain.ProjectRecord, string, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(ref), "#")
	if trimmed == "" {
		return domain.ProjectRecord{}, "", fmt.Errorf("issue reference is required, e.g. 53 or owner/repo#53")
	}
	if strings.Contains(trimmed, "/") {
		owner, _, ok := strings.Cut(trimmed, "#")
		if !ok || owner == "" {
			return domain.ProjectRecord{}, "", fmt.Errorf("%q is not an issue reference; want owner/repo#123", ref)
		}
		for _, project := range projects {
			repo, ok := trackerRepo(project, project.Config.TrackerIntake.WithDefaults())
			if ok && strings.EqualFold(repo.Native, owner) {
				return project, trimmed, nil
			}
		}
		return domain.ProjectRecord{}, "", fmt.Errorf("no intake-enabled project owns %s", owner)
	}
	if _, err := strconv.Atoi(trimmed); err != nil {
		return domain.ProjectRecord{}, "", fmt.Errorf("%q is not an issue number; want 53 or owner/repo#53", ref)
	}
	if len(projects) != 1 {
		return domain.ProjectRecord{}, "", fmt.Errorf("several projects have intake enabled; use owner/repo#%s", trimmed)
	}
	project := projects[0]
	repo, ok := trackerRepo(project, project.Config.TrackerIntake.WithDefaults())
	if !ok {
		return domain.ProjectRecord{}, "", fmt.Errorf("project %s has no tracker scope", project.ID)
	}
	return project, repo.Native + "#" + trimmed, nil
}

// sameIssue compares a stored session issue id against a native id. Stored ids
// come in two spellings — canonical "github:owner/repo#1" from intake and the
// bare form from `ao spawn --issue` — so both are folded before comparing.
func sameIssue(stored domain.IssueID, native string) bool {
	return strings.EqualFold(stripProvider(string(stored)), strings.TrimSpace(native))
}

func stripProvider(id string) string {
	trimmed := strings.TrimSpace(id)
	if _, rest, ok := strings.Cut(trimmed, ":"); ok {
		return rest
	}
	return trimmed
}
