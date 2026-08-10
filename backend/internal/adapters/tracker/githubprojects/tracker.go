// Package githubprojects implements ports.Tracker over a GitHub Projects v2
// board. Where the plain github tracker treats the repository issue list as the
// queue, this adapter treats one Status column as the queue: intake claims the
// cards sitting in "Ready", and the board — not AO's local state — stays the
// source of truth for what should be worked on.
//
// It also exposes the write side (SetStatus) that a status-sync observer uses
// to move a card as its session advances. That surface is deliberately outside
// ports.Tracker: no other provider has a board to write back to.
//
// Issue identity is unchanged from the github adapter — "owner/repo#123" — so
// every consumer downstream of intake (PR linking, session metadata, Get) keeps
// working against the same ids.
package githubprojects

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	trackergithub "github.com/aoagents/agent-orchestrator/backend/internal/adapters/tracker/github"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const (
	defaultGraphQLURL = "https://api.github.com/graphql"
	defaultUserAgent  = "ao-agent-orchestrator/tracker-github-projects"
	// defaultStatusField is the single-select field Projects v2 creates on every
	// new board. A board that renamed it must say so in Options.StatusField.
	defaultStatusField = "Status"
	// itemPageSize is GraphQL's max page size for project items.
	itemPageSize = 100
	// maxItemPages guards against a pathological cursor cycle: 100 pages at the
	// max page size covers a 10k-card board before failing loud.
	maxItemPages = 100
)

// Sentinel errors callers match with errors.Is rather than parsing GraphQL
// payloads.
var (
	ErrNotFound      = errors.New("github projects tracker: item not found")
	ErrAuthFailed    = errors.New("github projects tracker: authentication failed")
	ErrWrongProvider = errors.New("github projects tracker: id is not a github tracker id")
	ErrNoStatusField = errors.New("github projects tracker: board has no single-select status field")
	ErrNoSuchStatus  = errors.New("github projects tracker: board has no such status option")
)

// Options configures a Tracker. Token and ProjectID are required; the rest have
// production defaults and exist so tests can point at an httptest server.
type Options struct {
	// Token yields the GitHub bearer token. The token needs the `project`
	// scope on top of `repo` — reading a board is not covered by `repo` alone.
	Token trackergithub.TokenSource
	// ProjectID is the board's node id ("PVT_..."). A node id rather than
	// owner+number: it resolves the same for user- and org-owned boards.
	ProjectID string
	// ReadyStatus is the column List claims from. Empty means
	// domain.DefaultProjectReadyStatus.
	ReadyStatus string
	// StatusField names the single-select field holding the workflow column.
	// Empty means "Status".
	StatusField string
	// Issues, when set, serves Get. Board cards carry enough data for intake,
	// but Get is also called for issues that never sat on the board, so it
	// delegates to the repository-level tracker.
	Issues ports.Tracker

	HTTPClient *http.Client
	GraphQLURL string
	UserAgent  string
}

// Tracker reads a Projects v2 board through the GraphQL API.
type Tracker struct {
	http        *http.Client
	tokens      trackergithub.TokenSource
	graphqlURL  string
	userAgent   string
	projectID   string
	readyStatus string
	statusField string
	issues      ports.Tracker

	// itemMu guards the issue -> card mappings learned from every board read.
	// The board is polled on a timer, so the map is refreshed continuously;
	// it is bounded by the board size. Keys are normalized by issueKey.
	itemMu    sync.Mutex
	itemByID  map[string]string
	statusRef *statusFieldRef
}

// statusFieldRef caches the board's Status field id and its option ids. Both are
// stable for the lifetime of a board (renaming an option mints a new id, which
// surfaces as ErrNoSuchStatus and is re-read on the next call).
type statusFieldRef struct {
	fieldID string
	options map[string]string // lower-cased option name -> option id
}

// New validates the options and returns a ready adapter. It performs no network
// call, so daemon startup is never blocked on GitHub being reachable.
func New(opts Options) (*Tracker, error) {
	if opts.Token == nil {
		return nil, trackergithub.ErrNoToken
	}
	if strings.TrimSpace(opts.ProjectID) == "" {
		return nil, errors.New("github projects tracker: ProjectID is required")
	}
	t := &Tracker{
		http:        opts.HTTPClient,
		tokens:      opts.Token,
		graphqlURL:  strings.TrimSpace(opts.GraphQLURL),
		userAgent:   strings.TrimSpace(opts.UserAgent),
		projectID:   strings.TrimSpace(opts.ProjectID),
		readyStatus: strings.TrimSpace(opts.ReadyStatus),
		statusField: strings.TrimSpace(opts.StatusField),
		issues:      opts.Issues,
		itemByID:    map[string]string{},
	}
	if t.http == nil {
		t.http = &http.Client{Timeout: 30 * time.Second}
	}
	if t.graphqlURL == "" {
		t.graphqlURL = defaultGraphQLURL
	}
	if t.userAgent == "" {
		t.userAgent = defaultUserAgent
	}
	if t.readyStatus == "" {
		t.readyStatus = domain.DefaultProjectReadyStatus
	}
	if t.statusField == "" {
		t.statusField = defaultStatusField
	}
	return t, nil
}

var _ ports.Tracker = (*Tracker)(nil)

// Get delegates to the repository-level tracker: an issue's title, body and
// state live on the issue, not on the card. Without a delegate it reports
// ErrNotFound rather than inventing a projection from stale board data.
func (t *Tracker) Get(ctx context.Context, id domain.TrackerID) (domain.Issue, error) {
	if t.issues == nil {
		return domain.Issue{}, fmt.Errorf("%w: no issue tracker configured for Get", ErrNotFound)
	}
	return t.issues.Get(ctx, id)
}

// Preflight verifies the token can read the configured board. A board that
// cannot be read is a configuration error worth surfacing at startup rather
// than as a silent empty intake sweep.
func (t *Tracker) Preflight(ctx context.Context) error {
	_, err := t.loadStatusField(ctx)
	return err
}

// List returns the issues whose card sits in the ready column. The repo
// argument scopes the result to one repository, so a board spanning several
// repos only feeds each AO project its own cards; an empty repo means "every
// repository on the board".
//
// ListFilter.State/Labels/Assignee still apply on top of the column filter, so
// a project can additionally require, say, a label. filter.Limit caps results.
func (t *Tracker) List(ctx context.Context, repo domain.TrackerRepo, filter domain.ListFilter) ([]domain.Issue, error) {
	if repo.Native != "" && repo.Provider != domain.TrackerProviderGitHub && repo.Provider != domain.TrackerProviderGitHubProjects {
		return nil, fmt.Errorf("%w: provider=%q", ErrWrongProvider, repo.Provider)
	}
	cards, err := t.listCards(ctx)
	if err != nil {
		return nil, err
	}
	wantRepo := strings.TrimSpace(repo.Native)
	out := make([]domain.Issue, 0, len(cards))
	for _, card := range cards {
		if !strings.EqualFold(card.status, t.readyStatus) {
			continue
		}
		if wantRepo != "" && !strings.EqualFold(card.repo, wantRepo) {
			continue
		}
		if !matchesFilter(card.issue, filter) {
			continue
		}
		out = append(out, card.issue)
		if filter.Limit > 0 && len(out) >= filter.Limit {
			break
		}
	}
	return out, nil
}

// Status returns the board column an issue currently sits in. ErrNotFound means
// the issue has no card on this board.
func (t *Tracker) Status(ctx context.Context, id domain.IssueID) (string, error) {
	cards, err := t.listCards(ctx)
	if err != nil {
		return "", err
	}
	want := issueKey(id)
	for _, card := range cards {
		if issueKey(card.issueID) == want {
			return card.status, nil
		}
	}
	return "", fmt.Errorf("%w: %s", ErrNotFound, id)
}

// SetStatus moves an issue's card to the named column. It is a no-op when the
// card already sits there, so a status-sync loop can call it on every tick
// without burning GraphQL quota.
func (t *Tracker) SetStatus(ctx context.Context, id domain.IssueID, status string) error {
	want := strings.TrimSpace(status)
	if want == "" {
		return fmt.Errorf("%w: empty status", ErrNoSuchStatus)
	}
	current, err := t.Status(ctx, id)
	if err != nil {
		return err
	}
	if strings.EqualFold(current, want) {
		return nil
	}
	itemID, ok := t.lookupItem(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	ref, err := t.loadStatusField(ctx)
	if err != nil {
		return err
	}
	optionID, ok := ref.options[strings.ToLower(want)]
	if !ok {
		// The board may have gained the column since the ref was cached.
		t.invalidateStatusField()
		if ref, err = t.loadStatusField(ctx); err != nil {
			return err
		}
		if optionID, ok = ref.options[strings.ToLower(want)]; !ok {
			return fmt.Errorf("%w: %q", ErrNoSuchStatus, want)
		}
	}
	const mutation = `mutation($project:ID!,$item:ID!,$field:ID!,$option:String!){
  updateProjectV2ItemFieldValue(input:{projectId:$project,itemId:$item,fieldId:$field,value:{singleSelectOptionId:$option}}){
    projectV2Item{ id }
  }
}`
	var resp struct {
		UpdateProjectV2ItemFieldValue struct {
			ProjectV2Item struct {
				ID string `json:"id"`
			} `json:"projectV2Item"`
		} `json:"updateProjectV2ItemFieldValue"`
	}
	return t.do(ctx, mutation, map[string]any{
		"project": t.projectID,
		"item":    itemID,
		"field":   ref.fieldID,
		"option":  optionID,
	}, &resp)
}

// ---------------------------------------------------------------------------
// Board reads
// ---------------------------------------------------------------------------

// card is one board item projected onto what AO needs: the issue itself, the
// column it sits in, and the repository that owns it.
type card struct {
	issue   domain.Issue
	issueID domain.IssueID
	itemID  string
	status  string
	repo    string
}

const itemsQuery = `query($project:ID!,$field:String!,$cursor:String){
  node(id:$project){
    ... on ProjectV2 {
      items(first:%d, after:$cursor){
        pageInfo{ hasNextPage endCursor }
        nodes{
          id
          fieldValueByName(name:$field){
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          content{
            ... on Issue {
              number
              title
              body
              url
              state
              stateReason
              repository{ nameWithOwner }
              labels(first:50){ nodes{ name } }
              assignees(first:20){ nodes{ login } }
            }
          }
        }
      }
    }
  }
}`

type itemsResponse struct {
	Node struct {
		Items struct {
			PageInfo struct {
				HasNextPage bool   `json:"hasNextPage"`
				EndCursor   string `json:"endCursor"`
			} `json:"pageInfo"`
			Nodes []struct {
				ID               string `json:"id"`
				FieldValueByName *struct {
					Name string `json:"name"`
				} `json:"fieldValueByName"`
				Content *struct {
					Number      int    `json:"number"`
					Title       string `json:"title"`
					Body        string `json:"body"`
					URL         string `json:"url"`
					State       string `json:"state"`
					StateReason string `json:"stateReason"`
					Repository  struct {
						NameWithOwner string `json:"nameWithOwner"`
					} `json:"repository"`
					Labels struct {
						Nodes []struct {
							Name string `json:"name"`
						} `json:"nodes"`
					} `json:"labels"`
					Assignees struct {
						Nodes []struct {
							Login string `json:"login"`
						} `json:"nodes"`
					} `json:"assignees"`
				} `json:"content"`
			} `json:"nodes"`
		} `json:"items"`
	} `json:"node"`
}

// listCards walks every page of the board and refreshes the issue -> card index
// as a side effect, so SetStatus can address a card without a second lookup.
//
// Cards whose content is not an issue (draft notes, pull requests) are skipped:
// AO has nothing to spawn for them.
func (t *Tracker) listCards(ctx context.Context) ([]card, error) {
	query := fmt.Sprintf(itemsQuery, itemPageSize)
	var (
		cursor string
		cards  []card
	)
	for page := 0; page < maxItemPages; page++ {
		vars := map[string]any{"project": t.projectID, "field": t.statusField}
		if cursor != "" {
			vars["cursor"] = cursor
		}
		var resp itemsResponse
		if err := t.do(ctx, query, vars, &resp); err != nil {
			return nil, err
		}
		for _, node := range resp.Node.Items.Nodes {
			if node.Content == nil || node.Content.Number == 0 {
				continue
			}
			repo := strings.TrimSpace(node.Content.Repository.NameWithOwner)
			if repo == "" {
				continue
			}
			native := fmt.Sprintf("%s#%d", repo, node.Content.Number)
			// Canonical issue ids carry the provider prefix (see
			// trackerintake.CanonicalIssueID). Cards report the github
			// provider, not github-projects: the identity of an issue does not
			// change because it happens to sit on a board.
			issueID := domain.IssueID(string(domain.TrackerProviderGitHub) + ":" + native)
			labels := make([]string, 0, len(node.Content.Labels.Nodes))
			for _, l := range node.Content.Labels.Nodes {
				labels = append(labels, l.Name)
			}
			assignees := make([]string, 0, len(node.Content.Assignees.Nodes))
			for _, a := range node.Content.Assignees.Nodes {
				assignees = append(assignees, a.Login)
			}
			status := ""
			if node.FieldValueByName != nil {
				status = node.FieldValueByName.Name
			}
			cards = append(cards, card{
				issue: domain.Issue{
					ID:        domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: native},
					Title:     node.Content.Title,
					Body:      node.Content.Body,
					State:     mapIssueState(node.Content.State, node.Content.StateReason),
					URL:       node.Content.URL,
					Labels:    labels,
					Assignees: assignees,
				},
				issueID: issueID,
				itemID:  node.ID,
				status:  status,
				repo:    repo,
			})
		}
		if !resp.Node.Items.PageInfo.HasNextPage || resp.Node.Items.PageInfo.EndCursor == "" {
			break
		}
		cursor = resp.Node.Items.PageInfo.EndCursor
	}
	t.rememberItems(cards)
	return cards, nil
}

func (t *Tracker) rememberItems(cards []card) {
	t.itemMu.Lock()
	defer t.itemMu.Unlock()
	for _, c := range cards {
		t.itemByID[issueKey(c.issueID)] = c.itemID
	}
}

func (t *Tracker) lookupItem(id domain.IssueID) (string, bool) {
	t.itemMu.Lock()
	defer t.itemMu.Unlock()
	itemID, ok := t.itemByID[issueKey(id)]
	return itemID, ok
}

// issueKey normalizes an issue id for comparison. Ids reach this adapter in two
// spellings: intake stores the canonical "github:owner/repo#123", while a
// session spawned from the CLI carries the bare "owner/repo#123" the operator
// typed. Both name the same issue, so the provider prefix is dropped and the
// remainder is folded to lower case (GitHub owner/repo are case-insensitive).
func issueKey(id domain.IssueID) string {
	native := strings.TrimSpace(string(id))
	if _, rest, ok := strings.Cut(native, ":"); ok {
		native = rest
	}
	return strings.ToLower(native)
}

const statusFieldQuery = `query($project:ID!,$field:String!){
  node(id:$project){
    ... on ProjectV2 {
      field(name:$field){
        ... on ProjectV2SingleSelectField {
          id
          options{ id name }
        }
      }
    }
  }
}`

// loadStatusField resolves and caches the board's Status field id plus its
// option ids. Both are needed to write a column, and neither changes while a
// board keeps its columns.
func (t *Tracker) loadStatusField(ctx context.Context) (*statusFieldRef, error) {
	t.itemMu.Lock()
	cached := t.statusRef
	t.itemMu.Unlock()
	if cached != nil {
		return cached, nil
	}
	var resp struct {
		Node struct {
			Field *struct {
				ID      string `json:"id"`
				Options []struct {
					ID   string `json:"id"`
					Name string `json:"name"`
				} `json:"options"`
			} `json:"field"`
		} `json:"node"`
	}
	if err := t.do(ctx, statusFieldQuery, map[string]any{"project": t.projectID, "field": t.statusField}, &resp); err != nil {
		return nil, err
	}
	if resp.Node.Field == nil || resp.Node.Field.ID == "" {
		return nil, fmt.Errorf("%w: field %q on project %s", ErrNoStatusField, t.statusField, t.projectID)
	}
	ref := &statusFieldRef{fieldID: resp.Node.Field.ID, options: make(map[string]string, len(resp.Node.Field.Options))}
	for _, opt := range resp.Node.Field.Options {
		ref.options[strings.ToLower(opt.Name)] = opt.ID
	}
	t.itemMu.Lock()
	t.statusRef = ref
	t.itemMu.Unlock()
	return ref, nil
}

func (t *Tracker) invalidateStatusField() {
	t.itemMu.Lock()
	t.statusRef = nil
	t.itemMu.Unlock()
}

// ---------------------------------------------------------------------------
// GraphQL transport
// ---------------------------------------------------------------------------

// do executes one GraphQL call and decodes data into out. GraphQL reports
// application errors with HTTP 200 and an "errors" array, so the body is
// inspected even on success.
func (t *Tracker) do(ctx context.Context, query string, vars map[string]any, out any) error {
	token, err := t.tokens.Token(ctx)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{"query": query, "variables": vars})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.graphqlURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", t.userAgent)

	resp, err := t.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("%w: %s", ErrAuthFailed, strings.TrimSpace(string(body)))
	default:
		return fmt.Errorf("github projects tracker: graphql http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("github projects tracker: decode graphql response: %w", err)
	}
	if len(envelope.Errors) > 0 {
		messages := make([]string, 0, len(envelope.Errors))
		for _, e := range envelope.Errors {
			messages = append(messages, e.Message)
			if strings.EqualFold(e.Type, "FORBIDDEN") || strings.EqualFold(e.Type, "UNAUTHORIZED") {
				return fmt.Errorf("%w: %s", ErrAuthFailed, e.Message)
			}
		}
		return fmt.Errorf("github projects tracker: graphql: %s", strings.Join(messages, "; "))
	}
	if out == nil || len(envelope.Data) == 0 {
		return nil
	}
	return json.Unmarshal(envelope.Data, out)
}

// ---------------------------------------------------------------------------
// Projections
// ---------------------------------------------------------------------------

// mapIssueState mirrors the github adapter's mapping for the GraphQL state
// vocabulary (OPEN/CLOSED + NOT_PLANNED). Board columns are not folded into the
// issue state: the column is workflow position, the state is the issue's own.
func mapIssueState(state, reason string) domain.NormalizedIssueState {
	if !strings.EqualFold(state, "CLOSED") {
		return domain.IssueOpen
	}
	if strings.EqualFold(reason, "NOT_PLANNED") {
		return domain.IssueCancelled
	}
	return domain.IssueDone
}

// matchesFilter applies the non-column dimensions of a ListFilter. State is
// coarse (open/closed) exactly as ListStateFilter defines it.
func matchesFilter(issue domain.Issue, filter domain.ListFilter) bool {
	switch filter.State {
	case domain.ListOpen:
		if issue.State != domain.IssueOpen && issue.State != domain.IssueInProgress && issue.State != domain.IssueInReview {
			return false
		}
	case domain.ListClosed:
		if issue.State != domain.IssueDone && issue.State != domain.IssueCancelled {
			return false
		}
	}
	for _, want := range filter.Labels {
		if !containsFold(issue.Labels, want) {
			return false
		}
	}
	if assignee := strings.TrimSpace(filter.Assignee); assignee != "" {
		switch {
		case assignee == "*":
			if len(issue.Assignees) == 0 {
				return false
			}
		case strings.EqualFold(assignee, "none"):
			if len(issue.Assignees) > 0 {
				return false
			}
		default:
			if !containsFold(issue.Assignees, assignee) {
				return false
			}
		}
	}
	return true
}

func containsFold(values []string, needle string) bool {
	for _, v := range values {
		if strings.EqualFold(strings.TrimSpace(v), strings.TrimSpace(needle)) {
			return true
		}
	}
	return false
}
