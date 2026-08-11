package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// EnsurePRBodyBlock appends block to the pull request description unless marker
// is already there. It reports whether it wrote anything.
//
// The marker, not the block text, is the idempotency key: the block carries a
// session id and a URL that can legitimately change between daemon versions,
// while the marker is a stable HTML comment invisible in rendered Markdown.
func (p *Provider) EnsurePRBodyBlock(ctx context.Context, ref ports.SCMPRRef, marker, block string) (bool, error) {
	path := repoPath(ref.Repo.Owner, ref.Repo.Name, "pulls", strconv.Itoa(ref.Number))

	resp, err := p.client.doREST(ctx, http.MethodGet, path, nil, nil)
	if err != nil {
		return false, fmt.Errorf("read pull request body: %w", err)
	}
	var pull struct {
		Body string `json:"body"`
	}
	if err := json.Unmarshal(resp.Body, &pull); err != nil {
		return false, fmt.Errorf("parse pull request body: %w", err)
	}
	if strings.Contains(pull.Body, marker) {
		return false, nil
	}

	body := strings.TrimRight(pull.Body, "\n")
	if body != "" {
		body += "\n\n"
	}
	body += marker + "\n" + block

	if _, err := p.client.doREST(ctx, http.MethodPatch, path, nil, map[string]string{"body": body}); err != nil {
		return false, fmt.Errorf("update pull request body: %w", err)
	}
	return true, nil
}
