package notify

import (
	"context"
	"errors"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// Fanout delivers each notification event to several publishers — the live
// dashboard hub plus any side-channels a deployment wired up (chat, push).
//
// Every publisher is attempted even when an earlier one fails: a chat outage
// must not cost the dashboard its event. The errors are joined so the caller
// still sees what broke.
type Fanout []Publisher

// NewFanout returns the plain publisher when only one is given, so the common
// single-publisher case carries no wrapper.
func NewFanout(publishers ...Publisher) Publisher {
	live := make(Fanout, 0, len(publishers))
	for _, publisher := range publishers {
		if publisher != nil {
			live = append(live, publisher)
		}
	}
	switch len(live) {
	case 0:
		return nil
	case 1:
		return live[0]
	default:
		return live
	}
}

// Publish fans the event out to every publisher.
func (f Fanout) Publish(ctx context.Context, event domain.NotificationEvent) error {
	var errs []error
	for _, publisher := range f {
		if err := publisher.Publish(ctx, event); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
