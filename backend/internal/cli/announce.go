package cli

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

type announceOptions struct {
	message string
}

// announceAPIRequest mirrors the daemon's AnnounceRequest body for
// POST /api/v1/announce. The CLI keeps its own copy so it need not import
// httpd.
type announceAPIRequest struct {
	Text    string `json:"text"`
	Session string `json:"session,omitempty"`
}

func newAnnounceCommand(ctx *commandContext) *cobra.Command {
	var opts announceOptions
	cmd := &cobra.Command{
		Use:   "announce",
		Short: "Post a message to the operator chat",
		Long: "Post a message to the chat the daemon already notifies (Telegram).\n" +
			"The daemon owns the bot credentials; an agent only supplies the text.",
		Args: noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return ctx.announce(cmd.Context(), opts)
		},
	}
	cmd.Flags().StringVar(&opts.message, "message", "", "Message body (required)")
	return cmd
}

func (c *commandContext) announce(ctx context.Context, opts announceOptions) error {
	if strings.TrimSpace(opts.message) == "" {
		return usageError{errors.New("usage: --message is required")}
	}
	// The session travels as its own field rather than baked into the text:
	// the daemon labels the message with it, and uses it to find the message
	// this one is an answer to.
	session := strings.TrimSpace(os.Getenv("AO_SESSION_ID"))
	return c.postJSON(ctx, "announce", announceAPIRequest{Text: opts.message, Session: session}, nil)
}
