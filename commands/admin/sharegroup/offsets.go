package sharegroup

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func offsetDeleteCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "offset-delete GROUP TOPICS...",
		Short: "Delete share group offsets for topics (Kafka 4.0+).",
		Long: `Delete share group offsets for topics (KIP-932, Kafka 4.0+).

The group must be empty (no active consumers). This deletes all offset state
for the specified topics within the share group.
`,
		Example: "offset-delete mygroup foo bar",
		Args:    cobra.MinimumNArgs(2),
		Run: func(_ *cobra.Command, args []string) {
			group := args[0]
			topics := args[1:]

			req := kmsg.NewPtrDeleteShareGroupOffsetsRequest()
			req.GroupID = group
			for _, t := range topics {
				rt := kmsg.NewDeleteShareGroupOffsetsRequestTopic()
				rt.Topic = t
				req.Topics = append(req.Topics, rt)
			}

			kresp, err := req.RequestWith(context.Background(), cl.Client())
			out.MaybeDie(err, "unable to delete share group offsets: %v", err)

			if err := kerr.ErrorForCode(kresp.ErrorCode); err != nil {
				msg := err.Error()
				if kresp.ErrorMessage != nil {
					msg += ": " + *kresp.ErrorMessage
				}
				out.Die(msg)
			}

			table := out.NewFormattedTable(cl.Format(), "share-group.offset-delete", 1, "results",
				"TOPIC", "STATUS")
			for _, topic := range kresp.Topics {
				errMsg := "OK"
				if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
					errMsg = err.Error()
					if topic.ErrorMessage != nil {
						errMsg += ": " + *topic.ErrorMessage
					}
				}
				table.Row(topic.Topic, errMsg)
			}
			table.Flush()
		},
	}
	return cmd
}
