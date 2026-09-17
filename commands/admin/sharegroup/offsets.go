package sharegroup

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func offsetDeleteCommand(cl *client.Client) *cobra.Command {
	var topicFlags []string
	cmd := &cobra.Command{
		Use:   "offset-delete GROUP",
		Short: "Delete share group offsets for topics (Kafka 4.0+).",
		Long: `Delete share group offsets for topics (Kafka 4.0+).

Delete share group offsets for topics (KIP-932, Kafka 4.0+).

The group must be empty (no active consumers). This deletes all offset state
for the specified topics within the share group.

EXAMPLES:
  kcl share-group offset-delete mygroup -t foo -t bar

SEE ALSO:
  kcl share-group describe    describe share groups with lag
  kcl share-group seek        reset share group start offsets
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			group := args[0]
			// Positional topics after the group are the old form, kept
			// working but out of the help.
			topics := append(topicFlags, args[1:]...)
			if len(topics) == 0 {
				return out.Errf(out.ExitUsage, "at least one topic is required (-t)")
			}

			req := kmsg.NewPtrDeleteShareGroupOffsetsRequest()
			req.GroupID = group
			for _, t := range topics {
				rt := kmsg.NewDeleteShareGroupOffsetsRequestTopic()
				rt.Topic = t
				req.Topics = append(req.Topics, rt)
			}

			kresp, err := req.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to delete share group offsets: %v", err)
			}

			if err := kerr.ErrorForCode(kresp.ErrorCode); err != nil {
				msg := err.Error()
				if kresp.ErrorMessage != nil {
					msg += ": " + *kresp.ErrorMessage
				}
				return fmt.Errorf("%s", msg)
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results",
				"TOPIC", "ERROR", "MESSAGE").ResultColumns()
			sort.Slice(kresp.Topics, func(i, j int) bool { return kresp.Topics[i].Topic < kresp.Topics[j].Topic })
			for _, topic := range kresp.Topics {
				errStr, message := "", ""
				if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
					errStr = err.Error()
					if topic.ErrorMessage != nil {
						message = *topic.ErrorMessage
					}
				}
				table.Row(topic.Topic, errStr, message)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, "TOPIC", "ERROR", "MESSAGE")
	cmd.Flags().StringArrayVarP(&topicFlags, "topic", "t", nil, "topic to delete offsets for; repeatable")
	return cmd
}
