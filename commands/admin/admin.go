// Package admin contains admin commands.
package admin

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Admin utility commands.",
	}

	cmd.AddCommand(electLeaderCommand(cl))
	cmd.AddCommand(deleteRecordsCommand(cl))
	cmd.AddCommand(logdirsDescribeCommand(cl))
	cmd.AddCommand(logdirsAlterReplicasCommand(cl))

	return cmd
}

func electLeaderCommand(cl *client.Client) *cobra.Command {
	var allPartitions bool
	var unclean bool
	var run bool

	cmd := &cobra.Command{
		Use:   "elect-leaders",
		Short: "Trigger leader elections for partitions",
		Long: `Trigger leader elections for topic partitions.

This command allows for triggering leader elections on any topic and any
partition, as well as on all topic partitions. To run on all, you must not
pass any topic flags, and you must use the --all-partitions flag.

The format for triggering topic partitions is "foo:1,2,3", where foo is a
topic and 1,2,3 are partition numbers.

To avoid accidental triggers, this command requires a --run flag to run.
`,
		Example: "elect-leaders --run foo:1,2,3 bar:9",
		Args:    cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, topicParts []string) {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			out.MaybeDie(err, "unable to parse topic partitions: %v", err)
			if !run {
				out.Die("use --run to actually run this command")
			}

			req := &kmsg.ElectLeadersRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			if unclean {
				req.ElectionType = 1
			}

			topics := []kmsg.ElectLeadersRequestTopic{}
			if allPartitions {
				topics = nil
			} else if len(topics) == 0 {
				out.Die("no topics requested for leader election, and not triggering all; nothing to do")
			}

			for topic, partitions := range tps {
				req.Topics = append(req.Topics, kmsg.ElectLeadersRequestTopic{
					Topic:      topic,
					Partitions: partitions,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to elect leaders: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			resp := kresp.(*kmsg.ElectLeadersResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.Die("%v", err)
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					errKind := ""
					var msg string
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						errKind = err.Error()
					}
					if partition.ErrorMessage != nil {
						msg = *partition.ErrorMessage
					}
					fmt.Fprintf(tw, "%s\t%d\t%v\t%s\n",
						topic.Topic,
						partition.Partition,
						errKind,
						msg,
					)
				}
			}
		},
	}

	cmd.Flags().BoolVar(&allPartitions, "all-partitions", false, "trigger leader election on all topics for all partitions")
	cmd.Flags().BoolVar(&unclean, "unclean", false, "allow unclean leader election (requires at least Kafka 2.4.0)")
	cmd.Flags().BoolVarP(&run, "run", "r", false, "actually run the command (avoids accidental elections without this flag)")

	return cmd
}
