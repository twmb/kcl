package partas

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "reassign",
		Aliases: []string{"partas"},
		Short:   "Alter or list partition (re)assignments.",
	}
	cmd.AddCommand(listPartitionReassignments(cl))
	cmd.AddCommand(alterPartitionAssignments(cl))
	cmd.AddCommand(cancelPartitionReassignments(cl))
	cmd.AddCommand(verifyPartitionReassignments(cl))
	return cmd
}

var (
	resultHeaders = []string{"TOPIC", "PARTITION", "ERROR", "MESSAGE"}
	listHeaders   = []string{"TOPIC", "PARTITION", "CURRENT-REPLICAS", "ADDING", "REMOVING"}
)

// resultRows adds one row per partition of an AlterPartitionAssignments
// response, sorted by topic and partition.
func resultRows(table *out.FormattedTable, resp *kmsg.AlterPartitionAssignmentsResponse) {
	type row struct {
		topic     string
		partition int32
		err       string
		msg       string
	}
	var rows []row
	for _, topic := range resp.Topics {
		for _, p := range topic.Partitions {
			var errName, msg string
			if p.ErrorCode != 0 {
				errName = kerr.TypedErrorForCode(p.ErrorCode).Message
				if p.ErrorMessage != nil {
					msg = *p.ErrorMessage
				}
			}
			rows = append(rows, row{topic.Topic, p.Partition, errName, msg})
		}
	}
	slices.SortFunc(rows, func(a, b row) int {
		if a.topic != b.topic {
			return strings.Compare(a.topic, b.topic)
		}
		return int(a.partition - b.partition)
	})
	for _, r := range rows {
		table.Row(r.topic, r.partition, r.err, r.msg)
	}
}

func alterPartitionAssignments(cl *client.Client) *cobra.Command {
	var throttle int64
	cmd := &cobra.Command{
		Use:   "alter 'TOPIC:P->R,R...'...",
		Short: "Alter partition assignments.",
		Long: `Alter partition assignments.

Alter which brokers partitions are assigned to (Kafka 2.4.0+).

The syntax for each topic is

  topic: 1->2,3,4 ; 2->1,2,3

where the first number is the partition, and -> points to the replicas you
want to move the partition to. Note that since this contains a >, you likely
need to quote your input.

If a replica list is empty for a specific partition, this cancels any active
reassignment for that partition.

--throttle BYTES limits replication for the move to BYTES per second, the
way kafka-reassign-partitions.sh --throttle does: before the reassignment is
requested, leader.replication.throttled.rate and
follower.replication.throttled.rate are set to BYTES on every broker the
move touches, and leader.replication.throttled.replicas and
follower.replication.throttled.replicas are set on every topic to the
partition:broker pairs moving out of and into each broker. The configs stay
until "kcl reassign verify" sees every partition of the plan complete and
clears them, the way Kafka's --verify does.

The result prints one row per partition with ERROR and MESSAGE.

EXAMPLES:
  kcl reassign alter 'foo:1->1,2,3' 'bar:2->3,4,5;5->3,4,5'
  kcl reassign alter 'foo:0->2,3' --throttle 10485760    # move at 10MB/s

SEE ALSO:
  kcl reassign verify    check the plan landed, and clear its throttle
  kcl reassign list      list reassignments in progress
  kcl reassign cancel    cancel reassignments in progress
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, topicPartReplicas []string) error {
			tprs, err := flagutil.ParseTopicPartitionReplicas(topicPartReplicas)
			if err != nil {
				return out.Errf(out.ExitUsage, "unable to parse topic partitions replicas: %v", err)
			}

			if throttle >= 0 {
				m, err := applyThrottle(cl, tprs, throttle)
				if err != nil {
					return err
				}
				if cl.Format() == out.FormatText {
					brokers := make([]string, 0, len(m.brokers()))
					for _, b := range m.brokers() {
						brokers = append(brokers, strconv.FormatInt(int64(b), 10))
					}
					fmt.Fprintf(os.Stderr, "Replication throttled to %d bytes/s on brokers %s; \"kcl reassign verify\" clears it when the move completes.\n", throttle, strings.Join(brokers, ","))
				}
			}

			req := &kmsg.AlterPartitionAssignmentsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for topic, partitions := range tprs {
				t := kmsg.AlterPartitionAssignmentsRequestTopic{
					Topic: topic,
				}
				for partition, replicas := range partitions {
					t.Partitions = append(t.Partitions, kmsg.AlterPartitionAssignmentsRequestTopicPartition{
						Partition: partition,
						Replicas:  replicas,
					})
				}
				req.Topics = append(req.Topics, t)
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to alter partition assignments: %v", err)
			}
			resp := kresp.(*kmsg.AlterPartitionAssignmentsResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				return out.BrokerErr(err, resp.ErrorMessage)
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", resultHeaders...).ResultColumns()
			resultRows(table, resp)
			return table.Flush()
		},
	}
	out.Columns(cmd, resultHeaders...)
	cmd.Flags().Int64Var(&throttle, "throttle", -1, "replication throttle in bytes per second to set on the brokers and topics the move touches, or -1 for none")
	return cmd
}

func cancelPartitionReassignments(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cancel TOPIC:P...",
		Short: "Cancel in-progress partition reassignments.",
		Long: `Cancel in-progress partition reassignments.

Cancel active partition reassignments (Kafka 2.4.0+).

The syntax for each topic is

  topic:1,2,3

where the numbers correspond to partitions for a topic. Cancelling reverts each
partition to its pre-reassignment replica set. Use "kcl reassign list" to see
which partitions are currently being reassigned.

At least one topic:partitions must be given; this does not cancel everything at
once by default.

The result prints one row per partition with ERROR and MESSAGE.

EXAMPLES:
  kcl reassign cancel 'foo:1,2,3' 'bar:0'

SEE ALSO:
  kcl reassign list     list reassignments in progress
  kcl reassign alter    start a reassignment
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, topicParts []string) error {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			if err != nil {
				return out.Errf(out.ExitUsage, "unable to parse topic partitions: %v", err)
			}

			req := &kmsg.AlterPartitionAssignmentsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for topic, partitions := range tps {
				if len(partitions) == 0 {
					return out.Errf(out.ExitUsage, "topic %s has no partitions specified to cancel", topic)
				}
				t := kmsg.AlterPartitionAssignmentsRequestTopic{Topic: topic}
				for _, partition := range partitions {
					// A nil replica list cancels the active reassignment for the
					// partition, reverting it to its prior replica set.
					t.Partitions = append(t.Partitions, kmsg.AlterPartitionAssignmentsRequestTopicPartition{
						Partition: partition,
						Replicas:  nil,
					})
				}
				req.Topics = append(req.Topics, t)
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to cancel partition reassignments: %v", err)
			}
			resp := kresp.(*kmsg.AlterPartitionAssignmentsResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				return out.BrokerErr(err, resp.ErrorMessage)
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", resultHeaders...).ResultColumns()
			resultRows(table, resp)
			return table.Flush()
		},
	}
	out.Columns(cmd, resultHeaders...)
	return cmd
}

func listPartitionReassignments(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list [TOPIC:P...]",
		Aliases: []string{"ls"},
		Short:   "List partition reassignments.",
		Long: `List partition reassignments.

List which partitions are currently being reassigned (Kafka 2.4.0+).

The syntax for each topic is

  topic:1,2,3

where the numbers correspond to partitions for a topic.

If no topics are specified, this lists all active reassignments. Rows are
sorted by topic and partition.

EXAMPLES:
  kcl reassign list                # every reassignment in progress
  kcl reassign list foo:0,1        # two partitions of foo

SEE ALSO:
  kcl reassign alter     start a reassignment
  kcl reassign cancel    cancel reassignments in progress
`,
		RunE: func(_ *cobra.Command, topicParts []string) error {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			if err != nil {
				return out.Errf(out.ExitUsage, "unable to parse topic partitions: %v", err)
			}

			req := &kmsg.ListPartitionReassignmentsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for topic, partitions := range tps {
				if len(partitions) == 0 {
					return out.Errf(out.ExitUsage, "topic %s has no partitions specified to list", topic)
				}
				req.Topics = append(req.Topics, kmsg.ListPartitionReassignmentsRequestTopic{
					Topic:      topic,
					Partitions: partitions,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to list partition reassignments: %v", err)
			}
			resp := kresp.(*kmsg.ListPartitionReassignmentsResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				return out.BrokerErr(err, resp.ErrorMessage)
			}

			type row struct {
				topic     string
				partition int32
				replicas  []int32
				adding    []int32
				removing  []int32
			}
			var rows []row
			for _, topic := range resp.Topics {
				for _, p := range topic.Partitions {
					rows = append(rows, row{
						topic.Topic, p.Partition,
						sortedSet(p.Replicas), sortedSet(p.AddingReplicas), sortedSet(p.RemovingReplicas),
					})
				}
			}
			slices.SortFunc(rows, func(a, b row) int {
				if a.topic != b.topic {
					return strings.Compare(a.topic, b.topic)
				}
				return int(a.partition - b.partition)
			})

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "reassignments", listHeaders...)
			for _, r := range rows {
				table.Row(r.topic, r.partition, r.replicas, r.adding, r.removing)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, listHeaders...)
	return cmd
}
