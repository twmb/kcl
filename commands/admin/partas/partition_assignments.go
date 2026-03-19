package partas

import (
	"context"
	"fmt"
	"sort"

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
	return cmd
}

func alterPartitionAssignments(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "alter",
		Short: "Alter partition assignments.",
		Long: `Alter which brokers partitions are assigned to (Kafka 2.4.0+).

The syntax for each topic is

  topic: 1->2,3,4 ; 2->1,2,3

where the first number is the partition, and -> points to the replicas you
want to move the partition to. Note that since this contains a >, you likely
need to quote your input to the flag.

If a replica list is empty for a specific partition, this cancels any active
reassignment for that partition.
`,
		Example: "alter 'foo:1->1,2,3' 'bar:2->3,4,5;5->3,4,5'",
		RunE: func(_ *cobra.Command, topicPartReplicas []string) error {
			tprs, err := flagutil.ParseTopicPartitionReplicas(topicPartReplicas)
			if err != nil {
				return fmt.Errorf("unable to parse topic partitions replicas: %v", err)
			}

			req := &kmsg.AlterPartitionAssignmentsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for topic, partitions := range tprs {
				if len(partitions) == 0 {
				}
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

			if resp.ErrorCode != 0 {
				additional := ""
				if resp.ErrorMessage != nil {
					additional = ": " + *resp.ErrorMessage
				}
				return fmt.Errorf("%s%s", kerr.ErrorForCode(resp.ErrorCode), additional)
			}

			table := out.NewFormattedTable(cl.Format(), "reassign.alter", 1, "results",
				"TOPIC", "PARTITION", "STATUS", "DETAIL")
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					msg := "OK"
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						msg = err.Error()
					}
					detail := ""
					if partition.ErrorMessage != nil {
						detail = *partition.ErrorMessage
					}
					table.Row(topic.Topic, partition.Partition, msg, detail)
				}
			}
			table.Flush()
			return nil
		},
	}
}

func listPartitionReassignments(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List partition reassignments.",
		Long: `List which partitions are currently being reassigned (Kafka 2.4.0+).

The syntax for each topic is

  topic:1,2,3

where the numbers correspond to partitions for a topic.

If no topics are specified, this lists all active reassignments.
`,
		RunE: func(_ *cobra.Command, topicParts []string) error {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			if err != nil {
				return fmt.Errorf("unable to parse topic partitions: %v", err)
			}

			req := &kmsg.ListPartitionReassignmentsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for topic, partitions := range tps {
				if len(partitions) == 0 {
					return fmt.Errorf("topic %s has no partitions specified to list", topic)
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

			if resp.ErrorCode != 0 {
				additional := ""
				if resp.ErrorMessage != nil {
					additional = ": " + *resp.ErrorMessage
				}
				return fmt.Errorf("%s%s", kerr.ErrorForCode(resp.ErrorCode), additional)
			}

			table := out.NewFormattedTable(cl.Format(), "reassign.list", 1, "reassignments",
				"TOPIC", "PARTITION", "CURRENT REPLICAS", "ADDING", "REMOVING")
			for _, topic := range resp.Topics {
				for _, p := range topic.Partitions {
					sort.Slice(p.Replicas, func(i, j int) bool { return p.Replicas[i] < p.Replicas[j] })
					sort.Slice(p.AddingReplicas, func(i, j int) bool { return p.AddingReplicas[i] < p.AddingReplicas[j] })
					sort.Slice(p.RemovingReplicas, func(i, j int) bool { return p.RemovingReplicas[i] < p.RemovingReplicas[j] })
					table.Row(topic.Topic, p.Partition, fmt.Sprint(p.Replicas), fmt.Sprint(p.AddingReplicas), fmt.Sprint(p.RemovingReplicas))
				}
			}
			table.Flush()
			return nil
		},
	}
}
