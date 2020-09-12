package admin

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"
	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

func alterPartitionAssignments(cl *client.Client) *cobra.Command {
	var topicPartReplicas []string

	cmd := &cobra.Command{
		Use:   "partition-assignments",
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
		Example: "partition-assignments -t 'foo:1->1,2,3' -t 'bar:2->3,4,5;5->3,4,5'",
		Run: func(_ *cobra.Command, args []string) {
			tprs, err := flagutil.ParseTopicPartitionReplicas(topicPartReplicas)
			out.MaybeDie(err, "unable to parse topic partitions replicas: %v", err)

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
			out.MaybeDie(err, "unable to alter partition assignments: %v", err)
			resp := kresp.(*kmsg.AlterPartitionAssignmentsResponse)
			if cl.AsJSON() {
				out.ExitJSON(resp)
			}

			if resp.ErrorCode != 0 {
				out.ErrAndMsg(resp.ErrorCode, resp.ErrorMessage)
				out.Exit()
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()
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
					fmt.Fprintf(tw, "%s\t%d\t%s\t%s\n", topic.Topic, partition.Partition, msg, detail)
				}
			}
		},
	}

	cmd.Flags().StringArrayVarP(&topicPartReplicas, "topic", "t", nil, "topic, partitions, and replica destinations; repeatable")
	return cmd
}

func listPartitionReassignments(cl *client.Client) *cobra.Command {
	var topicParts []string

	cmd := &cobra.Command{
		Use:   "partition-reassignments",
		Short: "List partition reassignments.",
		Long: `List which partitions are currently being reassigned (Kafka 2.4.0+).

The syntax for each topic is

  topic:1,2,3

where the numbers correspond to partitions for a topic.

If no topics are specified, this lists all active reassignments.
`,
		Run: func(_ *cobra.Command, args []string) {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			out.MaybeDie(err, "unable to parse topic partitions: %v", err)

			req := &kmsg.ListPartitionReassignmentsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for topic, partitions := range tps {
				req.Topics = append(req.Topics, kmsg.ListPartitionReassignmentsRequestTopic{
					Topic:      topic,
					Partitions: partitions,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to list partition reassignments: %v", err)
			resp := kresp.(*kmsg.ListPartitionReassignmentsResponse)
			if cl.AsJSON() {
				out.ExitJSON(resp)
			}

			if resp.ErrorCode != 0 {
				out.ErrAndMsg(resp.ErrorCode, resp.ErrorMessage)
				out.Exit()
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()
			fmt.Fprint(tw, "TOPIC\tPARTITION\tCURRENT REPLICAS\tADDING\tREMOVING\n")
			for _, topic := range resp.Topics {
				for _, p := range topic.Partitions {
					sort.Slice(p.Replicas, func(i, j int) bool { return p.Replicas[i] < p.Replicas[j] })
					sort.Slice(p.AddingReplicas, func(i, j int) bool { return p.AddingReplicas[i] < p.AddingReplicas[j] })
					sort.Slice(p.RemovingReplicas, func(i, j int) bool { return p.RemovingReplicas[i] < p.RemovingReplicas[j] })
					fmt.Fprintf(tw, "%s\t%d\t%v\t%v\t%v\n", topic.Topic, p.Partition, p.Replicas, p.AddingReplicas, p.RemovingReplicas)
				}
			}
		},
	}

	cmd.Flags().StringArrayVarP(&topicParts, "topic", "t", nil, "topic and partitions to list partition reassignments for; repeatable")

	return cmd
}
