// Package group contains group related subcommands.
package group

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
		Use:     "group",
		Aliases: []string{"g"},
		Short:   "Perform group related actions (list, describe, delete, offset-delete).",
		Args:    cobra.ExactArgs(0),
	}

	cmd.AddCommand(
		listCommand(cl),
		describeCommand(cl),
		deleteCommand(cl),
		offsetDeleteCommand(cl),
	)

	return cmd
}

func listCommand(cl *client.Client) *cobra.Command {
	var statesFilter []string
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all groups (Kafka 0.9.0+).",
		Long: `List all Kafka groups.

This command simply lists groups and their protocol types; it does not describe
the groups listed. This prints all of the information from a ListGroups request.
`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			for i, f := range statesFilter {
				switch client.Strnorm(f) {
				case "preparing":
					statesFilter[i] = "Preparing"
				case "preparingrebalance":
					statesFilter[i] = "PreparingRebalance"
				case "completingrebalance":
					statesFilter[i] = "CompletingRebalance"
				case "stable":
					statesFilter[i] = "Stable"
				case "dead":
					statesFilter[i] = "Dead"
				case "empty":
					statesFilter[i] = "Empty"
				}
			}
			kresp, err := cl.Client().Request(context.Background(), &kmsg.ListGroupsRequest{
				StatesFilter: statesFilter,
			})
			out.MaybeDie(err, "unable to list groups: %v", err)
			resp := kresp.(*kmsg.ListGroupsResponse)

			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.Die("%s", err)
				return
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()
			if resp.Version >= 4 {
				fmt.Fprintf(tw, "GROUP ID\tPROTO TYPE\tSTATE\n")
				for _, group := range resp.Groups {
					fmt.Fprintf(tw, "%s\t%s\t%s\n", group.Group, group.ProtocolType, group.GroupState)
				}
			} else {
				fmt.Fprintf(tw, "GROUP ID\tPROTO TYPE\n")
				for _, group := range resp.Groups {
					fmt.Fprintf(tw, "%s\t%s\n", group.Group, group.ProtocolType)
				}
			}
		},
	}
	cmd.Flags().StringArrayVarP(&statesFilter, "filter", "f", nil, "filter groups listed by state (Preparing, PreparingRebalance, CompletingRebalance, Stable, Dead, Empty; Kafka 2.6.0+; repeatable)")
	return cmd
}

func deleteCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "delete GROUPS...",
		Short: "Delete all listed Kafka groups (Kafka 1.1.0+).",
		Args:  cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			kresp, err := cl.Client().Request(context.Background(), &kmsg.DeleteGroupsRequest{
				Groups: args,
			})
			out.MaybeDie(err, "unable to delete groups: %v", err)
			resp := kresp.(*kmsg.DeleteGroupsResponse)
			if cl.AsJSON() {
				out.ExitJSON(resp)
			}
			tw := out.BeginTabWrite()
			defer tw.Flush()
			for _, resp := range resp.Groups {
				msg := "OK"
				if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
					msg = err.Error()
				}
				fmt.Fprintf(tw, "%s\t%s\n", resp.Group, msg)
			}
		},
	}
}

func offsetDeleteCommand(cl *client.Client) *cobra.Command {
	var topicParts []string

	cmd := &cobra.Command{
		Use:   "offset-delete GROUP",
		Short: "Delete offsets for a Kafka group.",
		Long: `Forcefully delete offsets for a Kafka group (Kafka 2.4.0+).

Introduced in Kafka 2.4.0, this command forcefully deletes committed offsets
for a group. Why, you ask? Group commit expiration semantics have changed
across Kafka releases. KIP-211 addressed commits expiring in groups that were
infrequently committing but not yet dead, but introduced a problem where
commits can hang around in some edge cases. See the motivation in KIP-496 for
more detals.

The format for deleting offsets per topic partition is "foo:1,2,3", where foo
is a topic and 1,2,3 are partition numbers.
`,
		Example: "offset-delete mygroup -t foo:1,2,3 -t bar:9",
		Args:    cobra.ExactArgs(1),

		Run: func(_ *cobra.Command, args []string) {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			out.MaybeDie(err, "unable to parse topic partitions: %v", err)

			req := &kmsg.OffsetDeleteRequest{
				Group: args[0],
			}
			for topic, partitions := range tps {
				dt := kmsg.OffsetDeleteRequestTopic{
					Topic: topic,
				}
				for _, partition := range partitions {
					dt.Partitions = append(dt.Partitions, kmsg.OffsetDeleteRequestTopicPartition{Partition: partition})
				}
				req.Topics = append(req.Topics, dt)
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to delete offsets: %v", err)
			resp := kresp.(*kmsg.OffsetDeleteResponse)
			if cl.AsJSON() {
				out.ExitJSON(resp)
			}

			if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.Die(err.Error())
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					msg := "OK"
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						msg = err.Error()
					}
					fmt.Fprintf(tw, "%s\t%d\t%s\n", topic.Topic, partition.Partition, msg)
				}
			}
		},
	}

	cmd.Flags().StringArrayVarP(&topicParts, "topic", "t", nil, "topic and partitions to delete offsets for; repeatable")
	return cmd
}
