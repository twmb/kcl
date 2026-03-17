// Package group contains group related subcommands.
package group

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

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
		seekCommand(cl),
	)

	return cmd
}

func listCommand(cl *client.Client) *cobra.Command {
	var statesFilter []string
	var typesFilter []string
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
			kresps := cl.Client().RequestSharded(context.Background(), &kmsg.ListGroupsRequest{
				StatesFilter: statesFilter,
				TypesFilter:  typesFilter,
			})

			table := out.NewFormattedTable(cl.Format(), "group.list", 1, "groups",
				"BROKER", "GROUP ID", "PROTO TYPE", "GROUP TYPE", "STATE", "ERROR")
			for _, kresp := range kresps {
				err := kresp.Err
				if err == nil {
					err = kerr.ErrorForCode(kresp.Resp.(*kmsg.ListGroupsResponse).ErrorCode)
				}
				if err != nil {
					table.Row(kresp.Meta.NodeID, "", "", "", "", err)
					continue
				}

				resp := kresp.Resp.(*kmsg.ListGroupsResponse)
				for _, group := range resp.Groups {
					table.Row(kresp.Meta.NodeID, group.Group, group.ProtocolType, group.GroupType, group.GroupState, "")
				}
			}
			table.Flush()
		},
	}
	cmd.Flags().StringArrayVarP(&statesFilter, "filter", "f", nil, "filter groups listed by state (Preparing, PreparingRebalance, CompletingRebalance, Stable, Dead, Empty; Kafka 2.6.0+; repeatable)")
	cmd.Flags().StringArrayVar(&typesFilter, "type-filter", nil, "filter groups listed by type (Classic, Consumer, Share; Kafka 3.0+; repeatable)")
	return cmd
}

func deleteCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "delete GROUPS...",
		Short: "Delete all listed Kafka groups (Kafka 1.1.0+).",
		Args:  cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			brokerResps := cl.Client().RequestSharded(context.Background(), &kmsg.DeleteGroupsRequest{
				Groups: args,
			})
			tw := out.BeginTabWrite()
			defer tw.Flush()
			fmt.Fprintf(tw, "BROKER\tGROUP\tERROR\n")
			for _, brokerResp := range brokerResps {
				kresp, err := brokerResp.Resp, brokerResp.Err
				if err != nil {
					fmt.Fprintf(tw, "%d\t\tunable to issue request (addr %s:%d): %v\n", brokerResp.Meta.NodeID, brokerResp.Meta.Host, brokerResp.Meta.Port, err)
					continue
				}
				resp := kresp.(*kmsg.DeleteGroupsResponse)
				for _, resp := range resp.Groups {
					msg := "OK"
					if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
						msg = err.Error()
					}
					fmt.Fprintf(tw, "%d\t%s\t%s\n", brokerResp.Meta.NodeID, resp.Group, msg)
				}
			}
		},
	}
}

func offsetDeleteCommand(cl *client.Client) *cobra.Command {
	var topicParts []string
	var fromFile string

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
is a topic and 1,2,3 are partition numbers. Alternatively, use --from-file
with a JSON file:

  [{"topic": "foo", "partition": 1}, {"topic": "bar", "partition": 0}]
`,
		Example: "offset-delete mygroup -t foo:1,2,3 -t bar:9",
		Args:    cobra.ExactArgs(1),

		Run: func(_ *cobra.Command, args []string) {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			out.MaybeDie(err, "unable to parse topic partitions: %v", err)

			if fromFile != "" {
				type fileEntry struct {
					Topic     string `json:"topic"`
					Partition int32  `json:"partition"`
				}
				var entries []fileEntry
				raw, err := os.ReadFile(fromFile)
				out.MaybeDie(err, "unable to read --from-file: %v", err)
				err = json.Unmarshal(raw, &entries)
				out.MaybeDie(err, "unable to parse --from-file JSON: %v", err)
				for _, e := range entries {
					tps[e.Topic] = append(tps[e.Topic], e.Partition)
				}
			}

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
	cmd.Flags().StringVar(&fromFile, "from-file", "", "JSON file of [{topic, partition}, ...] to delete offsets for")
	return cmd
}
