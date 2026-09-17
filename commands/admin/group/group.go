// Package group contains group related subcommands.
package group

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kadm"
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
		Short:   "Consumer group operations (list, describe, seek, delete).",
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

// normStates spells each group state the way ListGroups wants it, so that
// --state stable and --state PreparingRebalance both match.
func normStates(states []string) {
	for i, s := range states {
		switch client.Strnorm(s) {
		case "preparing":
			states[i] = "Preparing"
		case "preparingrebalance":
			states[i] = "PreparingRebalance"
		case "completingrebalance":
			states[i] = "CompletingRebalance"
		case "stable":
			states[i] = "Stable"
		case "dead":
			states[i] = "Dead"
		case "empty":
			states[i] = "Empty"
		}
	}
}

// listedGroup is one row of group list: a group a broker answered with, or
// the error a broker answered instead, with no group.
type listedGroup struct {
	broker    int32
	group     string
	protoType string
	groupType string
	state     string
	err       error
}

// listGroupRows asks every broker for its groups and returns one row per
// group, sorted by group, with a broker that failed as a row of its own
// first.
func listGroupRows(cl *client.Client, states, types []string) []listedGroup {
	kresps := cl.Client().RequestSharded(context.Background(), &kmsg.ListGroupsRequest{
		StatesFilter: states,
		TypesFilter:  types,
	})
	var rows []listedGroup
	for _, kresp := range kresps {
		err := kresp.Err
		if err == nil {
			err = kerr.ErrorForCode(kresp.Resp.(*kmsg.ListGroupsResponse).ErrorCode)
		}
		if err != nil {
			rows = append(rows, listedGroup{broker: kresp.Meta.NodeID, err: err})
			continue
		}
		for _, g := range kresp.Resp.(*kmsg.ListGroupsResponse).Groups {
			rows = append(rows, listedGroup{
				broker:    kresp.Meta.NodeID,
				group:     g.Group,
				protoType: g.ProtocolType,
				groupType: g.GroupType,
				state:     g.GroupState,
			})
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].group != rows[j].group {
			return rows[i].group < rows[j].group
		}
		return rows[i].broker < rows[j].broker
	})
	return rows
}

func listCommand(cl *client.Client) *cobra.Command {
	var states []string
	var types []string
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all groups (Kafka 0.9.0+).",
		Long: `List all groups (Kafka 0.9.0+).

List all Kafka groups, sorted by name.

This command simply lists groups and their protocol types; it does not describe
the groups listed. This prints all of the information from a ListGroups request.

A broker that could not answer is one row with its error and no group, and
the command exits 1.

EXAMPLES:
  kcl group list                          # every group
  kcl group list --state empty            # groups with no members (Kafka 2.6+)
  kcl group list --type consumer          # KIP-848 groups (Kafka 3.0+)

SEE ALSO:
  kcl group describe      describe groups with lag
  kcl share-group list    list share groups
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			normStates(states)
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "groups",
				"BROKER", "GROUP", "PROTO-TYPE", "GROUP-TYPE", "STATE", "ERROR").ResultColumns()
			for _, r := range listGroupRows(cl, states, types) {
				if r.err != nil {
					table.Row(r.broker, out.Unknown, out.Unknown, out.Unknown, out.Unknown, r.err.Error())
					continue
				}
				table.Row(r.broker, r.group, r.protoType, r.groupType, r.state, "")
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, "BROKER", "GROUP", "PROTO-TYPE", "GROUP-TYPE", "STATE", "ERROR")
	cmd.Flags().StringArrayVar(&states, "state", nil, "keep only groups in this state (Preparing, PreparingRebalance, CompletingRebalance, Stable, Dead, Empty; Kafka 2.6.0+; repeatable)")
	cmd.Flags().StringArrayVar(&types, "type", nil, "keep only groups of this type (Classic, Consumer, Share; Kafka 3.0+; repeatable)")
	cmd.Flags().StringArrayVarP(&states, "filter", "f", nil, "old name of --state")
	cmd.Flags().MarkHidden("filter")
	cmd.Flags().StringArrayVar(&types, "type-filter", nil, "old name of --type")
	cmd.Flags().MarkHidden("type-filter")
	return cmd
}

// deleteGroupResult returns what to print for one deleted group: the ERROR
// column, "" when the delete succeeded, and the MESSAGE column. Kafka 4.4
// attaches a message to a failed delete (KIP-1331); an older broker sends
// none and the message is empty.
func deleteGroupResult(g kmsg.DeleteGroupsResponseGroup) (errStr, message string) {
	if err := kerr.ErrorForCode(g.ErrorCode); err != nil {
		errStr = err.Error()
	}
	if g.ErrorMessage != nil {
		message = *g.ErrorMessage
	}
	return errStr, message
}

func deleteCommand(cl *client.Client) *cobra.Command {
	var dryRun bool
	var regex bool
	cmd := &cobra.Command{
		Use:   "delete GROUPS...",
		Short: "Delete all listed Kafka groups (Kafka 1.1.0+).",
		Long: `Delete all listed Kafka groups (Kafka 1.1.0+).

Delete the named groups. A group must have no members (state Empty or Dead)
to be deleted; the broker answers NON_EMPTY_GROUP otherwise.

Use --regex to treat the arguments as patterns matched against every group.
Use --dry-run to print the groups that would be deleted without deleting
them.

EXAMPLES:
  kcl group delete g1 g2                    # delete two groups
  kcl group delete -r 'test-.*' --dry-run   # print what the pattern matches
  kcl group delete -r 'test-.*'             # delete them

SEE ALSO:
  kcl group list          list all groups
  kcl group offset-delete delete committed offsets of a group
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if regex {
				var err error
				args, err = filterGroupsByRegex(cl, args, listGroups)
				if err != nil {
					return err
				}
				if len(args) == 0 {
					fmt.Fprintln(os.Stderr, "No groups matched the provided regex patterns.")
				}
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results",
				"BROKER", "GROUP", "ERROR", "MESSAGE").ResultColumns()
			if dryRun {
				table.SetDryRun(true)
				for _, group := range args {
					table.Row(out.Unknown, group, out.Unknown, out.Unknown)
				}
				return table.Flush()
			}
			if len(args) == 0 {
				return table.Flush()
			}

			brokerResps := cl.Client().RequestSharded(context.Background(), &kmsg.DeleteGroupsRequest{
				Groups: args,
			})
			for _, brokerResp := range brokerResps {
				kresp, err := brokerResp.Resp, brokerResp.Err
				if err != nil {
					table.Row(brokerResp.Meta.NodeID, out.Unknown, fmt.Sprintf("unable to issue request (addr %s:%d): %v", brokerResp.Meta.Host, brokerResp.Meta.Port, err), "")
					continue
				}
				resp := kresp.(*kmsg.DeleteGroupsResponse)
				for _, g := range resp.Groups {
					errStr, message := deleteGroupResult(g)
					table.Row(brokerResp.Meta.NodeID, g.Group, errStr, message)
				}
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, "BROKER", "GROUP", "ERROR", "MESSAGE")
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "print groups that would be deleted without actually deleting them")
	cmd.Flags().BoolVarP(&regex, "regex", "r", false, "treat group arguments as regular expressions")
	return cmd
}

// fillPartitions replaces every nil partition list in tps, a topic named
// with no partitions, with every partition the topic has, from metadata. A
// topic the cluster does not know is an error.
func fillPartitions(cl *client.Client, tps map[string][]int32) error {
	var whole []string
	for topic, partitions := range tps {
		if partitions == nil {
			whole = append(whole, topic)
		}
	}
	if len(whole) == 0 {
		return nil
	}
	sort.Strings(whole)
	listed, err := kadm.NewClient(cl.Client()).ListTopics(context.Background(), whole...)
	if err != nil {
		return fmt.Errorf("unable to list partitions of %v: %v", whole, err)
	}
	for _, topic := range whole {
		td, ok := listed[topic]
		if !ok {
			return fmt.Errorf("topic %q not in metadata", topic)
		}
		if td.Err != nil {
			return fmt.Errorf("topic %q: %v", topic, td.Err)
		}
		partitions := td.Partitions.Numbers()
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
		tps[topic] = partitions
	}
	return nil
}

func offsetDeleteCommand(cl *client.Client) *cobra.Command {
	var topicParts []string
	var fromFile string

	cmd := &cobra.Command{
		Use:   "offset-delete GROUP",
		Short: "Delete offsets for a Kafka group.",
		Long: `Delete offsets for a Kafka group.

Forcefully delete offsets for a Kafka group (Kafka 2.4.0+).

Introduced in Kafka 2.4.0, this command forcefully deletes committed offsets
for a group. Why, you ask? Group commit expiration semantics have changed
across Kafka releases. KIP-211 addressed commits expiring in groups that were
infrequently committing but not yet dead, but introduced a problem where
commits can hang around in some edge cases. See the motivation in KIP-496 for
more detals.

-t accepts plain names or topic:partitions pairs:
  foo              every partition of foo
  foo:1,2,3        only partitions 1, 2, and 3 of foo

Alternatively, use --from-file with a JSON file:

  [{"topic": "foo", "partition": 1}, {"topic": "bar", "partition": 0}]

EXAMPLES:
  kcl group offset-delete mygroup -t foo                # every partition of foo
  kcl group offset-delete mygroup -t foo:1,2,3 -t bar:9
  kcl group offset-delete mygroup --from-file offsets.json

SEE ALSO:
  kcl group describe    describe groups with lag
  kcl group seek        reset group offsets
`,
		Args: cobra.ExactArgs(1),

		RunE: func(_ *cobra.Command, args []string) error {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			if err != nil {
				return out.Errf(out.ExitUsage, "unable to parse topic partitions: %v", err)
			}

			if fromFile != "" {
				type fileEntry struct {
					Topic     string `json:"topic"`
					Partition int32  `json:"partition"`
				}
				var entries []fileEntry
				raw, err := os.ReadFile(fromFile)
				if err != nil {
					return out.Errf(out.ExitUsage, "unable to read --from-file: %v", err)
				}
				err = json.Unmarshal(raw, &entries)
				if err != nil {
					return out.Errf(out.ExitUsage, "unable to parse --from-file JSON: %v", err)
				}
				for _, e := range entries {
					if p, ok := tps[e.Topic]; ok && p == nil {
						continue // -t named every partition already
					}
					tps[e.Topic] = append(tps[e.Topic], e.Partition)
				}
			}
			if len(tps) == 0 {
				return out.Errf(out.ExitUsage, "at least one topic is required (-t or --from-file)")
			}
			if err := fillPartitions(cl, tps); err != nil {
				return err
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
			if err != nil {
				return fmt.Errorf("unable to delete offsets: %v", err)
			}
			resp := kresp.(*kmsg.OffsetDeleteResponse)

			if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
				return fmt.Errorf("%s", err.Error())
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results",
				"TOPIC", "PARTITION", "ERROR", "MESSAGE").ResultColumns()
			sort.Slice(resp.Topics, func(i, j int) bool { return resp.Topics[i].Topic < resp.Topics[j].Topic })
			for _, topic := range resp.Topics {
				sort.Slice(topic.Partitions, func(i, j int) bool { return topic.Partitions[i].Partition < topic.Partitions[j].Partition })
				for _, partition := range topic.Partitions {
					errStr := ""
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						errStr = err.Error()
					}
					table.Row(topic.Topic, partition.Partition, errStr, "")
				}
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, "TOPIC", "PARTITION", "ERROR", "MESSAGE")

	cmd.Flags().StringArrayVarP(&topicParts, "topic", "t", nil, "topic, or topic:partitions, to delete offsets for; a bare topic is every partition; repeatable")
	cmd.Flags().StringVar(&fromFile, "from-file", "", "JSON file of [{topic, partition}, ...] to delete offsets for")
	return cmd
}
