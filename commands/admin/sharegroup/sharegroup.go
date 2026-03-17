// Package sharegroup contains share group related subcommands.
package sharegroup

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "share-group",
		Aliases: []string{"sg"},
		Short:   "Perform share group related actions (list, describe, delete, describe-offsets, alter-offsets, delete-offsets).",
		Args:    cobra.ExactArgs(0),
	}

	cmd.AddCommand(
		listCommand(cl),
		describeCommand(cl),
		deleteCommand(cl),
		describeOffsetsCommand(cl),
		alterOffsetsCommand(cl),
		deleteOffsetsCommand(cl),
		seekCommand(cl),
	)

	return cmd
}

func listCommand(cl *client.Client) *cobra.Command {
	var statesFilter []string
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all share groups (Kafka 4.0+).",
		Long: `List all share groups (KIP-932, Kafka 4.0+).

This is equivalent to "group list --type-filter share". It lists share groups
by issuing a ListGroups request with a type filter of "share".
`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			for i, f := range statesFilter {
				switch client.Strnorm(f) {
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
				TypesFilter:  []string{"share"},
			})

			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "BROKER\tGROUP ID\tSTATE\n")
			for _, kresp := range kresps {
				err := kresp.Err
				if err == nil {
					err = kerr.ErrorForCode(kresp.Resp.(*kmsg.ListGroupsResponse).ErrorCode)
				}
				if err != nil {
					fmt.Fprintf(tw, "%d\t\t%v\n", kresp.Meta.NodeID, err)
					continue
				}

				resp := kresp.Resp.(*kmsg.ListGroupsResponse)
				for _, group := range resp.Groups {
					fmt.Fprintf(tw, "%d\t%s\t%s\n", kresp.Meta.NodeID, group.Group, group.GroupState)
				}
			}
		},
	}
	cmd.Flags().StringArrayVarP(&statesFilter, "filter", "f", nil, "filter groups by state (Stable, Dead, Empty; repeatable)")
	return cmd
}

func describeCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "describe GROUPS...",
		Aliases: []string{"d"},
		Short:   "Describe share groups (Kafka 4.0+).",
		Long: `Describe share groups (KIP-932, Kafka 4.0+).

If no groups are provided, all share groups are listed and then described.
`,
		Run: func(_ *cobra.Command, groups []string) {
			if len(groups) == 0 {
				groups = listShareGroups(cl)
			}
			if len(groups) == 0 {
				out.Die("no share groups to describe")
			}

			req := kmsg.NewPtrShareGroupDescribeRequest()
			req.GroupIDs = groups

			shards := cl.Client().RequestSharded(context.Background(), req)
			if cl.AsJSON() {
				out.ExitJSON(shards)
			}

			for _, shard := range shards {
				if shard.Err != nil {
					fmt.Printf("unable to issue ShareGroupDescribe to broker %d (%s:%d): %v\n", shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, shard.Err)
					continue
				}

				resp := shard.Resp.(*kmsg.ShareGroupDescribeResponse)
				for _, group := range resp.Groups {
					printShareGroup(shard.Meta.NodeID, group)
					fmt.Println()
				}
			}
		},
	}
	return cmd
}

func printShareGroup(broker int32, group kmsg.ShareGroupDescribeResponseGroup) {
	tw := out.NewTabWriter()
	fmt.Fprintf(tw, "GROUP\t%s\n", group.GroupID)
	fmt.Fprintf(tw, "COORDINATOR\t%d\n", broker)
	fmt.Fprintf(tw, "STATE\t%s\n", group.GroupState)
	fmt.Fprintf(tw, "EPOCH\t%d\n", group.GroupEpoch)
	fmt.Fprintf(tw, "ASSIGNMENT EPOCH\t%d\n", group.AssignmentEpoch)
	fmt.Fprintf(tw, "ASSIGNOR\t%s\n", group.Assignor)
	fmt.Fprintf(tw, "MEMBERS\t%d\n", len(group.Members))
	if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
		msg := err.Error()
		if group.ErrorMessage != nil {
			msg += ": " + *group.ErrorMessage
		}
		fmt.Fprintf(tw, "ERROR\t%s\n", msg)
	}
	tw.Flush()

	if len(group.Members) == 0 {
		return
	}

	headers := []string{
		"MEMBER-ID",
		"CLIENT-ID",
		"HOST",
		"MEMBER-EPOCH",
		"SUBSCRIBED-TOPICS",
		"ASSIGNMENT",
	}
	table := out.NewTable(headers...)
	defer table.Flush()
	for _, member := range group.Members {
		var rack string
		if member.RackID != nil {
			rack = " (rack=" + *member.RackID + ")"
		}

		var assignedParts []string
		for _, tp := range member.Assignment.TopicPartitions {
			name := tp.Topic
			if name == "" {
				name = fmt.Sprintf("%x", tp.TopicID)
			}
			parts := make([]string, len(tp.Partitions))
			for i, p := range tp.Partitions {
				parts[i] = fmt.Sprintf("%d", p)
			}
			assignedParts = append(assignedParts, name+":"+strings.Join(parts, ","))
		}

		table.Print(
			member.MemberID,
			member.ClientID,
			member.ClientHost+rack,
			member.MemberEpoch,
			strings.Join(member.SubscribedTopicNames, ","),
			strings.Join(assignedParts, " "),
		)
	}
}

func deleteCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "delete GROUPS...",
		Short: "Delete share groups (Kafka 4.0+).",
		Long: `Delete share groups (KIP-932, Kafka 4.0+).

This is equivalent to "group delete". The DeleteGroups API is group-type
agnostic; this command is provided as a convenience. The groups must be
empty (no active consumers) to be deleted.
`,
		Args: cobra.MinimumNArgs(1),
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

func listShareGroups(cl *client.Client) []string {
	kresps := cl.Client().RequestSharded(context.Background(), &kmsg.ListGroupsRequest{
		TypesFilter: []string{"share"},
	})
	var groups []string
	var failures int
	for _, kresp := range kresps {
		if kresp.Err != nil {
			fmt.Printf("unable to issue ListGroups to broker %d (%s:%d): %v\n", kresp.Meta.NodeID, kresp.Meta.Host, kresp.Meta.Port, kresp.Err)
			failures++
			continue
		}
		resp := kresp.Resp.(*kmsg.ListGroupsResponse)
		if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
			fmt.Printf("ListGroups error from broker %d: %v\n", kresp.Meta.NodeID, err)
			continue
		}
		for _, group := range resp.Groups {
			groups = append(groups, group.Group)
		}
	}
	if failures == len(kresps) {
		out.Die("all %d ListGroups requests failed", failures)
	}
	return groups
}
