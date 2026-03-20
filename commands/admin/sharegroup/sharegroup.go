// Package sharegroup contains share group related subcommands.
package sharegroup

import (
	"context"
	"fmt"
	"os"
	"regexp"
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
		Short:   "Share group operations (list, describe, seek, delete).",
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
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all share groups (Kafka 4.0+).",
		Long: `List all share groups (KIP-932, Kafka 4.0+).

This is equivalent to "group list --type-filter share". It lists share groups
by issuing a ListGroups request with a type filter of "share".
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
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

			table := out.NewFormattedTable(cl.Format(), "share-group.list", 1, "groups",
				"BROKER", "GROUP-ID", "STATE")
			for _, kresp := range kresps {
				err := kresp.Err
				if err == nil {
					err = kerr.ErrorForCode(kresp.Resp.(*kmsg.ListGroupsResponse).ErrorCode)
				}
				if err != nil {
					table.Row(kresp.Meta.NodeID, "", err)
					continue
				}

				resp := kresp.Resp.(*kmsg.ListGroupsResponse)
				for _, group := range resp.Groups {
					table.Row(kresp.Meta.NodeID, group.Group, group.GroupState)
				}
			}
			table.Flush()
			return nil
		},
	}
	cmd.Flags().StringArrayVarP(&statesFilter, "filter", "f", nil, "filter groups by state (Stable, Dead, Empty; repeatable)")
	return cmd
}

func describeCommand(cl *client.Client) *cobra.Command {
	var section string
	var regex bool
	cmd := &cobra.Command{
		Use:     "describe GROUPS...",
		Aliases: []string{"d"},
		Short:   "Describe share groups with offsets and lag (Kafka 4.0+).",
		Long: `Describe share groups (KIP-932, Kafka 4.0+).

If no groups are provided, all share groups are listed and then described.
The output includes group metadata, members, and per-partition start offsets
with lag.

Use --section to show only a specific section of the output:
  summary   group metadata, member count, total lag
  members   per-member detail (id, host, epoch, assignment)
  offsets   per-partition start offsets and lag

Defaults: text shows all sections, awk shows offsets.
`,
		RunE: func(_ *cobra.Command, groups []string) error {
			if section != "" {
				switch client.Strnorm(section) {
				case "summary":
					section = "summary"
				case "members":
					section = "members"
				case "offsets":
					section = "offsets"
				default:
					return out.Errf(out.ExitUsage, "invalid --section %q: must be summary, members, or offsets", section)
				}
			}

			if regex {
				var err error
				groups, err = filterShareGroupsByRegex(cl, groups)
				if err != nil {
					return err
				}
			}
			if len(groups) == 0 {
				var err error
				groups, err = listShareGroups(cl)
				if err != nil {
					return err
				}
			}
			if len(groups) == 0 {
				return fmt.Errorf("no share groups to describe")
			}

			req := kmsg.NewPtrShareGroupDescribeRequest()
			req.GroupIDs = groups

			shards := cl.Client().RequestSharded(context.Background(), req)

			offsetsByGroup := fetchShareGroupOffsets(cl, groups)

			// Pre-compute total lag per group so it is available to all
			// format paths.
			type lagSummary struct {
				totalLag   int64
				partCount  int
				nonZeroLag int
			}
			lagByGroup := make(map[string]lagSummary)
			for gid, offsets := range offsetsByGroup {
				var ls lagSummary
				for _, topic := range offsets.Topics {
					for _, p := range topic.Partitions {
						ls.partCount++
						if p.Lag > 0 {
							ls.totalLag += p.Lag
							ls.nonZeroLag++
						}
					}
				}
				lagByGroup[gid] = ls
			}

			switch cl.Format() {
			case "json":
				type jsonOffset struct {
					Topic       string `json:"topic"`
					Partition   int32  `json:"partition"`
					StartOffset int64  `json:"start_offset"`
					LeaderEpoch int32  `json:"leader_epoch"`
					Lag         int64  `json:"lag"`
					Error       string `json:"error,omitempty"`
				}
				type jsonMember struct {
					MemberID         string   `json:"member_id"`
					ClientID         string   `json:"client_id"`
					Host             string   `json:"host"`
					MemberEpoch      int32    `json:"member_epoch"`
					SubscribedTopics []string `json:"subscribed_topics"`
					Assignment       string   `json:"assignment"`
				}
				type jsonGroup struct {
					GroupID         string       `json:"group_id"`
					Coordinator     int32        `json:"coordinator"`
					State           string       `json:"state"`
					Epoch           int32        `json:"epoch"`
					AssignmentEpoch int32        `json:"assignment_epoch"`
					Assignor        string       `json:"assignor"`
					Members         []jsonMember `json:"members"`
					TotalLag        int64        `json:"total_lag"`
					Offsets         []jsonOffset `json:"offsets"`
					Error           string       `json:"error,omitempty"`
				}
				var jgroups []jsonGroup
				for _, shard := range shards {
					if shard.Err != nil {
						continue
					}
					resp := shard.Resp.(*kmsg.ShareGroupDescribeResponse)
					for _, group := range resp.Groups {
						jg := jsonGroup{
							GroupID:         group.GroupID,
							Coordinator:     shard.Meta.NodeID,
							State:           group.GroupState,
							Epoch:           group.GroupEpoch,
							AssignmentEpoch: group.AssignmentEpoch,
							Assignor:        group.Assignor,
						}
						if ls, ok := lagByGroup[group.GroupID]; ok {
							jg.TotalLag = ls.totalLag
						}
						if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
							msg := err.Error()
							if group.ErrorMessage != nil {
								msg += ": " + *group.ErrorMessage
							}
							jg.Error = msg
						}
						for _, member := range group.Members {
							jg.Members = append(jg.Members, jsonMember{
								MemberID:         member.MemberID,
								ClientID:         member.ClientID,
								Host:             member.ClientHost,
								MemberEpoch:      member.MemberEpoch,
								SubscribedTopics: member.SubscribedTopicNames,
								Assignment:       formatShareMemberAssignment(member),
							})
						}
						if offsets, ok := offsetsByGroup[group.GroupID]; ok {
							for _, topic := range offsets.Topics {
								for _, p := range topic.Partitions {
									jo := jsonOffset{
										Topic:       topic.Topic,
										Partition:   p.Partition,
										StartOffset: p.StartOffset,
										LeaderEpoch: p.LeaderEpoch,
										Lag:         p.Lag,
									}
									if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
										jo.Error = err.Error()
									}
									jg.Offsets = append(jg.Offsets, jo)
								}
							}
						}
						jgroups = append(jgroups, jg)
					}
				}
				out.MarshalJSON("share-group.describe", 1, map[string]any{
					"groups": jgroups,
				})
			case "awk":
				awkSection := section
				if awkSection == "" {
					awkSection = "offsets"
				}
				for _, shard := range shards {
					if shard.Err != nil {
						continue
					}
					resp := shard.Resp.(*kmsg.ShareGroupDescribeResponse)
					for _, group := range resp.Groups {
						switch awkSection {
						case "summary":
							errMsg := ""
							if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
								errMsg = err.Error()
								if group.ErrorMessage != nil {
									errMsg += ": " + *group.ErrorMessage
								}
							}
							ls := lagByGroup[group.GroupID]
							fmt.Printf("%s\t%d\t%s\t%d\t%d\t%s\t%d\t%d\t%s\n",
								group.GroupID,
								shard.Meta.NodeID,
								group.GroupState,
								group.GroupEpoch,
								group.AssignmentEpoch,
								group.Assignor,
								len(group.Members),
								ls.totalLag,
								errMsg,
							)
						case "members":
							for _, member := range group.Members {
								fmt.Printf("%s\t%s\t%s\t%d\t%s\t%s\n",
									member.MemberID,
									member.ClientID,
									member.ClientHost,
									member.MemberEpoch,
									strings.Join(member.SubscribedTopicNames, ","),
									formatShareMemberAssignment(member),
								)
							}
						case "offsets":
							if offsets, ok := offsetsByGroup[group.GroupID]; ok {
								for _, topic := range offsets.Topics {
									for _, p := range topic.Partitions {
										errMsg := ""
										if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
											errMsg = err.Error()
										}
										fmt.Printf("%s\t%d\t%d\t%d\t%d\t%s\n",
											topic.Topic,
											p.Partition,
											p.StartOffset,
											p.LeaderEpoch,
											p.Lag,
											errMsg,
										)
									}
								}
							}
						}
					}
				}
			default:
				showSummary := section == "" || section == "summary"
				showMembers := section == "" || section == "members"
				showOffsets := section == "" || section == "offsets"

				for _, shard := range shards {
					if shard.Err != nil {
						fmt.Fprintf(os.Stderr, "unable to issue ShareGroupDescribe to broker %d (%s:%d): %v\n", shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, shard.Err)
						continue
					}

					resp := shard.Resp.(*kmsg.ShareGroupDescribeResponse)
					for _, group := range resp.Groups {
						if showSummary {
							ls := lagByGroup[group.GroupID]
							printShareGroupSummary(shard.Meta.NodeID, group, ls.totalLag, ls.partCount, ls.nonZeroLag)
						}

						if showMembers && len(group.Members) > 0 {
							if showSummary {
								fmt.Println()
							}
							printShareGroupMembers(cl.Format(), group)
						}

						if showOffsets {
							offsets, ok := offsetsByGroup[group.GroupID]
							if ok {
								if showSummary || showMembers {
									fmt.Println()
								}
								lagTable := out.NewFormattedTable(cl.Format(), "share-group.describe", 1, "offsets",
									"TOPIC", "PARTITION", "START-OFFSET", "LEADER-EPOCH", "LAG", "ERROR")
								for _, topic := range offsets.Topics {
									for _, p := range topic.Partitions {
										errMsg := ""
										if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
											errMsg = err.Error()
										}
										lagStr := "-"
										if p.Lag >= 0 {
											lagStr = fmt.Sprintf("%d", p.Lag)
										}
										lagTable.Row(topic.Topic, p.Partition, p.StartOffset, p.LeaderEpoch, lagStr, errMsg)
									}
								}
								lagTable.Flush()
							}
						}
						fmt.Println()
					}
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&section, "section", "", "output section (summary, members, offsets; default: all for text, offsets for awk)")
	cmd.Flags().BoolVar(&regex, "regex", false, "treat group arguments as regular expressions")
	return cmd
}

func fetchShareGroupOffsets(cl *client.Client, groups []string) map[string]*kmsg.DescribeShareGroupOffsetsResponseGroup {
	req := kmsg.NewPtrDescribeShareGroupOffsetsRequest()
	for _, g := range groups {
		req.Groups = append(req.Groups, kmsg.DescribeShareGroupOffsetsRequestGroup{GroupID: g})
	}

	shards := cl.Client().RequestSharded(context.Background(), req)
	result := make(map[string]*kmsg.DescribeShareGroupOffsetsResponseGroup)
	for _, shard := range shards {
		if shard.Err != nil {
			continue
		}
		resp := shard.Resp.(*kmsg.DescribeShareGroupOffsetsResponse)
		for i := range resp.Groups {
			result[resp.Groups[i].GroupID] = &resp.Groups[i]
		}
	}
	return result
}

func printShareGroupSummary(broker int32, group kmsg.ShareGroupDescribeResponseGroup, totalLag int64, partCount, nonZeroLag int) {
	tw := out.NewTabWriter()
	fmt.Fprintf(tw, "GROUP\t%s\n", group.GroupID)
	fmt.Fprintf(tw, "COORDINATOR\t%d\n", broker)
	fmt.Fprintf(tw, "STATE\t%s\n", group.GroupState)
	fmt.Fprintf(tw, "EPOCH\t%d\n", group.GroupEpoch)
	fmt.Fprintf(tw, "ASSIGNMENT-EPOCH\t%d\n", group.AssignmentEpoch)
	fmt.Fprintf(tw, "ASSIGNOR\t%s\n", group.Assignor)
	fmt.Fprintf(tw, "MEMBERS\t%d\n", len(group.Members))
	fmt.Fprintf(tw, "TOTAL-LAG\t%d across %d partitions (%d non-zero)\n", totalLag, partCount, nonZeroLag)
	if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
		msg := err.Error()
		if group.ErrorMessage != nil {
			msg += ": " + *group.ErrorMessage
		}
		fmt.Fprintf(tw, "ERROR\t%s\n", msg)
	}
	tw.Flush()
}

func printShareGroupMembers(format string, group kmsg.ShareGroupDescribeResponseGroup) {
	table := out.NewFormattedTable(format, "share-group.describe", 1, "members",
		"MEMBER-ID", "CLIENT-ID", "HOST", "MEMBER-EPOCH", "SUBSCRIBED-TOPICS", "ASSIGNMENT")
	for _, member := range group.Members {
		var rack string
		if member.RackID != nil {
			rack = " (rack=" + *member.RackID + ")"
		}

		table.Row(
			member.MemberID,
			member.ClientID,
			member.ClientHost+rack,
			member.MemberEpoch,
			strings.Join(member.SubscribedTopicNames, ","),
			formatShareMemberAssignment(member),
		)
	}
	table.Flush()
}

func formatShareMemberAssignment(member kmsg.ShareGroupDescribeResponseGroupMember) string {
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
	return strings.Join(assignedParts, " ")
}

func deleteCommand(cl *client.Client) *cobra.Command {
	var ifExists bool
	var dryRun bool
	var useRegex bool
	cmd := &cobra.Command{
		Use:   "delete GROUPS...",
		Short: "Delete share groups (Kafka 4.0+).",
		Long: `Delete share groups (KIP-932, Kafka 4.0+).

The groups must be empty (no active consumers) to be deleted.

Use --regex to treat arguments as regex patterns: all share groups matching
any pattern will be deleted. Use --dry-run to see which groups would be deleted
without actually deleting them.
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if useRegex {
				var err error
				args, err = filterShareGroupsByRegex(cl, args)
				if err != nil {
					return err
				}
				if len(args) == 0 {
					fmt.Fprintln(os.Stderr, "No share groups matched the provided regex patterns.")
					return nil
				}
			}
			if dryRun {
				fmt.Fprintln(os.Stderr, "Dry run: the following share groups would be deleted:")
				for _, g := range args {
					fmt.Fprintf(os.Stderr, "  %s\n", g)
				}
				return nil
			}
			brokerResps := cl.Client().RequestSharded(context.Background(), &kmsg.DeleteGroupsRequest{
				Groups: args,
			})
			table := out.NewFormattedTable(cl.Format(), "share-group.delete", 1, "results",
				"BROKER", "GROUP", "ERROR")
			for _, brokerResp := range brokerResps {
				kresp, err := brokerResp.Resp, brokerResp.Err
				if err != nil {
					table.Row(brokerResp.Meta.NodeID, "", fmt.Sprintf("unable to issue request (addr %s:%d): %v", brokerResp.Meta.Host, brokerResp.Meta.Port, err))
					continue
				}
				resp := kresp.(*kmsg.DeleteGroupsResponse)
				for _, r := range resp.Groups {
					msg := "OK"
					if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
						if ifExists && err == kerr.GroupIDNotFound {
							msg = "OK (did not exist)"
						} else {
							msg = err.Error()
						}
					}
					table.Row(brokerResp.Meta.NodeID, r.Group, msg)
				}
			}
			table.Flush()
			return nil
		},
	}
	cmd.Flags().BoolVar(&ifExists, "if-exists", false, "suppress error if group does not exist")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print groups that would be deleted without actually deleting them")
	cmd.Flags().BoolVar(&useRegex, "regex", false, "treat group arguments as regex patterns; match against all existing share groups")
	return cmd
}

func listShareGroups(cl *client.Client) ([]string, error) {
	kresps := cl.Client().RequestSharded(context.Background(), &kmsg.ListGroupsRequest{
		TypesFilter: []string{"share"},
	})
	var groups []string
	var failures int
	for _, kresp := range kresps {
		if kresp.Err != nil {
			fmt.Fprintf(os.Stderr, "unable to issue ListGroups to broker %d (%s:%d): %v\n", kresp.Meta.NodeID, kresp.Meta.Host, kresp.Meta.Port, kresp.Err)
			failures++
			continue
		}
		resp := kresp.Resp.(*kmsg.ListGroupsResponse)
		if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
			fmt.Fprintf(os.Stderr, "ListGroups error from broker %d: %v\n", kresp.Meta.NodeID, err)
			continue
		}
		for _, group := range resp.Groups {
			groups = append(groups, group.Group)
		}
	}
	if failures == len(kresps) {
		return nil, fmt.Errorf("all %d ListGroups requests failed", failures)
	}
	return groups, nil
}

// filterShareGroupsByRegex lists all share groups, compiles each pattern
// argument as a regex, and returns only groups matching at least one pattern.
func filterShareGroupsByRegex(cl *client.Client, patterns []string) ([]string, error) {
	if len(patterns) == 0 {
		return nil, out.Errf(out.ExitUsage, "--regex requires at least one pattern argument")
	}
	var compiled []*regexp.Regexp
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, out.Errf(out.ExitUsage, "invalid regex %q: %v", p, err)
		}
		compiled = append(compiled, re)
	}

	all, err := listShareGroups(cl)
	if err != nil {
		return nil, err
	}
	var matched []string
	for _, g := range all {
		for _, re := range compiled {
			if re.MatchString(g) {
				matched = append(matched, g)
				break
			}
		}
	}
	return matched, nil
}
