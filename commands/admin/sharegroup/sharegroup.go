// Package sharegroup contains share group related subcommands.
package sharegroup

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"slices"
	"sort"
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
	var states []string
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all share groups (Kafka 4.0+).",
		Long: `List all share groups (Kafka 4.0+).

List all share groups (KIP-932, Kafka 4.0+), sorted by name.

This is equivalent to "group list --type share". It lists share groups by
issuing a ListGroups request with a type filter of "share".

A broker that could not answer is one row with its error and no group, and
the command exits 1.

EXAMPLES:
  kcl share-group list                    # every share group
  kcl share-group list --state empty      # share groups with no members

SEE ALSO:
  kcl share-group describe    describe share groups with lag
  kcl group list              list every group, of every type
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			for i, f := range states {
				switch client.Strnorm(f) {
				case "stable":
					states[i] = "Stable"
				case "dead":
					states[i] = "Dead"
				case "empty":
					states[i] = "Empty"
				}
			}
			kresps := cl.Client().RequestSharded(context.Background(), &kmsg.ListGroupsRequest{
				StatesFilter: states,
				TypesFilter:  []string{"share"},
			})

			type row struct {
				broker int32
				group  string
				state  string
				err    error
			}
			var rows []row
			for _, kresp := range kresps {
				err := kresp.Err
				if err == nil {
					err = kerr.ErrorForCode(kresp.Resp.(*kmsg.ListGroupsResponse).ErrorCode)
				}
				if err != nil {
					rows = append(rows, row{broker: kresp.Meta.NodeID, err: err})
					continue
				}
				for _, g := range kresp.Resp.(*kmsg.ListGroupsResponse).Groups {
					rows = append(rows, row{broker: kresp.Meta.NodeID, group: g.Group, state: g.GroupState})
				}
			}
			sort.SliceStable(rows, func(i, j int) bool {
				if rows[i].group != rows[j].group {
					return rows[i].group < rows[j].group
				}
				return rows[i].broker < rows[j].broker
			})

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "groups",
				"BROKER", "GROUP", "STATE", "ERROR").ErrorColumn()
			for _, r := range rows {
				if r.err != nil {
					table.Row(r.broker, out.Unknown, out.Unknown, out.ErrCell(r.err))
					continue
				}
				table.Row(r.broker, r.group, r.state, "")
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, "BROKER", "GROUP", "STATE", "ERROR")
	cmd.Flags().StringArrayVar(&states, "state", nil, "keep only groups in this state (Stable, Dead, Empty; repeatable)")
	cmd.Flags().StringArrayVarP(&states, "filter", "f", nil, "old name of --state")
	cmd.Flags().MarkHidden("filter")
	return cmd
}

// Column shapes of share-group describe, by --section. awk leads every row
// with its group.
var (
	shareSummaryHeaders = []string{"GROUP", "COORDINATOR", "STATE", "EPOCH", "ASSIGNMENT-EPOCH", "ASSIGNOR", "MEMBERS", "TOTAL-LAG", "ERROR", "MESSAGE"}
	shareMemberHeaders  = []string{"MEMBER-ID", "CLIENT-ID", "HOST", "RACK", "MEMBER-EPOCH", "SUBSCRIBED-TOPICS", "ASSIGNMENT"}
	shareOffsetHeaders  = []string{"TOPIC", "PARTITION", "START-OFFSET", "LEADER-EPOCH", "LAG", "ERROR", "MESSAGE"}
)

// normSection is the section a --section value names, or "" when it names
// none.
func normSection(section string) string {
	switch client.Strnorm(section) {
	case "summary", "members", "offsets":
		return client.Strnorm(section)
	}
	return ""
}

func awkHeaders(section string) []string {
	switch normSection(section) {
	case "summary":
		return shareSummaryHeaders
	case "members":
		return append([]string{"GROUP"}, shareMemberHeaders...)
	}
	return append([]string{"GROUP"}, shareOffsetHeaders...)
}

// shareGroup is one described share group with its offsets, as the printer
// sees it.
type shareGroup struct {
	coordinator int32
	group       kmsg.ShareGroupDescribeResponseGroup
	err         string // why the broker could not describe the group, or ""
	message     string // the text the broker attached to err, or ""
	offsets     []shareOffset
	totalLag    int64
	totalValid  bool // whether any partition reported a lag
}

type shareOffset struct {
	topic       string
	partition   int32
	startOffset int64
	leaderEpoch int32
	lag         int64 // -1 when the broker did not report one
	err         string
	message     string
}

func (o shareOffset) lagNum() any {
	if o.lag < 0 {
		return out.Unknown
	}
	return o.lag
}

func (o shareOffset) values() []any {
	return []any{o.topic, o.partition, o.startOffset, o.leaderEpoch, o.lagNum(), o.err, o.message}
}

func (o shareOffset) json() map[string]any {
	return map[string]any{
		"topic":        o.topic,
		"partition":    o.partition,
		"start_offset": o.startOffset,
		"leader_epoch": o.leaderEpoch,
		"lag":          o.lagNum(),
		"error":        o.err,
		"message":      o.message,
	}
}

func (g shareGroup) totalLagNum() any {
	if !g.totalValid {
		return out.Unknown
	}
	return g.totalLag
}

func (g shareGroup) summaryValues() []any {
	return []any{g.group.GroupID, g.coordinator, g.group.GroupState, g.group.GroupEpoch, g.group.AssignmentEpoch, g.group.Assignor, len(g.group.Members), g.totalLagNum(), g.err, g.message}
}

func memberValues(member kmsg.ShareGroupDescribeResponseGroupMember) []any {
	return []any{member.MemberID, member.ClientID, member.ClientHost, rackCell(member.RackID), member.MemberEpoch, strings.Join(member.SubscribedTopicNames, ","), formatShareMemberAssignment(member)}
}

// rackCell is the RACK cell: the rack a member reported, or Unknown for a
// member that reported none.
func rackCell(rack *string) any {
	if rack == nil {
		return out.Unknown
	}
	return *rack
}

func memberJSON(member kmsg.ShareGroupDescribeResponseGroupMember) map[string]any {
	subscribed := member.SubscribedTopicNames
	if subscribed == nil {
		subscribed = []string{}
	}
	return map[string]any{
		"member_id":         member.MemberID,
		"client_id":         member.ClientID,
		"host":              member.ClientHost,
		"rack":              rackCell(member.RackID),
		"member_epoch":      member.MemberEpoch,
		"subscribed_topics": subscribed,
		"assignment":        formatShareMemberAssignment(member),
	}
}

// errorCells are the ERROR and MESSAGE of a group or partition the broker
// answered with an error, "" and "" otherwise.
func errorCells(code int16, message *string) (string, string) {
	if code == 0 {
		return "", ""
	}
	return out.ErrName(code), out.BrokerMessage(message)
}

// describeShareGroups describes the groups and their offsets, sorted by
// group, partitions by topic then number.
func describeShareGroups(cl *client.Client, groups []string) []shareGroup {
	req := kmsg.NewPtrShareGroupDescribeRequest()
	req.GroupIDs = groups
	shards := cl.Client().RequestSharded(context.Background(), req)
	offsetsByGroup := fetchShareGroupOffsets(cl, groups)

	var described []shareGroup
	for _, shard := range shards {
		if shard.Err != nil {
			fmt.Fprintf(os.Stderr, "unable to issue ShareGroupDescribe to broker %d (%s:%d): %v\n", shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, shard.Err)
			continue
		}
		resp := shard.Resp.(*kmsg.ShareGroupDescribeResponse)
		for _, group := range resp.Groups {
			g := shareGroup{
				coordinator: shard.Meta.NodeID,
				group:       group,
				offsets:     []shareOffset{},
			}
			g.err, g.message = errorCells(group.ErrorCode, group.ErrorMessage)
			// The broker answers members in join order, which changes
			// from one run to the next.
			g.group.Members = slices.Clone(g.group.Members)
			slices.SortFunc(g.group.Members, func(a, b kmsg.ShareGroupDescribeResponseGroupMember) int {
				return strings.Compare(a.MemberID, b.MemberID)
			})
			if offsets, ok := offsetsByGroup[group.GroupID]; ok {
				for _, topic := range offsets.Topics {
					for _, p := range topic.Partitions {
						o := shareOffset{
							topic:       topic.Topic,
							partition:   p.Partition,
							startOffset: p.StartOffset,
							leaderEpoch: p.LeaderEpoch,
							lag:         p.Lag,
						}
						o.err, o.message = errorCells(p.ErrorCode, p.ErrorMessage)
						if o.lag >= 0 {
							g.totalLag += o.lag
							g.totalValid = true
						}
						g.offsets = append(g.offsets, o)
					}
				}
			}
			sort.Slice(g.offsets, func(i, j int) bool {
				if g.offsets[i].topic != g.offsets[j].topic {
					return g.offsets[i].topic < g.offsets[j].topic
				}
				return g.offsets[i].partition < g.offsets[j].partition
			})
			described = append(described, g)
		}
	}
	sort.SliceStable(described, func(i, j int) bool {
		return described[i].group.GroupID < described[j].group.GroupID
	})
	return described
}

func describeCommand(cl *client.Client) *cobra.Command {
	var section string
	var regex bool
	cmd := &cobra.Command{
		Use:     "describe GROUPS...",
		Aliases: []string{"d"},
		Short:   "Describe share groups with offsets and lag (Kafka 4.0+).",
		Long: `Describe share groups with offsets and lag (Kafka 4.0+).

Describe share groups (KIP-932, Kafka 4.0+).

If no groups are provided, all share groups are listed and then described.
The output includes group metadata, members, and per-partition start offsets
with lag.

Use --section to show only a specific section of the output:
  summary   group metadata, member count, total lag
  members   per-member detail (id, host, epoch, assignment)
  offsets   per-partition start offsets and lag

Defaults: text shows all sections, awk shows offsets. In awk, every row
begins with the group it belongs to.

EXAMPLES:
  kcl share-group describe                    # every share group
  kcl share-group describe sg1                # one group, every section
  kcl share-group describe sg1 --section offsets --format awk

SEE ALSO:
  kcl share-group list    list share groups
  kcl share-group seek    reset share group start offsets
  kcl consume --share-group   consume as a share group member
`,
		RunE: func(_ *cobra.Command, groups []string) error {
			if section != "" {
				if normSection(section) == "" {
					return out.Errf(out.ExitUsage, "invalid --section %q: must be summary, members, or offsets", section)
				}
				section = normSection(section)
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

			described := describeShareGroups(cl, groups)

			switch cl.Format() {
			case out.FormatJSON:
				jgroups := make([]map[string]any, 0, len(described))
				for _, g := range described {
					members := make([]map[string]any, 0, len(g.group.Members))
					for _, member := range g.group.Members {
						members = append(members, memberJSON(member))
					}
					offsets := make([]map[string]any, 0, len(g.offsets))
					for _, o := range g.offsets {
						offsets = append(offsets, o.json())
					}
					jgroups = append(jgroups, map[string]any{
						"group":            g.group.GroupID,
						"coordinator":      g.coordinator,
						"state":            g.group.GroupState,
						"epoch":            g.group.GroupEpoch,
						"assignment_epoch": g.group.AssignmentEpoch,
						"assignor":         g.group.Assignor,
						"members":          members,
						"total_lag":        g.totalLagNum(),
						"offsets":          offsets,
						"error":            g.err,
						"message":          g.message,
					})
				}
				out.MarshalJSON(cl.Command(), 1, map[string]any{
					"groups": jgroups,
				})

			case out.FormatAWK:
				sect := section
				if sect == "" {
					sect = "offsets"
				}
				table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, sect, awkHeaders(sect)...)
				for _, g := range described {
					switch sect {
					case "summary":
						table.Row(g.summaryValues()...)
					case "members":
						for _, member := range g.group.Members {
							table.Row(append([]any{g.group.GroupID}, memberValues(member)...)...)
						}
					case "offsets":
						for _, o := range g.offsets {
							table.Row(append([]any{g.group.GroupID}, o.values()...)...)
						}
					}
				}
				table.Flush()

			default:
				showSummary := section == "" || section == "summary"
				showMembers := section == "" || section == "members"
				showOffsets := section == "" || section == "offsets"

				for gi, g := range described {
					if showSummary {
						printShareGroupSummary(g)
					}

					// A section asked for by name prints its
					// header even with no rows; the default view
					// skips an empty table.
					if showMembers && (len(g.group.Members) > 0 || section == "members") {
						if showSummary {
							fmt.Println()
						}
						table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "members", shareMemberHeaders...)
						for _, member := range g.group.Members {
							table.Row(memberValues(member)...)
						}
						table.Flush()
					}

					if showOffsets && g.err == "" && (len(g.offsets) > 0 || section == "offsets") {
						if showSummary || showMembers && len(g.group.Members) > 0 {
							fmt.Println()
						}
						table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "offsets", shareOffsetHeaders...)
						for _, o := range g.offsets {
							table.Row(o.values()...)
						}
						table.Flush()
					}
					if gi < len(described)-1 {
						fmt.Println()
					}
				}
			}

			// A group the broker could not describe is printed with its
			// error and is a failure, as a missing topic is for topic
			// describe.
			for _, g := range described {
				if g.err != "" {
					return out.ErrSilent
				}
			}
			return nil
		},
	}
	out.ColumnsFunc(cmd, func() []string { return awkHeaders(section) })
	cmd.Flags().StringVar(&section, "section", "", "output section (summary, members, offsets; default: all for text, offsets for awk)")
	cmd.Flags().BoolVarP(&regex, "regex", "r", false, "treat group arguments as regular expressions")
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

func printShareGroupSummary(g shareGroup) {
	tw := out.NewTabWriter()
	fmt.Fprintf(tw, "GROUP\t%s\n", g.group.GroupID)
	fmt.Fprintf(tw, "COORDINATOR\t%d\n", g.coordinator)
	// If the group errored (e.g. GROUP_ID_NOT_FOUND), skip the empty
	// state/epoch/members fields and surface just the error.
	if g.err != "" {
		fmt.Fprintf(tw, "ERROR\t%s\n", g.err)
		if g.message != "" {
			fmt.Fprintf(tw, "MESSAGE\t%s\n", g.message)
		}
		tw.Flush()
		return
	}
	fmt.Fprintf(tw, "STATE\t%s\n", g.group.GroupState)
	fmt.Fprintf(tw, "EPOCH\t%d\n", g.group.GroupEpoch)
	fmt.Fprintf(tw, "ASSIGNMENT-EPOCH\t%d\n", g.group.AssignmentEpoch)
	fmt.Fprintf(tw, "ASSIGNOR\t%s\n", g.group.Assignor)
	fmt.Fprintf(tw, "MEMBERS\t%d\n", len(g.group.Members))
	fmt.Fprintf(tw, "TOTAL-LAG\t%v\n", g.totalLagNum())
	tw.Flush()
}

// formatShareMemberAssignment is the partitions a member owns as one field,
// "t:0,1;u:2", topics sorted and partitions ascending. Nothing owned is "".
func formatShareMemberAssignment(member kmsg.ShareGroupDescribeResponseGroupMember) string {
	assigned := make(map[string][]int32)
	for _, tp := range member.Assignment.TopicPartitions {
		name := tp.Topic
		if name == "" {
			name = fmt.Sprintf("%x", tp.TopicID)
		}
		assigned[name] = append(assigned[name], tp.Partitions...)
	}
	topics := make([]string, 0, len(assigned))
	for topic := range assigned {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	var parts []string
	for _, topic := range topics {
		ps := assigned[topic]
		sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
		strs := make([]string, len(ps))
		for i, p := range ps {
			strs[i] = fmt.Sprintf("%d", p)
		}
		parts = append(parts, topic+":"+strings.Join(strs, ","))
	}
	return strings.Join(parts, ";")
}

// deleteGroupResult returns what to print for one deleted share group: the
// ERROR column, "" when the delete succeeded, and the MESSAGE column. Kafka
// 4.4 attaches a message to a failed delete (KIP-1331); an older broker
// sends none and the message is empty.
func deleteGroupResult(g kmsg.DeleteGroupsResponseGroup) (errStr, message string) {
	return out.ErrName(g.ErrorCode), out.BrokerMessage(g.ErrorMessage)
}

func deleteCommand(cl *client.Client) *cobra.Command {
	var dryRun bool
	var useRegex bool
	cmd := &cobra.Command{
		Use:   "delete GROUPS...",
		Short: "Delete share groups (Kafka 4.0+).",
		Long: `Delete share groups (Kafka 4.0+).

Delete share groups (KIP-932, Kafka 4.0+).

The groups must be empty (no active consumers) to be deleted.

Use --regex to treat arguments as regex patterns: all share groups matching
any pattern will be deleted. Use --dry-run to see which groups would be deleted
without actually deleting them.

EXAMPLES:
  kcl share-group delete sg1 sg2                # delete two groups
  kcl share-group delete -r 'test-.*' --dry-run # print what the pattern matches

SEE ALSO:
  kcl share-group list            list share groups
  kcl share-group offset-delete   delete a share group's offsets for a topic
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
				}
			}
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results",
				"BROKER", "GROUP", "ERROR", "MESSAGE").ResultColumns()
			if dryRun {
				table.SetDryRun(true)
				for _, g := range args {
					table.Row(out.Unknown, g, out.Unknown, out.Unknown)
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
	cmd.Flags().BoolVarP(&useRegex, "regex", "r", false, "treat group arguments as regex patterns; match against all existing share groups")
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
