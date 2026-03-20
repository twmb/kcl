package group

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func describeCommand(cl *client.Client) *cobra.Command {
	var (
		readCommitted       bool
		useConsumerDescribe bool
		section             string
		regex               bool
	)

	cmd := &cobra.Command{
		Use:     "describe GROUPS...",
		Aliases: []string{"d"},
		Short:   "Describe consumer groups with lag",
		Long: `Describe consumer groups with per-partition lag.

By default, text format shows all sections (summary, lag, members). AWK
format defaults to the lag section. JSON always includes all sections.

Use --section to show only a specific part:
  --section summary   group metadata (state, balancer, member count, total lag)
  --section lag       per-partition committed offsets, end offsets, and lag
  --section members   member assignments

EXAMPLES:
  kcl group describe                          # all groups, all sections
  kcl group describe mygroup                  # specific group
  kcl group describe --section lag            # lag only
  kcl group describe --section summary        # summary only
  kcl group describe --consumer-protocol      # KIP-848 groups

SEE ALSO:
  kcl group list       list all groups
  kcl group seek       reset group offsets
  kcl consume -g       consume as a group member
`,
		RunE: func(_ *cobra.Command, groups []string) error {
			if err := validateSection(section); err != nil {
				return err
			}

			if regex {
				var err error
				groups, err = filterGroupsByRegex(cl, groups, listGroups)
				if err != nil {
					return err
				}
			}

			if useConsumerDescribe {
				return describeConsumerGroups(cl, groups, readCommitted, section)
			}

			if len(groups) == 0 {
				var err error
				groups, err = listGroups(cl)
				if err != nil {
					return err
				}
			}
			if len(groups) == 0 {
				return fmt.Errorf("no groups to describe")
			}

			described, err := describeClassicGroups(cl, groups)
			if err != nil {
				return err
			}
			fetchedOffsets, err := fetchOffsets(cl, groups)
			if err != nil {
				return err
			}
			listedOffsets, err := listOffsets(cl, described, fetchedOffsets, readCommitted)
			if err != nil {
				return err
			}
			return printDescribed(cl.Format(), described, fetchedOffsets, listedOffsets, section)
		},
	}

	cmd.Flags().BoolVar(&readCommitted, "committed", false, "use committed (read_committed) offsets for lag computation instead of latest")
	cmd.Flags().BoolVar(&useConsumerDescribe, "consumer-protocol", false, "use ConsumerGroupDescribe API for new consumer group protocol (KIP-848, Kafka 4.0+)")
	cmd.Flags().StringVar(&section, "section", "", "output section (summary, lag, members; default: all for text, lag for awk)")
	cmd.Flags().BoolVar(&regex, "regex", false, "treat group arguments as regular expressions")

	return cmd
}

// validateSection returns an error if section is not a recognized value.
func validateSection(section string) error {
	switch section {
	case "", "summary", "lag", "members":
		return nil
	default:
		return out.Errf(out.ExitUsage, "invalid --section %q: must be summary, lag, or members", section)
	}
}

func describeConsumerGroups(cl *client.Client, groups []string, readCommitted bool, section string) error {
	if len(groups) == 0 {
		var err error
		groups, err = listGroupsByType(cl, []string{"consumer"})
		if err != nil {
			return err
		}
	}
	if len(groups) == 0 {
		return fmt.Errorf("no consumer groups to describe")
	}

	req := kmsg.NewPtrConsumerGroupDescribeRequest()
	req.Groups = groups

	shards := cl.Client().RequestSharded(context.Background(), req)

	// Collect all described groups with their broker metadata.
	type consumerGroupInfo struct {
		broker int32
		group  kmsg.ConsumerGroupDescribeResponseGroup
	}
	var allGroups []consumerGroupInfo
	for _, shard := range shards {
		if shard.Err != nil {
			fmt.Fprintf(os.Stderr, "unable to issue ConsumerGroupDescribe to broker %d (%s:%d): %v\n", shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, shard.Err)
			continue
		}
		resp := shard.Resp.(*kmsg.ConsumerGroupDescribeResponse)
		for _, group := range resp.Groups {
			allGroups = append(allGroups, consumerGroupInfo{broker: shard.Meta.NodeID, group: group})
		}
	}

	// Build topic-partition set from member assignments for offset lookups.
	tps := make(map[string]map[int32]struct{})
	for _, gi := range allGroups {
		for _, member := range gi.group.Members {
			for _, tp := range member.Assignment.TopicPartitions {
				topic := tp.Topic
				if topic == "" {
					topic = fmt.Sprintf("%x", tp.TopicID)
				}
				if tps[topic] == nil {
					tps[topic] = make(map[int32]struct{})
				}
				for _, p := range tp.Partitions {
					tps[topic][p] = struct{}{}
				}
			}
		}
	}

	// Fetch committed offsets.
	fetchedOffsets, err := fetchOffsets(cl, groups)
	if err != nil {
		return err
	}

	// Also include partitions from committed offsets.
	for topic, parts := range fetchedOffsets {
		if tps[topic] == nil {
			tps[topic] = make(map[int32]struct{})
		}
		for p := range parts {
			tps[topic][p] = struct{}{}
		}
	}

	// List end offsets.
	listedOffsets, err := listOffsetsForTopicPartitions(cl, tps, readCommitted)
	if err != nil {
		return err
	}

	lookup := func(m map[string]map[int32]offset, topic string, partition int32) offset {
		p := m[topic]
		if p == nil {
			return offset{at: -1}
		}
		o, exists := p[partition]
		if !exists {
			return offset{at: -1}
		}
		return o
	}

	sort.Slice(allGroups, func(i, j int) bool {
		return allGroups[i].group.Group < allGroups[j].group.Group
	})

	format := cl.Format()

	// Build lag rows per group.
	type groupResult struct {
		broker int32
		group  kmsg.ConsumerGroupDescribeResponseGroup
		rows   []describeRow
	}
	var results []groupResult
	for _, gi := range allGroups {
		assigned := make(map[string]map[int32]*describeRow)
		for _, member := range gi.group.Members {
			for _, tp := range member.Assignment.TopicPartitions {
				topic := tp.Topic
				if topic == "" {
					topic = fmt.Sprintf("%x", tp.TopicID)
				}
				for _, p := range tp.Partitions {
					committed := lookup(fetchedOffsets, topic, p)
					end := lookup(listedOffsets, topic, p)
					row := &describeRow{
						topic:         topic,
						partition:     p,
						currentOffset: committed.at,
						logEndOffset:  end.at,
						memberID:      member.MemberID,
						clientID:      member.ClientID,
						host:          member.ClientHost,
						err:           committed.err,
					}
					if row.err == nil {
						row.err = end.err
					}
					if end.at >= 0 && committed.at >= 0 {
						row.lag = end.at - committed.at
						row.lagValid = true
					} else if end.at > 0 && committed.at == -1 {
						row.lag = end.at
						row.lagValid = true
					}
					if assigned[topic] == nil {
						assigned[topic] = make(map[int32]*describeRow)
					}
					assigned[topic][p] = row
				}
			}
		}
		// Add committed-but-unassigned partitions.
		for topic, parts := range fetchedOffsets {
			for p, committed := range parts {
				if assigned[topic] != nil {
					if _, ok := assigned[topic][p]; ok {
						continue
					}
				}
				end := lookup(listedOffsets, topic, p)
				row := &describeRow{
					topic:         topic,
					partition:     p,
					currentOffset: committed.at,
					logEndOffset:  end.at,
					err:           committed.err,
				}
				if row.err == nil {
					row.err = end.err
				}
				if end.at >= 0 && committed.at >= 0 {
					row.lag = end.at - committed.at
					row.lagValid = true
				} else if end.at > 0 && committed.at == -1 {
					row.lag = end.at
					row.lagValid = true
				}
				if assigned[topic] == nil {
					assigned[topic] = make(map[int32]*describeRow)
				}
				assigned[topic][p] = row
			}
		}
		var rows []describeRow
		for _, parts := range assigned {
			for _, row := range parts {
				rows = append(rows, *row)
			}
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].topic != rows[j].topic {
				return rows[i].topic < rows[j].topic
			}
			return rows[i].partition < rows[j].partition
		})
		results = append(results, groupResult{broker: gi.broker, group: gi.group, rows: rows})
	}

	switch format {
	case "json":
		var jsonGroups []map[string]any
		for _, r := range results {
			var totalLag int64
			for _, row := range r.rows {
				if row.lagValid {
					totalLag += row.lag
				}
			}
			lagRows := make([]map[string]any, 0, len(r.rows))
			for _, row := range r.rows {
				lagRows = append(lagRows, map[string]any{
					"topic":          row.topic,
					"partition":      row.partition,
					"current_offset": row.currentOffset,
					"log_end_offset": row.logEndOffset,
					"lag":            row.lag,
					"member_id":      row.memberID,
					"client_id":      row.clientID,
					"host":           row.host,
				})
			}
			members := make([]map[string]any, 0, len(r.group.Members))
			for _, member := range r.group.Members {
				m := map[string]any{
					"member_id":    member.MemberID,
					"client_id":    member.ClientID,
					"host":         member.ClientHost,
					"member_epoch": member.MemberEpoch,
					"assignment":   formatAssignment(member.Assignment),
				}
				if member.InstanceID != nil {
					m["instance_id"] = *member.InstanceID
				}
				members = append(members, m)
			}
			errStr := ""
			if e := kerr.ErrorForCode(r.group.ErrorCode); e != nil {
				errStr = e.Error()
				if r.group.ErrorMessage != nil {
					errStr += ": " + *r.group.ErrorMessage
				}
			}
			jsonGroups = append(jsonGroups, map[string]any{
				"group":       r.group.Group,
				"coordinator": r.broker,
				"state":       r.group.State,
				"balancer":    r.group.AssignorName,
				"members":     members,
				"total_lag":   totalLag,
				"lag":         lagRows,
				"error":       errStr,
			})
		}
		out.MarshalJSON("group.describe", 1, map[string]any{
			"groups": jsonGroups,
		})

	case "awk":
		sect := section
		if sect == "" {
			sect = "lag"
		}
		for _, r := range results {
			switch sect {
			case "summary":
				var totalLag int64
				for _, row := range r.rows {
					if row.lagValid {
						totalLag += row.lag
					}
				}
				errStr := ""
				if e := kerr.ErrorForCode(r.group.ErrorCode); e != nil {
					errStr = e.Error()
					if r.group.ErrorMessage != nil {
						errStr += ": " + *r.group.ErrorMessage
					}
				}
				fmt.Printf("%s\t%d\t%s\t%s\t%d\t%d\t%s\n",
					r.group.Group, r.broker, r.group.State, r.group.AssignorName,
					len(r.group.Members), totalLag, errStr)
			case "lag":
				for _, row := range r.rows {
					curStr := strconv.FormatInt(row.currentOffset, 10)
					if row.currentOffset < 0 {
						curStr = "-"
					}
					lagStr := strconv.FormatInt(row.lag, 10)
					if !row.lagValid {
						lagStr = "-"
					}
					fmt.Printf("%s\t%d\t%s\t%d\t%s\t%s\t%s\t%s\n",
						row.topic, row.partition, curStr, row.logEndOffset,
						lagStr, row.memberID, row.clientID, row.host)
				}
			case "members":
				for _, member := range r.group.Members {
					var extras []string
					if member.InstanceID != nil {
						extras = append(extras, "instance="+*member.InstanceID)
					}
					if member.RackID != nil {
						extras = append(extras, "rack="+*member.RackID)
					}
					host := member.ClientHost
					if len(extras) > 0 {
						host += " (" + strings.Join(extras, ",") + ")"
					}
					fmt.Printf("%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
						r.group.Group,
						member.MemberID,
						member.ClientID,
						host,
						member.MemberEpoch,
						strings.Join(member.SubscribedTopics, ","),
						formatAssignment(member.Assignment),
						formatAssignment(member.TargetAssignment),
					)
				}
			}
		}

	default: // text
		for gi, r := range results {
			showSummary := section == "" || section == "summary"
			showLag := section == "" || section == "lag"
			showMembers := section == "" || section == "members"

			var totalLag int64
			var totalLagValid bool
			for _, row := range r.rows {
				if row.lagValid {
					totalLag += row.lag
					totalLagValid = true
				}
			}

			if showSummary {
				tw := out.NewTabWriter()
				fmt.Fprintf(tw, "GROUP\t%s\n", r.group.Group)
				fmt.Fprintf(tw, "COORDINATOR\t%d\n", r.broker)
				fmt.Fprintf(tw, "STATE\t%s\n", r.group.State)
				fmt.Fprintf(tw, "BALANCER\t%s\n", r.group.AssignorName)
				fmt.Fprintf(tw, "MEMBERS\t%d\n", len(r.group.Members))
				if totalLagValid {
					fmt.Fprintf(tw, "TOTAL-LAG\t%d\n", totalLag)
				}
				if e := kerr.ErrorForCode(r.group.ErrorCode); e != nil {
					msg := e.Error()
					if r.group.ErrorMessage != nil {
						msg += ": " + *r.group.ErrorMessage
					}
					fmt.Fprintf(tw, "ERROR\t%s\n", msg)
				}
				tw.Flush()
			}

			if showLag && len(r.rows) > 0 {
				table := out.NewFormattedTable(format, "group.describe", 1, "lag",
					"TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "MEMBER-ID", "CLIENT-ID", "HOST")
				for _, row := range r.rows {
					curStr := strconv.FormatInt(row.currentOffset, 10)
					if row.currentOffset < 0 {
						curStr = "-"
					}
					lagStr := strconv.FormatInt(row.lag, 10)
					if !row.lagValid {
						lagStr = "-"
					}
					table.Row(row.topic, row.partition, curStr, row.logEndOffset, lagStr, row.memberID, row.clientID, row.host)
				}
				table.Flush()
			}

			if showMembers && len(r.group.Members) > 0 {
				table := out.NewFormattedTable(format, "group.describe-consumer", 1, "members",
					"MEMBER-ID", "CLIENT-ID", "HOST", "MEMBER-EPOCH", "SUBSCRIBED-TOPICS", "ASSIGNMENT", "TARGET-ASSIGNMENT")
				for _, member := range r.group.Members {
					var extras []string
					if member.InstanceID != nil {
						extras = append(extras, "instance="+*member.InstanceID)
					}
					if member.RackID != nil {
						extras = append(extras, "rack="+*member.RackID)
					}
					host := member.ClientHost
					if len(extras) > 0 {
						host += " (" + strings.Join(extras, ",") + ")"
					}
					table.Row(
						member.MemberID,
						member.ClientID,
						host,
						member.MemberEpoch,
						strings.Join(member.SubscribedTopics, ","),
						formatAssignment(member.Assignment),
						formatAssignment(member.TargetAssignment),
					)
				}
				table.Flush()
			}

			if gi < len(results)-1 {
				fmt.Println()
			}
		}
	}
	return nil
}

func formatAssignment(a kmsg.Assignment) string {
	var parts []string
	for _, tp := range a.TopicPartitions {
		name := tp.Topic
		if name == "" {
			name = fmt.Sprintf("%x", tp.TopicID)
		}
		ps := make([]string, len(tp.Partitions))
		for i, p := range tp.Partitions {
			ps[i] = fmt.Sprintf("%d", p)
		}
		parts = append(parts, name+":"+strings.Join(ps, ","))
	}
	return strings.Join(parts, " ")
}

func listGroupsByType(cl *client.Client, types []string) ([]string, error) {
	req := kmsg.NewPtrListGroupsRequest()
	req.TypesFilter = types

	shards := cl.Client().RequestSharded(context.Background(), req)
	var groups []string
	var failures int
	for _, shard := range shards {
		if shard.Err != nil {
			shardFail("ListGroups", shard, &failures)
			continue
		}
		resp := shard.Resp.(*kmsg.ListGroupsResponse)
		if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
			shardErr("ListGroups", shard, err)
			continue
		}
		for _, group := range resp.Groups {
			groups = append(groups, group.Group)
		}
	}
	if failures == len(shards) {
		return nil, fmt.Errorf("all %d ListGroups requests failed", failures)
	}
	return groups, nil
}

func shardFail(name string, shard kgo.ResponseShard, failures *int) {
	fmt.Fprintf(os.Stderr, "unable to issue %s to broker %d (%s:%d): %v\n", name, shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, shard.Err)
	*failures++
}

func shardErr(name string, shard kgo.ResponseShard, err error) {
	fmt.Fprintf(os.Stderr, "%s request error to broker %d (%s:%d): %v\n", name, shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, err)
}

func listGroups(cl *client.Client) ([]string, error) {
	return listGroupsByType(cl, []string{"classic", "consumer"})
}

func describeClassicGroups(cl *client.Client, groups []string) ([]describedGroup, error) {
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = groups

	shards := cl.Client().RequestSharded(context.Background(), req)
	var described []describedGroup
	var failures int
	for _, shard := range shards {
		if shard.Err != nil {
			shardFail("DescribeGroups", shard, &failures)
			continue
		}

		resp := unmarshalGroupDescribeMembers(shard.Meta, shard.Resp.(*kmsg.DescribeGroupsResponse))
		described = append(described, resp.Groups...)
	}
	if failures == len(shards) {
		return nil, fmt.Errorf("all %d DescribeGroups requests failed", failures)
	}
	return described, nil
}

type offset struct {
	at  int64
	err error
}

func fetchOffsets(cl *client.Client, groups []string) (map[string]map[int32]offset, error) {
	fetched := make(map[string]map[int32]offset)
	var failures int
	for i := range groups {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = groups[i]
		resp, err := req.RequestWith(context.Background(), cl.Client())
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to issue OffsetFetch: %v\n", err)
			failures++
			continue
		}

		for _, topic := range resp.Topics {
			topicOffsets := fetched[topic.Topic]
			if topicOffsets == nil {
				topicOffsets = make(map[int32]offset)
				fetched[topic.Topic] = topicOffsets
			}
			for _, partition := range topic.Partitions {
				topicOffsets[partition.Partition] = offset{
					at:  partition.Offset,
					err: kerr.ErrorForCode(partition.ErrorCode),
				}
			}
		}
	}
	if failures == len(groups) {
		return nil, fmt.Errorf("all %d OffsetFetch requests failed", failures)
	}
	return fetched, nil
}

// listOffsets lists end offsets for all topic-partitions that have committed
// offsets or member assignments.
func listOffsets(cl *client.Client, described []describedGroup, fetched map[string]map[int32]offset, readCommitted bool) (map[string]map[int32]offset, error) {
	tps := make(map[string]map[int32]struct{})

	// Include partitions from member assignments.
	for _, group := range described {
		for _, member := range group.Members {
			for _, topic := range member.MemberAssignment.Topics {
				if tps[topic.Topic] == nil {
					tps[topic.Topic] = make(map[int32]struct{})
				}
				for _, partition := range topic.Partitions {
					tps[topic.Topic][partition] = struct{}{}
				}
			}
		}
	}

	// Also include partitions from committed offsets (may include
	// partitions no longer assigned to any member).
	for topic, parts := range fetched {
		if tps[topic] == nil {
			tps[topic] = make(map[int32]struct{})
		}
		for p := range parts {
			tps[topic][p] = struct{}{}
		}
	}

	return listOffsetsForTopicPartitions(cl, tps, readCommitted)
}

// listOffsetsForTopicPartitions issues ListOffsets for the given topic-partition set.
func listOffsetsForTopicPartitions(cl *client.Client, tps map[string]map[int32]struct{}, readCommitted bool) (map[string]map[int32]offset, error) {
	if len(tps) == 0 {
		return nil, nil
	}

	req := kmsg.NewPtrListOffsetsRequest()
	if readCommitted {
		req.IsolationLevel = 1
	}
	for topic, partitions := range tps {
		reqTopic := kmsg.NewListOffsetsRequestTopic()
		reqTopic.Topic = topic
		for partition := range partitions {
			reqPartition := kmsg.NewListOffsetsRequestTopicPartition()
			reqPartition.Partition = partition
			reqPartition.Timestamp = -1 // latest
			reqTopic.Partitions = append(reqTopic.Partitions, reqPartition)
		}
		req.Topics = append(req.Topics, reqTopic)
	}

	shards := cl.Client().RequestSharded(context.Background(), req)
	listed := make(map[string]map[int32]offset)
	var failures int
	for _, shard := range shards {
		if shard.Err != nil {
			shardFail("ListOffsets", shard, &failures)
			continue
		}

		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		for _, topic := range resp.Topics {
			partOffsets := listed[topic.Topic]
			if partOffsets == nil {
				partOffsets = make(map[int32]offset)
				listed[topic.Topic] = partOffsets
			}
			for _, partition := range topic.Partitions {
				partOffsets[partition.Partition] = offset{
					at:  partition.Offset,
					err: kerr.ErrorForCode(partition.ErrorCode),
				}
			}
		}
	}
	if failures == len(shards) {
		return nil, fmt.Errorf("all %d ListOffsets requests failed", failures)
	}
	return listed, nil
}

type describeRow struct {
	topic         string
	partition     int32
	currentOffset int64
	logEndOffset  int64
	lag           int64
	lagValid      bool
	memberID      string
	instanceID    *string
	clientID      string
	host          string
	err           error
}

func printDescribed(
	format string,
	groups []describedGroup,
	fetched map[string]map[int32]offset,
	listed map[string]map[int32]offset,
	section string,
) error {
	lookup := func(m map[string]map[int32]offset, topic string, partition int32) offset {
		p := m[topic]
		if p == nil {
			return offset{at: -1}
		}
		o, exists := p[partition]
		if !exists {
			return offset{at: -1}
		}
		return o
	}

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Group < groups[j].Group
	})

	// Build rows for each group.
	type groupRows struct {
		group describedGroup
		rows  []describeRow
	}
	var allResults []groupRows
	for _, group := range groups {
		// Build rows from member assignments.
		assigned := make(map[string]map[int32]*describeRow) // topic -> partition -> row
		for _, member := range group.Members {
			for _, topic := range member.MemberAssignment.Topics {
				t := topic.Topic
				for _, p := range topic.Partitions {
					committed := lookup(fetched, t, p)
					end := lookup(listed, t, p)

					row := &describeRow{
						topic:         t,
						partition:     p,
						currentOffset: committed.at,
						logEndOffset:  end.at,
						memberID:      member.MemberID,
						instanceID:    member.InstanceID,
						clientID:      member.ClientID,
						host:          member.ClientHost,
						err:           committed.err,
					}
					if row.err == nil {
						row.err = end.err
					}

					if end.at >= 0 && committed.at >= 0 {
						row.lag = end.at - committed.at
						row.lagValid = true
					} else if end.at > 0 && committed.at == -1 {
						row.lag = end.at
						row.lagValid = true
					}

					if assigned[t] == nil {
						assigned[t] = make(map[int32]*describeRow)
					}
					assigned[t][p] = row
				}
			}
		}

		// Add committed-but-unassigned partitions.
		for topic, parts := range fetched {
			for p, committed := range parts {
				if assigned[topic] != nil {
					if _, ok := assigned[topic][p]; ok {
						continue
					}
				}
				end := lookup(listed, topic, p)
				row := &describeRow{
					topic:         topic,
					partition:     p,
					currentOffset: committed.at,
					logEndOffset:  end.at,
					err:           committed.err,
				}
				if row.err == nil {
					row.err = end.err
				}
				if end.at >= 0 && committed.at >= 0 {
					row.lag = end.at - committed.at
					row.lagValid = true
				} else if end.at > 0 && committed.at == -1 {
					row.lag = end.at
					row.lagValid = true
				}
				if assigned[topic] == nil {
					assigned[topic] = make(map[int32]*describeRow)
				}
				assigned[topic][p] = row
			}
		}

		// Flatten to slice.
		var rows []describeRow
		for _, parts := range assigned {
			for _, row := range parts {
				rows = append(rows, *row)
			}
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].topic != rows[j].topic {
				return rows[i].topic < rows[j].topic
			}
			return rows[i].partition < rows[j].partition
		})

		allResults = append(allResults, groupRows{group: group, rows: rows})
	}

	switch format {
	case "json":
		var jsonGroups []map[string]any
		for _, gr := range allResults {
			group := gr.group
			rows := gr.rows

			var totalLag int64
			for _, r := range rows {
				if r.lagValid {
					totalLag += r.lag
				}
			}

			lagRows := make([]map[string]any, 0, len(rows))
			for _, r := range rows {
				lagRows = append(lagRows, map[string]any{
					"topic":          r.topic,
					"partition":      r.partition,
					"current_offset": r.currentOffset,
					"log_end_offset": r.logEndOffset,
					"lag":            r.lag,
					"member_id":      r.memberID,
					"client_id":      r.clientID,
					"host":           r.host,
				})
			}

			members := make([]map[string]any, 0, len(group.Members))
			for _, member := range group.Members {
				m := map[string]any{
					"member_id": member.MemberID,
					"client_id": member.ClientID,
					"host":      member.ClientHost,
				}
				if member.InstanceID != nil {
					m["instance_id"] = *member.InstanceID
				}
				var assignedTopics []string
				for _, topic := range member.MemberAssignment.Topics {
					assignedTopics = append(assignedTopics, topic.Topic)
				}
				m["assigned_topics"] = assignedTopics
				members = append(members, m)
			}

			errStr := ""
			if e := kerr.ErrorForCode(group.ErrorCode); e != nil {
				errStr = e.Error()
				if group.ErrorMessage != nil {
					errStr += ": " + *group.ErrorMessage
				}
			}

			jsonGroups = append(jsonGroups, map[string]any{
				"group":       group.Group,
				"coordinator": group.Broker.NodeID,
				"state":       group.State,
				"balancer":    group.Protocol,
				"members":     members,
				"total_lag":   totalLag,
				"lag":         lagRows,
				"error":       errStr,
			})
		}
		out.MarshalJSON("group.describe", 1, map[string]any{
			"groups": jsonGroups,
		})

	case "awk":
		sect := section
		if sect == "" {
			sect = "lag"
		}
		for _, gr := range allResults {
			group := gr.group
			rows := gr.rows

			switch sect {
			case "summary":
				var totalLag int64
				for _, r := range rows {
					if r.lagValid {
						totalLag += r.lag
					}
				}
				errStr := ""
				if e := kerr.ErrorForCode(group.ErrorCode); e != nil {
					errStr = e.Error()
					if group.ErrorMessage != nil {
						errStr += ": " + *group.ErrorMessage
					}
				}
				fmt.Printf("%s\t%d\t%s\t%s\t%d\t%d\t%s\n",
					group.Group, group.Broker.NodeID, group.State, group.Protocol,
					len(group.Members), totalLag, errStr)

			case "lag":
				for _, r := range rows {
					curStr := strconv.FormatInt(r.currentOffset, 10)
					if r.currentOffset < 0 {
						curStr = "-"
					}
					lagStr := strconv.FormatInt(r.lag, 10)
					if !r.lagValid {
						lagStr = "-"
					}
					fmt.Printf("%s\t%d\t%s\t%d\t%s\t%s\t%s\t%s\n",
						r.topic, r.partition, curStr, r.logEndOffset,
						lagStr, r.memberID, r.clientID, r.host)
				}

			case "members":
				for _, member := range group.Members {
					host := member.ClientHost
					if member.InstanceID != nil {
						host += " (instance=" + *member.InstanceID + ")"
					}
					var parts []string
					for _, topic := range member.MemberAssignment.Topics {
						ps := make([]string, len(topic.Partitions))
						for i, p := range topic.Partitions {
							ps[i] = fmt.Sprintf("%d", p)
						}
						parts = append(parts, topic.Topic+":"+strings.Join(ps, ","))
					}
					fmt.Printf("%s\t%s\t%s\t%s\n",
						member.MemberID, member.ClientID, host, strings.Join(parts, " "))
				}
			}
		}

	default: // text
		for gi, gr := range allResults {
			group := gr.group
			rows := gr.rows

			showSummary := section == "" || section == "summary"
			showLag := section == "" || section == "lag"
			showMembers := section == "" || section == "members"

			// Compute total lag for the summary.
			var totalLag int64
			var totalLagValid bool
			for _, r := range rows {
				if r.lagValid {
					totalLag += r.lag
					totalLagValid = true
				}
			}

			// Summary section.
			if showSummary {
				tw := out.NewTabWriter()
				fmt.Fprintf(tw, "GROUP\t%s\n", group.Group)
				fmt.Fprintf(tw, "COORDINATOR\t%d\n", group.Broker.NodeID)
				fmt.Fprintf(tw, "STATE\t%s\n", group.State)
				fmt.Fprintf(tw, "BALANCER\t%s\n", group.Protocol)
				fmt.Fprintf(tw, "MEMBERS\t%d\n", len(group.Members))
				if totalLagValid {
					fmt.Fprintf(tw, "TOTAL-LAG\t%d\n", totalLag)
				}
				if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
					msg := err.Error()
					if group.ErrorMessage != nil {
						msg += ": " + *group.ErrorMessage
					}
					fmt.Fprintf(tw, "ERROR\t%s\n", msg)
				}
				tw.Flush()
			}

			// Lag section.
			if showLag && len(rows) > 0 {
				table := out.NewFormattedTable(format, "group.describe", 1, "lag",
					"TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "MEMBER-ID", "CLIENT-ID", "HOST")
				for _, r := range rows {
					curStr := strconv.FormatInt(r.currentOffset, 10)
					if r.currentOffset < 0 {
						curStr = "-"
					}
					lagStr := strconv.FormatInt(r.lag, 10)
					if !r.lagValid {
						lagStr = "-"
					}
					table.Row(r.topic, r.partition, curStr, r.logEndOffset, lagStr, r.memberID, r.clientID, r.host)
				}
				table.Flush()
			}

			// Members section.
			if showMembers && len(group.Members) > 0 {
				table := out.NewFormattedTable(format, "group.describe", 1, "members",
					"MEMBER-ID", "CLIENT-ID", "HOST", "ASSIGNMENT")
				for _, member := range group.Members {
					host := member.ClientHost
					if member.InstanceID != nil {
						host += " (instance=" + *member.InstanceID + ")"
					}
					var parts []string
					for _, topic := range member.MemberAssignment.Topics {
						ps := make([]string, len(topic.Partitions))
						for i, p := range topic.Partitions {
							ps[i] = fmt.Sprintf("%d", p)
						}
						parts = append(parts, topic.Topic+":"+strings.Join(ps, ","))
					}
					table.Row(member.MemberID, member.ClientID, host, strings.Join(parts, " "))
				}
				table.Flush()
			}

			if gi < len(allResults)-1 {
				fmt.Println()
			}
		}
	}
	return nil
}

type describedGroupMember struct {
	MemberID         string
	InstanceID       *string
	ClientID         string
	ClientHost       string
	MemberMetadata   kmsg.ConsumerMemberMetadata
	MemberAssignment kmsg.ConsumerMemberAssignment
}

type describedGroup struct {
	Broker               kgo.BrokerMetadata
	ErrorCode            int16
	ErrorMessage         *string
	Group                string
	State                string
	ProtocolType         string
	Protocol             string
	Members              []describedGroupMember
	AuthorizedOperations int32
}

type describeGroupsResponse struct {
	ThrottleMillis int32
	Groups         []describedGroup
}

func unmarshalGroupDescribeMembers(
	meta kgo.BrokerMetadata, resp *kmsg.DescribeGroupsResponse,
) *describeGroupsResponse {
	dresp := &describeGroupsResponse{
		ThrottleMillis: resp.ThrottleMillis,
	}
	for _, group := range resp.Groups {
		dgroup := describedGroup{
			Broker:               meta,
			ErrorCode:            group.ErrorCode,
			ErrorMessage:         group.ErrorMessage,
			Group:                group.Group,
			State:                group.State,
			ProtocolType:         group.ProtocolType,
			Protocol:             group.Protocol,
			AuthorizedOperations: group.AuthorizedOperations,
		}
		for _, member := range group.Members {
			dmember := describedGroupMember{
				MemberID:   member.MemberID,
				InstanceID: member.InstanceID,
				ClientID:   member.ClientID,
				ClientHost: member.ClientHost,
			}
			dmember.MemberMetadata.ReadFrom(member.ProtocolMetadata)
			dmember.MemberAssignment.ReadFrom(member.MemberAssignment)

			dgroup.Members = append(dgroup.Members, dmember)
		}
		dresp.Groups = append(dresp.Groups, dgroup)
	}

	return dresp
}

// filterGroupsByRegex lists all groups using listFn, compiles each pattern
// argument as a regex, and returns only groups matching at least one pattern.
func filterGroupsByRegex(cl *client.Client, patterns []string, listFn func(*client.Client) ([]string, error)) ([]string, error) {
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

	all, err := listFn(cl)
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
