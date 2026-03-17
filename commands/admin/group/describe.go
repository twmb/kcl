package group

import (
	"context"
	"fmt"
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
		readCommitted      bool
		useConsumerDescribe bool
		showSummary        bool
		showMembers        bool
		showLag            bool
		lagPerTopic        bool
		lagFilter          string
	)

	cmd := &cobra.Command{
		Use:     "describe GROUPS...",
		Aliases: []string{"d"},
		Short:   "Describe consumer groups with lag",
		Long: `Describe consumer groups with per-partition lag.

By default, shows a summary section followed by a per-partition lag table
with committed offsets, log-end offsets, lag, and member assignments. If no
groups are provided, all non-share groups are listed and described.

Use section flags to show only specific parts:
  -s  summary only (group, state, balancer, members count)
  -m  members/assignments only
  -l  lag/offsets only

EXAMPLES:
  kcl group describe                       # all groups
  kcl group describe mygroup               # specific group
  kcl group describe -l --lag-filter '>0'  # only partitions with lag
  kcl group describe --lag-per-topic       # lag aggregated per topic
  kcl group describe --consumer-protocol   # KIP-848 groups

SEE ALSO:
  kcl group list       list all groups
  kcl group seek       reset group offsets
  kcl consume -g       consume as a group member
`,
		Run: func(_ *cobra.Command, groups []string) {
			if useConsumerDescribe {
				describeConsumerGroups(cl, groups, readCommitted, showSummary, showMembers, showLag, lagPerTopic, lagFilter)
				return
			}

			if len(groups) == 0 {
				groups = listGroups(cl)
			}
			if len(groups) == 0 {
				out.Die("no groups to describe")
			}

			described := describeClassicGroups(cl, groups)
			fetchedOffsets := fetchOffsets(cl, groups)
			listedOffsets := listOffsets(cl, described, fetchedOffsets, readCommitted)
			printDescribed(described, fetchedOffsets, listedOffsets, showSummary, showMembers, showLag, lagPerTopic, lagFilter)
		},
	}

	cmd.Flags().BoolVar(&readCommitted, "committed", false, "use committed (read_committed) offsets for lag computation instead of latest")
	cmd.Flags().BoolVar(&useConsumerDescribe, "consumer-protocol", false, "use ConsumerGroupDescribe API for new consumer group protocol (KIP-848, Kafka 4.0+)")
	cmd.Flags().BoolVarP(&showSummary, "summary", "s", false, "show only the summary section")
	cmd.Flags().BoolVarP(&showMembers, "members", "m", false, "show only the members section")
	cmd.Flags().BoolVarP(&showLag, "lag", "l", false, "show only the lag section")
	cmd.Flags().BoolVar(&lagPerTopic, "lag-per-topic", false, "aggregate and print lag summed per topic")
	cmd.Flags().StringVar(&lagFilter, "lag-filter", "", "filter partitions by lag (>N, >=N, <N, <=N, =N)")

	return cmd
}

// parseLagFilter parses a lag filter expression like ">0", ">=1000", "=0", "<100".
func parseLagFilter(expr string) (func(lag int64) bool, error) {
	if expr == "" {
		return nil, nil
	}
	expr = strings.TrimSpace(expr)
	var op string
	var valStr string
	switch {
	case strings.HasPrefix(expr, ">="):
		op, valStr = ">=", expr[2:]
	case strings.HasPrefix(expr, "<="):
		op, valStr = "<=", expr[2:]
	case strings.HasPrefix(expr, ">"):
		op, valStr = ">", expr[1:]
	case strings.HasPrefix(expr, "<"):
		op, valStr = "<", expr[1:]
	case strings.HasPrefix(expr, "="):
		op, valStr = "=", expr[1:]
	default:
		// Bare number treated as >=N.
		op, valStr = ">=", expr
	}
	n, err := strconv.ParseInt(strings.TrimSpace(valStr), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid lag filter %q: %w", expr, err)
	}
	switch op {
	case ">":
		return func(lag int64) bool { return lag > n }, nil
	case ">=":
		return func(lag int64) bool { return lag >= n }, nil
	case "<":
		return func(lag int64) bool { return lag < n }, nil
	case "<=":
		return func(lag int64) bool { return lag <= n }, nil
	case "=":
		return func(lag int64) bool { return lag == n }, nil
	}
	return nil, fmt.Errorf("unknown lag filter operator in %q", expr)
}

// anySection returns true if any section flag is set; false means show all.
func anySection(s, m, l bool) bool { return s || m || l }

func describeConsumerGroups(cl *client.Client, groups []string, readCommitted, showSummary, showMembers, showLag bool, lagPerTopic bool, lagFilter string) {
	if len(groups) == 0 {
		groups = listGroupsByType(cl, []string{"consumer"})
	}
	if len(groups) == 0 {
		out.Die("no consumer groups to describe")
	}

	req := kmsg.NewPtrConsumerGroupDescribeRequest()
	req.Groups = groups

	shards := cl.Client().RequestSharded(context.Background(), req)
	if cl.AsJSON() {
		out.ExitJSON(shards)
	}

	for _, shard := range shards {
		if shard.Err != nil {
			fmt.Printf("unable to issue ConsumerGroupDescribe to broker %d (%s:%d): %v\n", shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, shard.Err)
			continue
		}

		resp := shard.Resp.(*kmsg.ConsumerGroupDescribeResponse)
		for _, group := range resp.Groups {
			printConsumerGroup(shard.Meta.NodeID, group)
			fmt.Println()
		}
	}
}

func printConsumerGroup(broker int32, group kmsg.ConsumerGroupDescribeResponseGroup) {
	tw := out.NewTabWriter()
	fmt.Fprintf(tw, "GROUP\t%s\n", group.Group)
	fmt.Fprintf(tw, "COORDINATOR\t%d\n", broker)
	fmt.Fprintf(tw, "STATE\t%s\n", group.State)
	fmt.Fprintf(tw, "EPOCH\t%d\n", group.Epoch)
	fmt.Fprintf(tw, "ASSIGNMENT EPOCH\t%d\n", group.AssignmentEpoch)
	fmt.Fprintf(tw, "ASSIGNOR\t%s\n", group.AssignorName)
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
		"TARGET-ASSIGNMENT",
	}
	table := out.NewTable(headers...)
	defer table.Flush()
	for _, member := range group.Members {
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

		table.Print(
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

func listGroupsByType(cl *client.Client, types []string) []string {
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
		out.Die("all %d ListGroups requests failed", failures)
	}
	return groups
}

func shardFail(name string, shard kgo.ResponseShard, failures *int) {
	fmt.Printf("unable to issue %s to broker %d (%s:%d): %v\n", name, shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, shard.Err)
	*failures++
}

func shardErr(name string, shard kgo.ResponseShard, err error) {
	fmt.Printf("%s request error to broker %d (%s:%d): %v\n", name, shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, err)
}

func listGroups(cl *client.Client) []string {
	return listGroupsByType(cl, []string{"classic", "consumer"})
}

func describeClassicGroups(cl *client.Client, groups []string) []describedGroup {
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
		out.Die("all %d DescribeGroups requests failed", failures)
	}
	return described
}

type offset struct {
	at  int64
	err error
}

func fetchOffsets(cl *client.Client, groups []string) map[string]map[int32]offset {
	fetched := make(map[string]map[int32]offset)
	var failures int
	for i := range groups {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = groups[i]
		resp, err := req.RequestWith(context.Background(), cl.Client())
		if err != nil {
			fmt.Printf("unable to issue OffsetFetch: %v\n", err)
			failures++
			continue
		}

		for _, topic := range resp.Topics {
			fetchedt := fetched[topic.Topic]
			if fetchedt == nil {
				fetchedt = make(map[int32]offset)
				fetched[topic.Topic] = fetchedt
			}
			for _, partition := range topic.Partitions {
				fetchedt[partition.Partition] = offset{
					at:  partition.Offset,
					err: kerr.ErrorForCode(partition.ErrorCode),
				}
			}
		}
	}
	if failures == len(groups) {
		out.Die("all %d OffsetFetch requests failed", failures)
	}
	return fetched
}

// listOffsets lists end offsets for all topic-partitions that have committed
// offsets or member assignments.
func listOffsets(cl *client.Client, described []describedGroup, fetched map[string]map[int32]offset, readCommitted bool) map[string]map[int32]offset {
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

	if len(tps) == 0 {
		return nil
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
			listedt := listed[topic.Topic]
			if listedt == nil {
				listedt = make(map[int32]offset)
				listed[topic.Topic] = listedt
			}
			for _, partition := range topic.Partitions {
				listedt[partition.Partition] = offset{
					at:  partition.Offset,
					err: kerr.ErrorForCode(partition.ErrorCode),
				}
			}
		}
	}
	if failures == len(shards) {
		out.Die("all %d ListOffsets requests failed", failures)
	}
	return listed
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
	groups []describedGroup,
	fetched map[string]map[int32]offset,
	listed map[string]map[int32]offset,
	showSummary, showMembers, showLag bool,
	lagPerTopic bool,
	lagFilterExpr string,
) {
	lagFn, err := parseLagFilter(lagFilterExpr)
	if err != nil {
		out.Die("%v", err)
	}

	showAll := !anySection(showSummary, showMembers, showLag)

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

	for gi, group := range groups {
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

		// Apply lag filter.
		if lagFn != nil {
			var filtered []describeRow
			for _, r := range rows {
				if r.lagValid && lagFn(r.lag) {
					filtered = append(filtered, r)
				}
			}
			rows = filtered
		}

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
		if showAll || showSummary {
			tw := out.NewTabWriter()
			fmt.Fprintf(tw, "GROUP\t%s\n", group.Group)
			fmt.Fprintf(tw, "COORDINATOR\t%d\n", group.Broker.NodeID)
			fmt.Fprintf(tw, "STATE\t%s\n", group.State)
			fmt.Fprintf(tw, "BALANCER\t%s\n", group.Protocol)
			fmt.Fprintf(tw, "MEMBERS\t%d\n", len(group.Members))
			if totalLagValid {
				fmt.Fprintf(tw, "TOTAL LAG\t%d\n", totalLag)
			}
			if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
				fmt.Fprintf(tw, "ERROR\t%s\n", err)
			}
			tw.Flush()
		}

		// Lag section.
		if (showAll || showLag) && !lagPerTopic && len(rows) > 0 {
			sort.Slice(rows, func(i, j int) bool {
				if rows[i].topic != rows[j].topic {
					return rows[i].topic < rows[j].topic
				}
				return rows[i].partition < rows[j].partition
			})

			table := out.NewTable("TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "MEMBER-ID", "CLIENT-ID", "HOST")
			for _, r := range rows {
				curStr := strconv.FormatInt(r.currentOffset, 10)
				if r.currentOffset < 0 {
					curStr = "-"
				}
				lagStr := strconv.FormatInt(r.lag, 10)
				if !r.lagValid {
					lagStr = "-"
				}
				table.Print(r.topic, r.partition, curStr, r.logEndOffset, lagStr, r.memberID, r.clientID, r.host)
			}
			table.Flush()
		}

		// Lag-per-topic section.
		if (showAll || showLag) && lagPerTopic && len(rows) > 0 {
			type topicLag struct {
				topic string
				lag   int64
			}
			lagMap := make(map[string]int64)
			for _, r := range rows {
				if r.lagValid {
					lagMap[r.topic] += r.lag
				}
			}
			var tls []topicLag
			for t, l := range lagMap {
				tls = append(tls, topicLag{t, l})
			}
			sort.Slice(tls, func(i, j int) bool { return tls[i].topic < tls[j].topic })

			table := out.NewTable("TOPIC", "LAG")
			for _, tl := range tls {
				table.Print(tl.topic, tl.lag)
			}
			table.Flush()
		}

		if gi < len(groups)-1 {
			fmt.Println()
		}
	}
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
