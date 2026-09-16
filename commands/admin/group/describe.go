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
		lagExpr             string
	)

	cmd := &cobra.Command{
		Use:     "describe GROUPS...",
		Aliases: []string{"d"},
		Short:   "Describe consumer groups with lag.",
		Long: `Describe consumer groups with lag.

Describe consumer groups with per-partition lag.

By default, text format shows all sections (summary, lag, members). AWK
format defaults to the lag section. JSON always includes all sections.

Use --section to show only a specific part:
  --section summary   group metadata (state, balancer, member count, total lag)
  --section lag       per-partition committed, log start, and log end offsets, and lag
  --section members   member assignments

In awk format, every lag row begins with the group it belongs to, so rows
from several groups can be told apart.

Use --lag to keep only the partitions whose lag matches: '>0' for the
partitions that are behind, '>=1000' or '1000' for those at least a
thousand behind, '<10', '<=10', or '=0'. A partition whose lag we could
not compute, printed as a dash, never matches. A group left with no
matching partitions is left out entirely, so describing every group with
--lag '>0' lists only the groups that are behind. A group that survives
prints its summary and members whole, and TOTAL-LAG stays the group's
full total.

EXAMPLES:
  kcl group describe                          # all groups, all sections
  kcl group describe mygroup                  # specific group
  kcl group describe --section lag            # lag only
  kcl group describe --section summary        # summary only
  kcl group describe --lag '>0'               # only the groups that are behind
  kcl group describe --lag '>=1000' -r 'prod-.*'  # far behind, by group regex
  kcl group describe --consumer-protocol      # KIP-848 groups

SEE ALSO:
  kcl group list       list all groups
  kcl group seek       reset group offsets
  kcl consume -g       consume as a group member
`,
		RunE: func(cmd *cobra.Command, groups []string) error {
			if err := validateSection(section); err != nil {
				return err
			}
			opts := describeOpts{section: section}
			if cmd.Flags().Changed("lag") {
				var err error
				if opts.lag, err = parseLagFilter(lagExpr); err != nil {
					return err
				}
			}

			if regex {
				var err error
				groups, err = filterGroupsByRegex(cl, groups, listGroups)
				if err != nil {
					return err
				}
			}

			if useConsumerDescribe {
				return describeConsumerGroups(cl, groups, readCommitted, opts)
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
			tps := make(map[string]map[int32]struct{})
			for _, group := range described {
				for _, member := range group.Members {
					for _, topic := range member.MemberAssignment.Topics {
						addPartitions(tps, topic.Topic, topic.Partitions)
					}
				}
			}
			starts, ends, err := listOffsets(cl, tps, fetchedOffsets, readCommitted)
			if err != nil {
				return err
			}

			sort.Slice(described, func(i, j int) bool {
				return described[i].Group < described[j].Group
			})
			var printed []printGroup
			for _, group := range described {
				printed = append(printed, classicPrintGroup(group, fetchedOffsets[group.Group], starts, ends))
			}
			printGroups(cl.Format(), cl.Command(), opts, printed)

			// A group the broker could not describe, GROUP_ID_NOT_FOUND above
			// all, is printed with its error and is a failure, as a missing
			// topic is for topic describe.
			for _, d := range described {
				if d.ErrorCode != 0 {
					return out.ErrSilent
				}
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&readCommitted, "committed", false, "use committed (read_committed) offsets for lag computation instead of latest")
	cmd.Flags().BoolVar(&useConsumerDescribe, "consumer-protocol", false, "use ConsumerGroupDescribe API for new consumer group protocol (KIP-848, Kafka 4.0+)")
	cmd.Flags().StringVar(&section, "section", "", "output section (summary, lag, members; default: all for text, lag for awk)")
	cmd.Flags().BoolVarP(&regex, "regex", "r", false, "treat group arguments as regular expressions")
	cmd.Flags().StringVar(&lagExpr, "lag", "", "keep only partitions whose lag matches (>N, >=N, <N, <=N, =N, or N for >=N); a group with none left is dropped")

	return cmd
}

// describeOpts are the flags that shape what printGroups prints.
type describeOpts struct {
	section string
	lag     *lagFilter // nil when --lag is not set
}

// lagFilter is a parsed --lag expression: an operator and the number it
// compares lag against.
type lagFilter struct {
	op string
	n  int64
}

var lagExprRe = regexp.MustCompile(`^\s*(>=|<=|>|<|=)?\s*(-?\d+)\s*$`)

func parseLagFilter(expr string) (*lagFilter, error) {
	m := lagExprRe.FindStringSubmatch(expr)
	if m == nil {
		return nil, out.Errf(out.ExitUsage, "invalid --lag %q: want >N, >=N, <N, <=N, =N, or N alone for >=N", expr)
	}
	n, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		return nil, out.Errf(out.ExitUsage, "invalid --lag %q: %v", expr, err)
	}
	op := m[1]
	if op == "" {
		op = ">="
	}
	return &lagFilter{op: op, n: n}, nil
}

// matches is whether a row with this lag survives the filter. A nil filter
// keeps every row; an invalid lag matches nothing.
func (f *lagFilter) matches(lag int64, valid bool) bool {
	if f == nil {
		return true
	}
	if !valid {
		return false
	}
	switch f.op {
	case ">":
		return lag > f.n
	case ">=":
		return lag >= f.n
	case "<":
		return lag < f.n
	case "<=":
		return lag <= f.n
	default:
		return lag == f.n
	}
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

func describeError(code int16, message *string) string {
	e := kerr.ErrorForCode(code)
	if e == nil {
		return ""
	}
	s := e.Error()
	if message != nil {
		s += ": " + *message
	}
	return s
}

func classicPrintGroup(group describedGroup, fetched, starts, ends map[string]map[int32]offset) printGroup {
	var members []rowMember
	for _, member := range group.Members {
		m := rowMember{
			memberID:   member.MemberID,
			instanceID: member.InstanceID,
			clientID:   member.ClientID,
			host:       member.ClientHost,
			assigned:   make(map[string][]int32),
		}
		for _, topic := range member.MemberAssignment.Topics {
			m.assigned[topic.Topic] = append(m.assigned[topic.Topic], topic.Partitions...)
		}
		members = append(members, m)
	}

	pg := printGroup{
		group:         group.Group,
		coordinator:   group.Broker.NodeID,
		state:         group.State,
		balancer:      group.Protocol,
		err:           describeError(group.ErrorCode, group.ErrorMessage),
		nMembers:      len(group.Members),
		rows:          buildRows(members, fetched, starts, ends),
		memberHeaders: []string{"MEMBER-ID", "CLIENT-ID", "HOST", "ASSIGNMENT"},
		memberJSON:    make([]map[string]any, 0, len(group.Members)),
	}
	for _, member := range group.Members {
		host := member.ClientHost
		if member.InstanceID != nil {
			host += " (instance=" + *member.InstanceID + ")"
		}
		var parts []string
		var assignedTopics []string
		for _, topic := range member.MemberAssignment.Topics {
			ps := make([]string, len(topic.Partitions))
			for i, p := range topic.Partitions {
				ps[i] = fmt.Sprintf("%d", p)
			}
			parts = append(parts, topic.Topic+":"+strings.Join(ps, ","))
			assignedTopics = append(assignedTopics, topic.Topic)
		}
		row := []any{member.MemberID, member.ClientID, host, strings.Join(parts, " ")}
		pg.memberRows = append(pg.memberRows, row)
		pg.awkMemberRows = append(pg.awkMemberRows, row)

		m := map[string]any{
			"member_id":       member.MemberID,
			"client_id":       member.ClientID,
			"host":            member.ClientHost,
			"assigned_topics": assignedTopics,
		}
		if member.InstanceID != nil {
			m["instance_id"] = *member.InstanceID
		}
		pg.memberJSON = append(pg.memberJSON, m)
	}
	return pg
}

func describeConsumerGroups(cl *client.Client, groups []string, readCommitted bool, opts describeOpts) error {
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

	var assignments []*kmsg.Assignment
	for i := range allGroups {
		for j := range allGroups[i].group.Members {
			m := &allGroups[i].group.Members[j]
			assignments = append(assignments, &m.Assignment, &m.TargetAssignment)
		}
	}
	nameAssignedTopics(cl, assignments)

	// Build topic-partition set from member assignments for offset lookups.
	tps := make(map[string]map[int32]struct{})
	for _, gi := range allGroups {
		for _, member := range gi.group.Members {
			for _, tp := range member.Assignment.TopicPartitions {
				addPartitions(tps, assignmentTopic(tp), tp.Partitions)
			}
		}
	}

	fetchedOffsets, err := fetchOffsets(cl, groups)
	if err != nil {
		return err
	}
	starts, ends, err := listOffsets(cl, tps, fetchedOffsets, readCommitted)
	if err != nil {
		return err
	}

	sort.Slice(allGroups, func(i, j int) bool {
		return allGroups[i].group.Group < allGroups[j].group.Group
	})

	var printed []printGroup
	for _, gi := range allGroups {
		g := gi.group
		var members []rowMember
		for _, member := range g.Members {
			m := rowMember{
				memberID:   member.MemberID,
				instanceID: member.InstanceID,
				clientID:   member.ClientID,
				host:       member.ClientHost,
				assigned:   make(map[string][]int32),
			}
			for _, tp := range member.Assignment.TopicPartitions {
				topic := assignmentTopic(tp)
				m.assigned[topic] = append(m.assigned[topic], tp.Partitions...)
			}
			members = append(members, m)
		}

		pg := printGroup{
			group:         g.Group,
			coordinator:   gi.broker,
			state:         g.State,
			balancer:      g.AssignorName,
			err:           describeError(g.ErrorCode, g.ErrorMessage),
			nMembers:      len(g.Members),
			rows:          buildRows(members, fetchedOffsets[g.Group], starts, ends),
			memberHeaders: []string{"MEMBER-ID", "CLIENT-ID", "HOST", "MEMBER-EPOCH", "SUBSCRIBED-TOPICS", "ASSIGNMENT", "TARGET-ASSIGNMENT"},
			memberJSON:    make([]map[string]any, 0, len(g.Members)),
		}
		for _, member := range g.Members {
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
			row := []any{
				member.MemberID,
				member.ClientID,
				host,
				member.MemberEpoch,
				strings.Join(member.SubscribedTopics, ","),
				formatAssignment(member.Assignment),
				formatAssignment(member.TargetAssignment),
			}
			pg.memberRows = append(pg.memberRows, row)
			pg.awkMemberRows = append(pg.awkMemberRows, append([]any{g.Group}, row...))

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
			pg.memberJSON = append(pg.memberJSON, m)
		}
		printed = append(printed, pg)
	}

	printGroups(cl.Format(), cl.Command(), opts, printed)

	for _, g := range allGroups {
		if g.group.ErrorCode != 0 {
			return out.ErrSilent
		}
	}
	return nil
}

// assignmentTopic is the topic an assignment names, or its ID in hex when
// the broker sent only the ID.
func assignmentTopic(tp kmsg.AssignmentTopicPartition) string {
	if tp.Topic != "" {
		return tp.Topic
	}
	return fmt.Sprintf("%x", tp.TopicID)
}

func addPartitions(tps map[string]map[int32]struct{}, topic string, partitions []int32) {
	if tps[topic] == nil {
		tps[topic] = make(map[int32]struct{})
	}
	for _, p := range partitions {
		tps[topic][p] = struct{}{}
	}
}

// nameAssignedTopics fills in the name of every topic an assignment carries
// by ID alone, from cluster metadata. A broker can send the ID without the
// name, and the name is what committed offsets and ListOffsets are keyed by.
// A topic the cluster no longer has keeps its ID, printed in hex.
func nameAssignedTopics(cl *client.Client, assignments []*kmsg.Assignment) {
	var unnamed bool
	for _, a := range assignments {
		for _, tp := range a.TopicPartitions {
			if tp.Topic == "" && tp.TopicID != [16]byte{} {
				unnamed = true
			}
		}
	}
	if !unnamed {
		return
	}

	req := kmsg.NewPtrMetadataRequest() // nil Topics lists every topic
	resp, err := req.RequestWith(context.Background(), cl.Client())
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to issue Metadata to name assigned topics: %v\n", err)
		return
	}
	names := make(map[[16]byte]string)
	for _, t := range resp.Topics {
		if t.Topic != nil {
			names[t.TopicID] = *t.Topic
		}
	}
	for _, a := range assignments {
		for i := range a.TopicPartitions {
			tp := &a.TopicPartitions[i]
			if tp.Topic == "" {
				tp.Topic = names[tp.TopicID]
			}
		}
	}
}

func formatAssignment(a kmsg.Assignment) string {
	var parts []string
	for _, tp := range a.TopicPartitions {
		name := assignmentTopic(tp)
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

// fetchOffsets fetches the committed offsets of each group, keyed by group,
// then topic, then partition.
func fetchOffsets(cl *client.Client, groups []string) (map[string]map[string]map[int32]offset, error) {
	fetched := make(map[string]map[string]map[int32]offset)
	var failures int
	for _, group := range groups {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = group
		resp, err := req.RequestWith(context.Background(), cl.Client())
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to issue OffsetFetch for group %s: %v\n", group, err)
			failures++
			continue
		}

		groupOffsets := make(map[string]map[int32]offset)
		fetched[group] = groupOffsets
		for _, topic := range resp.Topics {
			topicOffsets := groupOffsets[topic.Topic]
			if topicOffsets == nil {
				topicOffsets = make(map[int32]offset)
				groupOffsets[topic.Topic] = topicOffsets
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

// listOffsets lists the log start and log end offsets of every partition in
// tps, the member assignments, and every partition any group has committed
// to, which may be one no member owns any more. A ListOffsets request answers
// one timestamp per partition, so start and end are two requests.
func listOffsets(cl *client.Client, tps map[string]map[int32]struct{}, fetched map[string]map[string]map[int32]offset, readCommitted bool) (starts, ends map[string]map[int32]offset, err error) {
	for _, groupOffsets := range fetched {
		for topic, parts := range groupOffsets {
			for p := range parts {
				addPartitions(tps, topic, []int32{p})
			}
		}
	}
	if starts, err = listOffsetsForTopicPartitions(cl, tps, readCommitted, -2); err != nil {
		return nil, nil, err
	}
	if ends, err = listOffsetsForTopicPartitions(cl, tps, readCommitted, -1); err != nil {
		return nil, nil, err
	}
	return starts, ends, nil
}

// listOffsetsForTopicPartitions issues ListOffsets for the given
// topic-partition set at one timestamp: -2 for the log start, -1 for the
// log end.
func listOffsetsForTopicPartitions(cl *client.Client, tps map[string]map[int32]struct{}, readCommitted bool, timestamp int64) (map[string]map[int32]offset, error) {
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
			reqPartition.Timestamp = timestamp
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

// describeRow is one partition of the lag section. An offset is -1 when the
// cluster did not report it, and lag is valid only when we could compute it.
type describeRow struct {
	topic          string
	partition      int32
	currentOffset  int64
	logStartOffset int64
	logEndOffset   int64
	lag            int64
	lagValid       bool
	memberID       string
	instanceID     *string
	clientID       string
	host           string
	err            error
}

// rowMember is a group member as the lag rows see it: who it is, and which
// partitions it owns.
type rowMember struct {
	memberID   string
	instanceID *string
	clientID   string
	host       string
	assigned   map[string][]int32
}

func lookupOffset(m map[string]map[int32]offset, topic string, partition int32) offset {
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

// buildRows is one row per partition a member owns or the group has
// committed to, sorted by topic then partition.
func buildRows(members []rowMember, fetched, starts, ends map[string]map[int32]offset) []describeRow {
	assigned := make(map[string]map[int32]*describeRow)
	add := func(topic string, p int32, m *rowMember) {
		committed := lookupOffset(fetched, topic, p)
		start := lookupOffset(starts, topic, p)
		end := lookupOffset(ends, topic, p)
		row := &describeRow{
			topic:          topic,
			partition:      p,
			currentOffset:  committed.at,
			logStartOffset: start.at,
			logEndOffset:   end.at,
			err:            committed.err,
		}
		if m != nil {
			row.memberID = m.memberID
			row.instanceID = m.instanceID
			row.clientID = m.clientID
			row.host = m.host
		}
		if row.err == nil {
			row.err = end.err
		}
		switch {
		case end.at >= 0 && committed.at >= 0:
			row.lag = end.at - committed.at
			row.lagValid = true
		case end.at > 0 && committed.at == -1:
			// Nothing is committed, so the whole log is unread: from
			// its start when we know it, else from zero.
			row.lag = end.at
			row.lagValid = true
			if start.at >= 0 {
				row.lag = end.at - start.at
			}
		}
		if assigned[topic] == nil {
			assigned[topic] = make(map[int32]*describeRow)
		}
		assigned[topic][p] = row
	}

	for i := range members {
		m := &members[i]
		for topic, parts := range m.assigned {
			for _, p := range parts {
				add(topic, p, m)
			}
		}
	}
	// Committed-but-unassigned partitions.
	for topic, parts := range fetched {
		for p := range parts {
			if assigned[topic] != nil {
				if _, ok := assigned[topic][p]; ok {
					continue
				}
			}
			add(topic, p, nil)
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
	return rows
}

// printGroup is one described group as the printer sees it, from either
// describe API. The members section differs between the two APIs, so each
// hands over its member rows already shaped, per format.
type printGroup struct {
	group       string
	coordinator int32
	state       string
	balancer    string
	err         string // why the broker could not describe the group, or ""
	nMembers    int
	rows        []describeRow

	// The group's lag over every row, set by printGroups before --lag
	// filters the rows.
	totalLag      int64
	totalLagValid bool

	memberHeaders []string
	memberRows    [][]any
	awkMemberRows [][]any
	memberJSON    []map[string]any
}

var lagHeaders = []string{"TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-START-OFFSET", "LOG-END-OFFSET", "LAG", "MEMBER-ID", "CLIENT-ID", "HOST"}

func offsetNum(o int64) out.Number {
	if o < 0 {
		return out.NoNum
	}
	return out.Num(o)
}

func lagNum(r describeRow) out.Number {
	if !r.lagValid {
		return out.NoNum
	}
	return out.Num(r.lag)
}

func lagValues(r describeRow) []any {
	return []any{r.topic, r.partition, offsetNum(r.currentOffset), offsetNum(r.logStartOffset), offsetNum(r.logEndOffset), lagNum(r), r.memberID, r.clientID, r.host}
}

func lagJSON(r describeRow) map[string]any {
	return map[string]any{
		"topic":            r.topic,
		"partition":        r.partition,
		"current_offset":   r.currentOffset,
		"log_start_offset": r.logStartOffset,
		"log_end_offset":   r.logEndOffset,
		"lag":              r.lag,
		"member_id":        r.memberID,
		"client_id":        r.clientID,
		"host":             r.host,
	}
}

func totalLag(rows []describeRow) (int64, bool) {
	var total int64
	var valid bool
	for _, r := range rows {
		if r.lagValid {
			total += r.lag
			valid = true
		}
	}
	return total, valid
}

// printGroups prints the described groups, in order, in the format. Text
// prints the sections of each group under a GROUP line; awk prints the one
// section, lag by default, with the group as the first column of every lag
// row; JSON nests everything per group.
//
// With --lag, the lag rows are filtered first, and a group with no row left
// is dropped from every section, unless the broker could not describe it:
// that group still prints its error. The summary's TOTAL-LAG is the group's
// full total, computed before the filter.
func printGroups(format, command string, opts describeOpts, groups []printGroup) {
	section := opts.section
	var kept []printGroup
	for _, g := range groups {
		g.totalLag, g.totalLagValid = totalLag(g.rows)
		if opts.lag != nil {
			var rows []describeRow
			for _, r := range g.rows {
				if opts.lag.matches(r.lag, r.lagValid) {
					rows = append(rows, r)
				}
			}
			g.rows = rows
			if len(rows) == 0 && g.err == "" {
				continue
			}
		}
		kept = append(kept, g)
	}
	groups = kept

	switch format {
	case "json":
		jsonGroups := make([]map[string]any, 0, len(groups))
		for _, g := range groups {
			lagRows := make([]map[string]any, 0, len(g.rows))
			for _, r := range g.rows {
				lagRows = append(lagRows, lagJSON(r))
			}
			jsonGroups = append(jsonGroups, map[string]any{
				"group":       g.group,
				"coordinator": g.coordinator,
				"state":       g.state,
				"balancer":    g.balancer,
				"members":     g.memberJSON,
				"total_lag":   g.totalLag,
				"lag":         lagRows,
				"error":       g.err,
			})
		}
		out.MarshalJSON(command, 1, map[string]any{
			"groups": jsonGroups,
		})

	case "awk":
		sect := section
		if sect == "" {
			sect = "lag"
		}
		for _, g := range groups {
			switch sect {
			case "summary":
				fmt.Printf("%s\t%d\t%s\t%s\t%d\t%d\t%s\n",
					g.group, g.coordinator, g.state, g.balancer, g.nMembers, g.totalLag, g.err)
			case "lag":
				table := out.NewFormattedTable(format, command, 1, "lag", append([]string{"GROUP"}, lagHeaders...)...)
				for _, r := range g.rows {
					table.Row(append([]any{g.group}, lagValues(r)...)...)
				}
				table.Flush()
			case "members":
				table := out.NewFormattedTable(format, command, 1, "members", g.memberHeaders...)
				for _, row := range g.awkMemberRows {
					table.Row(row...)
				}
				table.Flush()
			}
		}

	default: // text
		for gi, g := range groups {
			showSummary := section == "" || section == "summary"
			showLag := section == "" || section == "lag"
			showMembers := section == "" || section == "members"

			if showSummary {
				tw := out.NewTabWriter()
				fmt.Fprintf(tw, "GROUP\t%s\n", g.group)
				fmt.Fprintf(tw, "COORDINATOR\t%d\n", g.coordinator)
				// On group-level error (e.g. GROUP_ID_NOT_FOUND),
				// skip the empty state/balancer/members fields.
				if g.err != "" {
					fmt.Fprintf(tw, "ERROR\t%s\n", g.err)
					tw.Flush()
					continue
				}
				fmt.Fprintf(tw, "STATE\t%s\n", g.state)
				fmt.Fprintf(tw, "BALANCER\t%s\n", g.balancer)
				fmt.Fprintf(tw, "MEMBERS\t%d\n", g.nMembers)
				if g.totalLagValid {
					fmt.Fprintf(tw, "TOTAL-LAG\t%d\n", g.totalLag)
				}
				tw.Flush()
			}

			if showLag && len(g.rows) > 0 {
				table := out.NewFormattedTable(format, command, 1, "lag", lagHeaders...)
				for _, r := range g.rows {
					table.Row(lagValues(r)...)
				}
				table.Flush()
			}

			if showMembers && len(g.memberRows) > 0 {
				table := out.NewFormattedTable(format, command, 1, "members", g.memberHeaders...)
				for _, row := range g.memberRows {
					table.Row(row...)
				}
				table.Flush()
			}

			if gi < len(groups)-1 {
				fmt.Println()
			}
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
