package group

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"slices"
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
		by                  string
		instanceIDs         bool
	)

	cmd := &cobra.Command{
		Use:     "describe [GROUPS...]",
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

In awk format, every row begins with the group it belongs to, so rows from
several groups can be told apart.

Use --instance-ids to show each member's group instance id (a static
member's group.instance.id) in an INSTANCE-ID column after RACK, on the lag
rows and the members rows. awk and json always carry the column, empty
without the flag.

The members section has the same columns for both protocols. A classic
group reports no RACK, MEMBER-EPOCH, or TARGET-ASSIGNMENT, so those are
unknown for its members: a dash in text and awk, null in json. INSTANCE-ID
is unknown the same way without --instance-ids.

A partition row ends in ERROR and MESSAGE: the error the coordinator
answered for its committed offset, or the error its leader answered for
its log offsets, and the command exits 1. A group the coordinator refused
as a whole, GROUP_AUTHORIZATION_FAILED say, has no partition rows; the
error is on its summary row instead, with STATE and MEMBERS as the broker
described them.

Use --lag to keep only the rows whose lag matches: '>0' for the rows that
are behind, '>=1000' or '1000' for those at least a thousand behind,
'<10', '<=10', or '=0'. A row whose lag we could not compute, printed as
a dash, never matches, though its error still exits 1. A group left with
no matching rows is left out entirely, so describing every group with
--lag '>0' lists only the groups that are behind. A group that survives
prints its summary and members whole, and TOTAL-LAG stays the group's
full total.

Use --by to roll the lag section up. Each view has its own columns, and
--lag then filters at that grain:
  --by partition   one row per partition (the default)
                   TOPIC PARTITION CURRENT-OFFSET LOG-START-OFFSET LOG-END-OFFSET LAG MEMBER-ID CLIENT-ID HOST RACK INSTANCE-ID ERROR MESSAGE
  --by topic       one row per topic
                   TOPIC PARTITIONS LAG
  --by member      one row per member; partitions no member owns share a
                   row with an empty MEMBER-ID
                   MEMBER-ID PARTITIONS LAG CLIENT-ID HOST RACK INSTANCE-ID
  --by group       one table with a row per group, across every group
                   GROUP STATE MEMBERS PARTITIONS LAG

PARTITIONS is how many partitions the row rolls up. LAG is their sum over
the partitions whose lag we could compute, or a dash when none has one; a
rolled up row carries no ERROR, but a partition's error still exits 1.
--by group prints only that table, and --by topic, member, or group cannot
be combined with --section summary or members, which the view does not
change.

EXAMPLES:
  kcl group describe                          # all groups, all sections
  kcl group describe mygroup                  # specific group
  kcl group describe --section lag            # lag only
  kcl group describe --section summary        # summary only
  kcl group describe --lag '>0'               # only the groups that are behind
  kcl group describe --lag '>=1000' -r 'prod-.*'  # far behind, by group regex
  kcl group describe --by group               # one row per group
  kcl group describe --by group --lag '>0'    # the groups that are behind, one row each
  kcl group describe g --by member            # lag per member of g
  kcl group describe g --by topic             # lag per topic of g
  kcl group describe --consumer-protocol      # KIP-848 groups
  kcl group describe g --instance-ids         # with each member's group.instance.id

SEE ALSO:
  kcl group list       list all groups
  kcl group seek       reset group offsets
  kcl consume -g       consume as a group member
`,
		RunE: func(cmd *cobra.Command, groups []string) error {
			if err := validateSection(section); err != nil {
				return err
			}
			if err := validateBy(by, section); err != nil {
				return err
			}
			opts := describeOpts{section: section, by: by, instanceIDs: instanceIDs}
			if by == "group" {
				opts.section = "lag"
			}
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
			tps := make(map[string]map[int32]struct{})
			for _, group := range described {
				for _, member := range group.Members {
					for _, topic := range member.MemberAssignment.Topics {
						addPartitions(tps, topic.Topic, topic.Partitions)
					}
				}
			}
			fetched, starts, ends, err := fetchLag(cl, opts, groups, tps, readCommitted)
			if err != nil {
				return err
			}

			sort.Slice(described, func(i, j int) bool {
				return described[i].Group < described[j].Group
			})
			var printed []printGroup
			for _, group := range described {
				printed = append(printed, classicPrintGroup(group, fetched[group.Group], starts, ends))
			}
			return printGroups(cl.Format(), cl.Command(), opts, printed)
		},
	}
	out.ColumnsFunc(cmd, func() []string {
		return awkHeaders(describeOpts{section: section, by: by})
	})

	cmd.Flags().BoolVar(&readCommitted, "committed", false, "use committed (read_committed) offsets for lag computation instead of latest")
	cmd.Flags().BoolVar(&useConsumerDescribe, "consumer-protocol", false, "use ConsumerGroupDescribe API for new consumer group protocol (KIP-848, Kafka 4.0+)")
	cmd.Flags().StringVar(&section, "section", "", "output section (summary, lag, members; default: all for text, lag for awk)")
	cmd.Flags().BoolVarP(&regex, "regex", "r", false, "treat group arguments as regular expressions")
	cmd.Flags().StringVar(&lagExpr, "lag", "", "keep only rows whose lag matches (>N, >=N, <N, <=N, =N, or N for >=N); a group with none left is dropped")
	cmd.Flags().StringVar(&by, "by", "partition", "roll the lag section up by partition, topic, member, or group")
	cmd.Flags().BoolVar(&instanceIDs, "instance-ids", false, "show each member's group instance id in an INSTANCE-ID column")

	return cmd
}

// describeOpts are the flags that shape what printGroups prints.
type describeOpts struct {
	section     string
	by          string
	lag         *lagFilter // nil when --lag is not set
	instanceIDs bool
}

// awkHeaders is the one row shape the flags select in awk: the --by group
// table, else the --section, lag by default, every row led by its group.
func awkHeaders(opts describeOpts) []string {
	if opts.by == "group" {
		return groupViewHeaders
	}
	switch opts.section {
	case "summary":
		return summaryHeaders
	case "members":
		return append([]string{"GROUP"}, memberHeaders...)
	}
	return append([]string{"GROUP"}, lagHeaders(opts.by)...)
}

var (
	summaryHeaders   = []string{"GROUP", "COORDINATOR", "STATE", "BALANCER", "MEMBERS", "TOTAL-LAG", "ERROR", "MESSAGE"}
	memberHeaders    = []string{"MEMBER-ID", "CLIENT-ID", "HOST", "RACK", "INSTANCE-ID", "MEMBER-EPOCH", "SUBSCRIBED-TOPICS", "ASSIGNMENT", "TARGET-ASSIGNMENT"}
	groupViewHeaders = []string{"GROUP", "STATE", "MEMBERS", "PARTITIONS", "LAG"}
)

// validateBy returns a usage error if by is not a view, or if it is a view
// that reshapes the lag section while section asks for another one.
func validateBy(by, section string) error {
	switch by {
	case "partition", "topic", "member", "group":
	default:
		return out.Errf(out.ExitUsage, "invalid --by %q: must be partition, topic, member, or group", by)
	}
	if by != "partition" && (section == "summary" || section == "members") {
		return out.Errf(out.ExitUsage, "--by %s cannot be combined with --section %s: --by reshapes the lag section only", by, section)
	}
	return nil
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

// setErrors sets the group's ERROR and MESSAGE: the describe error when the
// broker could not describe the group, else the error the coordinator
// answered its OffsetFetch with, else "". A group the broker described is
// known, whatever its offsets did.
func (g *printGroup) setErrors(code int16, message *string, offsets error) {
	switch {
	case code != 0:
		g.err, g.message = out.ErrName(code), out.BrokerMessage(message)
	case offsets != nil:
		g.known = true
		g.err = out.ErrCell(offsets)
	default:
		g.known = true
	}
}

func classicPrintGroup(group describedGroup, fetched groupOffsets, starts, ends map[string]map[int32]offset) printGroup {
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
		group:       group.Group,
		coordinator: group.Broker.NodeID,
		state:       group.State,
		balancer:    group.Protocol,
		members:     make([]describeMember, 0, len(group.Members)),
	}
	pg.setErrors(group.ErrorCode, group.ErrorMessage, fetched.err)
	if pg.err == "" {
		pg.rows = buildRows(members, fetched.topics, starts, ends)
	}
	for i, member := range group.Members {
		pg.members = append(pg.members, describeMember{
			memberID:   member.MemberID,
			instanceID: member.InstanceID,
			clientID:   member.ClientID,
			host:       member.ClientHost,
			epoch:      out.Unknown,
			subscribed: member.MemberMetadata.Topics,
			assignment: formatAssigned(members[i].assigned),
			target:     out.Unknown,
		})
	}
	sortMembers(pg.members)
	return pg
}

// sortMembers orders the members section by member id; the broker answers
// in join order, which changes from one run to the next.
func sortMembers(members []describeMember) {
	slices.SortFunc(members, func(a, b describeMember) int { return strings.Compare(a.memberID, b.memberID) })
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

	fetched, starts, ends, err := fetchLag(cl, opts, groups, tps, readCommitted)
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
				rack:       member.RackID,
				assigned:   make(map[string][]int32),
			}
			for _, tp := range member.Assignment.TopicPartitions {
				topic := assignmentTopic(tp)
				m.assigned[topic] = append(m.assigned[topic], tp.Partitions...)
			}
			members = append(members, m)
		}

		pg := printGroup{
			group:       g.Group,
			coordinator: gi.broker,
			state:       g.State,
			balancer:    g.AssignorName,
			members:     make([]describeMember, 0, len(g.Members)),
		}
		pg.setErrors(g.ErrorCode, g.ErrorMessage, fetched[g.Group].err)
		if pg.err == "" {
			pg.rows = buildRows(members, fetched[g.Group].topics, starts, ends)
		}
		for _, member := range g.Members {
			pg.members = append(pg.members, describeMember{
				memberID:   member.MemberID,
				instanceID: member.InstanceID,
				clientID:   member.ClientID,
				host:       member.ClientHost,
				rack:       member.RackID,
				epoch:      member.MemberEpoch,
				subscribed: member.SubscribedTopics,
				assignment: formatAssignment(member.Assignment),
				target:     formatAssignment(member.TargetAssignment),
			})
		}
		sortMembers(pg.members)
		printed = append(printed, pg)
	}

	return printGroups(cl.Format(), cl.Command(), opts, printed)
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
	assigned := make(map[string][]int32, len(a.TopicPartitions))
	for _, tp := range a.TopicPartitions {
		name := assignmentTopic(tp)
		assigned[name] = append(assigned[name], tp.Partitions...)
	}
	return formatAssigned(assigned)
}

// formatAssigned is the partitions a member owns as one field, "t:0,1;u:2",
// topics sorted and partitions ascending. Nothing owned is "".
func formatAssigned(assigned map[string][]int32) string {
	topics := make([]string, 0, len(assigned))
	for topic := range assigned {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	var parts []string
	for _, topic := range topics {
		ps := make([]string, len(assigned[topic]))
		sort.Slice(assigned[topic], func(i, j int) bool { return assigned[topic][i] < assigned[topic][j] })
		for i, p := range assigned[topic] {
			ps[i] = strconv.Itoa(int(p))
		}
		parts = append(parts, topic+":"+strings.Join(ps, ","))
	}
	return strings.Join(parts, ";")
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

// groupOffsets is one group's committed offsets, by topic then partition,
// or the error the coordinator answered the group as a whole with, which
// v8 and later of OffsetFetch carry per group: GROUP_AUTHORIZATION_FAILED,
// or NOT_COORDINATOR after the retries ran out. A request we could not
// issue at all is the group's error too.
type groupOffsets struct {
	err    error
	topics map[string]map[int32]offset
}

// fetchLag fetches what the lag rows need: every group's committed offsets,
// and the log start and end of every partition in tps or committed to.
func fetchLag(cl *client.Client, opts describeOpts, groups []string, tps map[string]map[int32]struct{}, readCommitted bool) (fetched map[string]groupOffsets, starts, ends map[string]map[int32]offset, err error) {
	if fetched, err = fetchOffsets(cl, groups); err != nil {
		return nil, nil, nil, err
	}
	starts, ends, err = listOffsets(cl, tps, fetched, readCommitted)
	return fetched, starts, ends, err
}

// fetchOffsets fetches the committed offsets of each group, keyed by group.
func fetchOffsets(cl *client.Client, groups []string) (map[string]groupOffsets, error) {
	fetched := make(map[string]groupOffsets)
	var failures int
	for _, group := range groups {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = group
		resp, err := req.RequestWith(context.Background(), cl.Client())
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to issue OffsetFetch for group %s: %v\n", group, err)
			fetched[group] = groupOffsets{err: err}
			failures++
			continue
		}
		// franz-go folds a one group response into the top level; the
		// group's own entry is checked too, for a broker that answers the
		// batch shape with more than we asked for.
		code := resp.ErrorCode
		for _, g := range resp.Groups {
			if g.Group == group && g.ErrorCode != 0 {
				code = g.ErrorCode
			}
		}
		if err := kerr.ErrorForCode(code); err != nil {
			fetched[group] = groupOffsets{err: err}
			continue
		}

		topics := make(map[string]map[int32]offset)
		fetched[group] = groupOffsets{topics: topics}
		for _, topic := range resp.Topics {
			topicOffsets := topics[topic.Topic]
			if topicOffsets == nil {
				topicOffsets = make(map[int32]offset)
				topics[topic.Topic] = topicOffsets
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
func listOffsets(cl *client.Client, tps map[string]map[int32]struct{}, fetched map[string]groupOffsets, readCommitted bool) (starts, ends map[string]map[int32]offset, err error) {
	for _, g := range fetched {
		for topic, parts := range g.topics {
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
	set := func(topic string, partition int32, o offset) {
		partOffsets := listed[topic]
		if partOffsets == nil {
			partOffsets = make(map[int32]offset)
			listed[topic] = partOffsets
		}
		partOffsets[partition] = o
	}
	var failures int
	for _, shard := range shards {
		// A broker we could not ask answers with its error for every
		// partition it was asked about, so each row carries it.
		if shard.Err != nil {
			shardFail("ListOffsets", shard, &failures)
			for _, topic := range shard.Req.(*kmsg.ListOffsetsRequest).Topics {
				for _, partition := range topic.Partitions {
					set(topic.Topic, partition.Partition, offset{at: -1, err: shard.Err})
				}
			}
			continue
		}

		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		for _, topic := range resp.Topics {
			for _, partition := range topic.Partitions {
				set(topic.Topic, partition.Partition, offset{
					at:  partition.Offset,
					err: kerr.ErrorForCode(partition.ErrorCode),
				})
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
	rack           *string
	partitions     int // how many partitions a topic or member row rolls up
	err            error
}

// rowMember is a group member as the lag rows see it: who it is, and which
// partitions it owns.
type rowMember struct {
	memberID   string
	instanceID *string
	clientID   string
	host       string
	rack       *string
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
			row.rack = m.rack
		}
		if row.err == nil {
			row.err = end.err
		}
		if row.err == nil {
			row.err = start.err
		}
		switch {
		case end.at >= 0 && committed.at >= 0:
			row.lag = end.at - committed.at
			row.lagValid = true
		case end.at >= 0 && committed.at == -1:
			// Nothing is committed, so the whole log is unread: from
			// its start when we know it, else from zero. An empty log
			// is lag 0, known.
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
// describe API.
type printGroup struct {
	group       string
	coordinator int32
	state       string
	balancer    string
	known       bool   // the broker described the group: state, balancer, and members are known
	err         string // why the broker could not describe the group or fetch its offsets, or ""
	message     string // the text the broker attached to err, or ""
	rows        []describeRow
	members     []describeMember

	// The group's lag over every row, set by printGroups before --lag
	// filters the rows.
	totalLag      int64
	totalLagValid bool
}

// describeMember is one member of the members section. The classic protocol
// has no member epoch and no target assignment, so those are Unknown there.
type describeMember struct {
	memberID   string
	instanceID *string
	clientID   string
	host       string
	rack       *string
	epoch      any // int32, or Unknown
	subscribed []string
	assignment string
	target     any // string, or Unknown
}

// instanceCell is the INSTANCE-ID cell: Unknown unless --instance-ids, then
// the id, or "" for a member without one.
func instanceCell(opts describeOpts, id *string) any {
	if !opts.instanceIDs {
		return out.Unknown
	}
	if id == nil {
		return ""
	}
	return *id
}

func (m describeMember) values(opts describeOpts) []any {
	return []any{m.memberID, m.clientID, m.host, rackCell(m.rack), instanceCell(opts, m.instanceID), m.epoch, strings.Join(m.subscribed, ","), m.assignment, m.target}
}

// rackCell is the RACK cell: the rack a member reported, or Unknown for a
// member that reported none, which every classic protocol member is.
func rackCell(rack *string) any {
	if rack == nil {
		return out.Unknown
	}
	return *rack
}

// rowRackCell is rackCell for a partition row. A partition nobody owns has
// empty member columns, and its rack is empty with them rather than unknown.
func rowRackCell(r describeRow) any {
	if r.memberID == "" {
		return ""
	}
	return rackCell(r.rack)
}

func (m describeMember) json(opts describeOpts) map[string]any {
	subscribed := m.subscribed
	if subscribed == nil {
		subscribed = []string{}
	}
	return map[string]any{
		"member_id":         m.memberID,
		"client_id":         m.clientID,
		"host":              m.host,
		"rack":              rackCell(m.rack),
		"instance_id":       instanceCell(opts, m.instanceID),
		"member_epoch":      m.epoch,
		"subscribed_topics": subscribed,
		"assignment":        m.assignment,
		"target_assignment": m.target,
	}
}

func lagHeaders(by string) []string {
	switch by {
	case "topic":
		return []string{"TOPIC", "PARTITIONS", "LAG"}
	case "member":
		return []string{"MEMBER-ID", "PARTITIONS", "LAG", "CLIENT-ID", "HOST", "RACK", "INSTANCE-ID"}
	default:
		return []string{"TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-START-OFFSET", "LOG-END-OFFSET", "LAG", "MEMBER-ID", "CLIENT-ID", "HOST", "RACK", "INSTANCE-ID", "ERROR", "MESSAGE"}
	}
}

// errCell is the ERROR cell of a partition row: the kerr name of the error
// the coordinator or the leader answered, or "". Neither OffsetFetch nor
// ListOffsets attaches a message, so MESSAGE is "".
func (r describeRow) errCell() string {
	return out.ErrCell(r.err)
}

func offsetNum(o int64) any {
	if o < 0 {
		return out.Unknown
	}
	return o
}

func lagNum(r describeRow) any {
	if !r.lagValid {
		return out.Unknown
	}
	return r.lag
}

func lagValues(opts describeOpts, r describeRow) []any {
	switch opts.by {
	case "topic":
		return []any{r.topic, r.partitions, lagNum(r)}
	case "member":
		return []any{r.memberID, r.partitions, lagNum(r), r.clientID, r.host, rowRackCell(r), instanceCell(opts, r.instanceID)}
	default:
		return []any{r.topic, r.partition, offsetNum(r.currentOffset), offsetNum(r.logStartOffset), offsetNum(r.logEndOffset), lagNum(r), r.memberID, r.clientID, r.host, rowRackCell(r), instanceCell(opts, r.instanceID), r.errCell(), ""}
	}
}

func lagJSON(opts describeOpts, r describeRow) map[string]any {
	switch opts.by {
	case "topic":
		return map[string]any{
			"topic":      r.topic,
			"partitions": r.partitions,
			"lag":        lagNum(r),
		}
	case "member":
		return map[string]any{
			"member_id":   r.memberID,
			"partitions":  r.partitions,
			"lag":         lagNum(r),
			"client_id":   r.clientID,
			"host":        r.host,
			"rack":        rowRackCell(r),
			"instance_id": instanceCell(opts, r.instanceID),
		}
	default:
		return map[string]any{
			"topic":            r.topic,
			"partition":        r.partition,
			"current_offset":   offsetNum(r.currentOffset),
			"log_start_offset": offsetNum(r.logStartOffset),
			"log_end_offset":   offsetNum(r.logEndOffset),
			"lag":              lagNum(r),
			"member_id":        r.memberID,
			"client_id":        r.clientID,
			"host":             r.host,
			"rack":             rowRackCell(r),
			"instance_id":      instanceCell(opts, r.instanceID),
			"error":            r.errCell(),
			"message":          "",
		}
	}
}

// rollup sums partition rows into one row per key, in first-seen order:
// the partition count, and the lag over the partitions whose lag we know.
func rollup(rows []describeRow, key func(describeRow) string) []describeRow {
	var rolled []describeRow
	idx := make(map[string]int)
	for _, r := range rows {
		k := key(r)
		i, ok := idx[k]
		if !ok {
			i = len(rolled)
			idx[k] = i
			rolled = append(rolled, describeRow{topic: r.topic, memberID: r.memberID, instanceID: r.instanceID, clientID: r.clientID, host: r.host, rack: r.rack})
		}
		rolled[i].partitions++
		if r.lagValid {
			rolled[i].lag += r.lag
			rolled[i].lagValid = true
		}
	}
	return rolled
}

// shapeRows is the lag rows in the --by view: the partition rows as they
// are, one row per topic, or one row per member with the partitions no
// member owns sharing a row with an empty member last.
func shapeRows(by string, rows []describeRow) []describeRow {
	switch by {
	case "topic":
		return rollup(rows, func(r describeRow) string { return r.topic })
	case "member":
		rolled := rollup(rows, func(r describeRow) string { return r.memberID })
		sort.SliceStable(rolled, func(i, j int) bool {
			if (rolled[i].memberID == "") != (rolled[j].memberID == "") {
				return rolled[j].memberID == ""
			}
			return rolled[i].memberID < rolled[j].memberID
		})
		return rolled
	default:
		return rows
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
// section, lag by default, with the group as the first column of every row;
// JSON nests everything per group.
//
// The lag rows are shaped by --by first and filtered by --lag second, and a
// group with no row left is dropped from every section, unless the broker
// could not describe it: that group still prints its error. The summary's
// TOTAL-LAG is the group's full total, computed before both. --by group is
// one table across every group rather than sections per group.
//
// It returns ErrSilent when any group carries an error or any partition row
// did before the view and the filter, so that the command exits 1 on a
// failure the output may not show. A partition error the view rolls up or
// the filter drops, which never matches, goes to stderr instead.
func printGroups(format, command string, opts describeOpts, groups []printGroup) error {
	var failed bool
	for i := range groups {
		groups[i].totalLag, groups[i].totalLagValid = totalLag(groups[i].rows)
		failed = failed || groups[i].err != ""
		for _, r := range groups[i].rows {
			if r.err == nil {
				continue
			}
			failed = true
			if opts.by != "partition" || opts.lag != nil {
				fmt.Fprintf(os.Stderr, "unable to describe group %s partition %s/%d: %s\n", groups[i].group, r.topic, r.partition, r.errCell())
			}
		}
	}
	var err error
	if failed {
		err = out.ErrSilent
	}
	if opts.by == "group" {
		printGroupView(format, command, opts, groups)
		return err
	}

	section := opts.section
	var kept []printGroup
	for _, g := range groups {
		g.rows = shapeRows(opts.by, g.rows)
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
	case out.FormatJSON:
		jsonGroups := make([]map[string]any, 0, len(groups))
		for _, g := range groups {
			lagRows := make([]map[string]any, 0, len(g.rows))
			for _, r := range g.rows {
				lagRows = append(lagRows, lagJSON(opts, r))
			}
			members := make([]map[string]any, 0, len(g.members))
			for _, m := range g.members {
				members = append(members, m.json(opts))
			}
			jsonGroups = append(jsonGroups, map[string]any{
				"group":       g.group,
				"coordinator": g.coordinator,
				"state":       g.stateCell(),
				"balancer":    g.balancerCell(),
				"members":     members,
				"total_lag":   g.totalLagNum(),
				"lag":         lagRows,
				"error":       g.err,
				"message":     g.message,
			})
		}
		out.MarshalJSON(command, 1, map[string]any{
			"groups": jsonGroups,
		})

	case out.FormatAWK:
		sect := section
		if sect == "" {
			sect = "lag"
		}
		table := out.NewFormattedTable(format, command, 1, sect, awkHeaders(opts)...)
		if sect == "summary" || sect == "lag" && opts.by == "partition" {
			table.ErrorColumn()
		}
		for _, g := range groups {
			switch sect {
			case "summary":
				table.Row(g.summaryValues()...)
			case "lag":
				for _, r := range g.rows {
					table.Row(append([]any{g.group}, lagValues(opts, r)...)...)
				}
			case "members":
				for _, m := range g.members {
					table.Row(append([]any{g.group}, m.values(opts)...)...)
				}
			}
		}
		table.Flush()

	default: // text
		for gi, g := range groups {
			showSummary := section == "" || section == "summary"
			showLag := section == "" || section == "lag"
			showMembers := section == "" || section == "members"

			// A group with an error prints it whichever section was
			// asked for, since the section may have nothing else to
			// show for it.
			if showSummary || g.err != "" {
				tw := out.NewTabWriter()
				fmt.Fprintf(tw, "GROUP\t%s\n", g.group)
				fmt.Fprintf(tw, "COORDINATOR\t%d\n", g.coordinator)
				if g.known && showSummary {
					fmt.Fprintf(tw, "STATE\t%s\n", g.state)
					fmt.Fprintf(tw, "BALANCER\t%s\n", g.balancer)
					fmt.Fprintf(tw, "MEMBERS\t%d\n", len(g.members))
					if g.totalLagValid {
						fmt.Fprintf(tw, "TOTAL-LAG\t%d\n", g.totalLag)
					}
				}
				if g.err != "" {
					fmt.Fprintf(tw, "ERROR\t%s\n", g.err)
					if g.message != "" {
						fmt.Fprintf(tw, "MESSAGE\t%s\n", g.message)
					}
				}
				tw.Flush()
				if !g.known {
					continue
				}
			}

			// A section asked for by name prints its header even
			// with no rows, so that an Empty group under --section
			// members prints something; the default view skips an
			// empty table, and a group whose offsets errored has no
			// lag table, its error printed above.
			if showLag && g.err == "" && (len(g.rows) > 0 || section == "lag") {
				rows := make([][]any, 0, len(g.rows))
				for _, r := range g.rows {
					rows = append(rows, lagValues(opts, r))
				}
				printTextTable(command, lagHeaders(opts.by), rows, opts, opts.by == "partition")
			}

			if showMembers && (len(g.members) > 0 || section == "members") {
				rows := make([][]any, 0, len(g.members))
				for _, m := range g.members {
					rows = append(rows, m.values(opts))
				}
				printTextTable(command, memberHeaders, rows, opts, false)
			}

			if gi < len(groups)-1 {
				fmt.Println()
			}
		}
	}
	return err
}

// printTextTable prints one text table, without the INSTANCE-ID column
// unless --instance-ids asked for it. With errors set the table ends in
// ERROR and MESSAGE, blank on a row the broker answered.
func printTextTable(command string, headers []string, rows [][]any, opts describeOpts, errors bool) {
	if i := slices.Index(headers, "INSTANCE-ID"); i >= 0 && !opts.instanceIDs {
		headers = slices.Delete(slices.Clone(headers), i, i+1)
		for j, row := range rows {
			rows[j] = slices.Delete(row, i, i+1)
		}
	}
	table := out.NewFormattedTable(out.FormatText, command, 1, "rows", headers...)
	if errors {
		table.ErrorColumn()
	}
	for _, row := range rows {
		table.Row(row...)
	}
	table.Flush()
}

func (g printGroup) totalLagNum() any {
	if !g.totalLagValid {
		return out.Unknown
	}
	return g.totalLag
}

// stateCell is the group's state, Unknown when the broker could not
// describe it; balancerCell and memberCount are the same for the rest of
// the summary.
func (g printGroup) stateCell() any {
	if !g.known {
		return out.Unknown
	}
	return g.state
}

func (g printGroup) balancerCell() any {
	if !g.known {
		return out.Unknown
	}
	return g.balancer
}

func (g printGroup) memberCount() any {
	if !g.known {
		return out.Unknown
	}
	return len(g.members)
}

// summaryValues is the group's summary row, in summaryHeaders order.
func (g printGroup) summaryValues() []any {
	return []any{g.group, g.coordinator, g.stateCell(), g.balancerCell(), g.memberCount(), g.totalLagNum(), g.err, g.message}
}

// printGroupView prints the --by group table: one row per group, across
// every group, filtered by --lag on the group's total. A group the broker
// could not describe has no row; its error goes to stderr, since the summary
// that would carry it is not printed. When no row is left, text and awk print
// nothing and JSON prints an empty list.
func printGroupView(format, command string, opts describeOpts, groups []printGroup) {
	table := out.NewFormattedTable(format, command, 1, "groups", groupViewHeaders...).
		WithKeys(map[string]string{"MEMBERS": "member_count", "PARTITIONS": "partition_count", "LAG": "total_lag"})
	var rows int
	for _, g := range groups {
		if g.err != "" {
			fmt.Fprintf(os.Stderr, "unable to describe group %s: %s\n", g.group, g.err)
			continue
		}
		if !opts.lag.matches(g.totalLag, g.totalLagValid) {
			continue
		}
		table.Row(g.group, g.state, len(g.members), len(g.rows), g.totalLagNum())
		rows++
	}
	if rows == 0 && format != out.FormatJSON {
		return
	}
	table.Flush()
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
