package group

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func describeCommand(cl *client.Client) *cobra.Command {
	var verbose bool
	var readCommitted bool

	// TODO include authorized options (Kafka 2.3.0+)?
	cmd := &cobra.Command{
		Use:     "describe GROUPS...",
		Aliases: []string{"d"},
		Short:   "Describe Kafka groups (Kafka 0.9.0+)",
		Run: func(_ *cobra.Command, groups []string) {
			if len(groups) == 0 {
				groups = listGroups(cl)
			}
			if len(groups) == 0 {
				out.Die("no groups to describe")
			}

			if verbose {
				described := describeGroups(cl, groups)
				fetchedOffsets := fetchOffsets(cl, groups)
				listedOffsets := listOffsets(cl, described, readCommitted)
				printDescribed(
					described,
					fetchedOffsets,
					listedOffsets,
				)
				return
			}

			req := kmsg.NewPtrDescribeGroupsRequest()
			req.Groups = groups

			shards := cl.Client().RequestSharded(context.Background(), req)

			tw := out.BeginTabWrite()
			defer tw.Flush()
			fmt.Fprintf(tw, "BROKER\tGROUP ID\tSTATE\tPROTO TYPE\tPROTO\tERROR\n")

			var failures int
			for _, shard := range shards {
				if shard.Err != nil {
					shardFail("DescribeGroups", shard, &failures)
					continue
				}

				for _, group := range shard.Resp.(*kmsg.DescribeGroupsResponse).Groups {
					errMsg := ""
					if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
						errMsg = err.Error()
					}
					fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\n",
						shard.Meta.NodeID,
						group.Group,
						group.State,
						group.ProtocolType,
						group.Protocol,
						errMsg,
					)
				}
			}
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose printing including client id, host, committed offset, lag, and user data")
	cmd.Flags().BoolVar(&readCommitted, "committed", false, "if describing verbosely, whether to list only committed offsets as opposed to latest (Kafka 0.11.0+)")

	return cmd
}

func shardFail(name string, shard kgo.ResponseShard, failures *int) {
	fmt.Printf("unable to issue %s to broker %d (%s:%d): %v\n", name, shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, shard.Err)
	*failures++
}

func shardErr(name string, shard kgo.ResponseShard, err error) {
	fmt.Printf("%s request error to broker %d (%s:%d): %v\n", name, shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, err)
}

func listGroups(cl *client.Client) []string {
	req := kmsg.NewPtrListGroupsRequest()

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

func describeGroups(cl *client.Client, groups []string) []describedGroup {
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = groups

	shards := cl.Client().RequestSharded(context.Background(), req)
	var described []describedGroup
	var failures int
	for _, shard := range shards {
		if shard.Err != nil {
			shardFail("DescribeGroups", shard, &failures)
			failures++
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

func listOffsets(cl *client.Client, described []describedGroup, readCommitted bool) map[string]map[int32]offset {
	tps := make(map[string]map[int32]struct{})
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
	currentOffset string
	logEndOffset  int64
	lag           string
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
) {
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

	for _, group := range groups {
		var rows []describeRow
		var useInstanceID, useErr bool
		for _, member := range group.Members {
			for _, topic := range member.MemberAssignment.Topics {
				t := topic.Topic
				for _, p := range topic.Partitions {
					committed := lookup(fetched, t, p)
					end := lookup(listed, t, p)

					row := describeRow{
						topic:     t,
						partition: p,

						logEndOffset: end.at,

						memberID:   member.MemberID,
						instanceID: member.InstanceID,
						clientID:   member.ClientID,
						host:       member.ClientHost,
						err:        committed.err,
					}
					if row.err == nil {
						row.err = end.err
					}

					useErr = row.err != nil
					useInstanceID = row.instanceID != nil

					row.currentOffset = strconv.FormatInt(committed.at, 10)
					if committed.at == -1 {
						row.currentOffset = "-"
					}

					row.lag = strconv.FormatInt(end.at-committed.at, 10)
					if end.at == 0 {
						row.lag = "-"
					} else if committed.at == -1 {
						row.lag = strconv.FormatInt(end.at, 10)
					}

					rows = append(rows, row)

				}
			}
		}

		printDescribedGroup(group, rows, useInstanceID, useErr)
		fmt.Println()
	}
}

func printDescribedGroup(group describedGroup, rows []describeRow, useInstanceID bool, useErr bool) {
	tw := out.NewTabWriter()
	fmt.Fprintf(tw, "GROUP\t%s\n", group.Group)
	fmt.Fprintf(tw, "COORDINATOR\t%d\n", group.Broker.NodeID)
	fmt.Fprintf(tw, "STATE\t%s\n", group.State)
	fmt.Fprintf(tw, "BALANCER\t%s\n", group.Protocol)
	fmt.Fprintf(tw, "MEMBERS\t%d\n", len(group.Members))
	if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
		fmt.Fprintf(tw, "ERROR\t%s\n", err)
	}
	tw.Flush()

	if len(rows) == 0 {
		return
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].topic < rows[j].topic ||
			rows[i].topic == rows[j].topic &&
				rows[i].partition < rows[j].partition
	})

	headers := []string{
		"TOPIC",
		"PARTITION",
		"CURRENT-OFFSET",
		"LOG-END-OFFSET",
		"LAG",
		"MEMBER-ID",
	}
	args := func(r *describeRow) []interface{} {
		return []interface{}{
			r.topic,
			r.partition,
			r.currentOffset,
			r.logEndOffset,
			r.lag,
			r.memberID,
		}
	}

	if useInstanceID {
		headers = append(headers, "INSTANCE-ID")
		orig := args
		args = func(r *describeRow) []interface{} {
			return append(orig(r), r.instanceID)
		}
	}

	{
		headers = append(headers, "CLIENT-ID", "HOST")
		orig := args
		args = func(r *describeRow) []interface{} {
			return append(orig(r), r.clientID, r.host)
		}

	}

	if useErr {
		headers = append(headers, "ERROR")
		orig := args
		args = func(r *describeRow) []interface{} {
			return append(orig(r), r.err)
		}
	}

	tw = out.NewTable(headers...)
	defer tw.Flush()
	for _, row := range rows {
		tw.Print(args(&row)...)
	}
}

type describedGroupMember struct {
	MemberID         string
	InstanceID       *string
	ClientID         string
	ClientHost       string
	MemberMetadata   kmsg.GroupMemberMetadata
	MemberAssignment kmsg.GroupMemberAssignment
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
