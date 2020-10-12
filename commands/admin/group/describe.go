package group

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/twmb/frang/pkg/kerr"
	"github.com/twmb/frang/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func describeCommand(cl *client.Client) *cobra.Command {
	var req kmsg.DescribeGroupsRequest
	var verbose bool
	var readCommitted bool

	// TODO include authorized options (Kafka 2.3.0+)?
	cmd := &cobra.Command{
		Use:     "describe GROUPS...",
		Aliases: []string{"d"},
		Short:   "Describe Kafka groups (Kafka 0.9.0+)",
		Args:    cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			req.Groups = args

			kresp, err := cl.Client().Request(context.Background(), &req)

			out.MaybeDie(err, "unable to describe groups: %v", err)
			resp := unbinaryGroupDescribeMembers(kresp.(*kmsg.DescribeGroupsResponse))

			if cl.AsJSON() {
				out.ExitJSON(resp)
			}

			if verbose {
				for _, group := range resp.Groups {
					describeGroupVerbose(cl, &group, readCommitted)
					fmt.Println()
				}
				return
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()
			fmt.Fprintf(tw, "GROUP ID\tSTATE\tPROTO TYPE\tPROTO\tERROR\n")
			for _, group := range resp.Groups {
				errMsg := ""
				if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
					errMsg = err.Error()
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
					group.Group,
					group.State,
					group.ProtocolType,
					group.Protocol,
					errMsg,
				)
			}
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose printing including client id, host, committed offset, lag, and user data")
	cmd.Flags().BoolVar(&readCommitted, "committed", false, "if describing verbosely, whether to list only committed offsets as opposed to latest (Kafka 0.11.0+)")

	return cmd
}

func describeGroupVerbose(cl *client.Client, group *consumerGroup, readCommitted bool) {
	tw := out.BeginTabWrite()
	fmt.Fprintf(tw, "ID\t%s\n", group.Group)
	fmt.Fprintf(tw, "STATE\t%s\n", group.State)
	fmt.Fprintf(tw, "PROTO TYPE\t%s\n", group.ProtocolType)
	fmt.Fprintf(tw, "PROTO\t%s\n", group.Protocol)
	fmt.Fprintf(tw, "MEMBERS\t%d\n", len(group.Members))
	errStr := ""
	if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
		errStr = err.Error()
	}
	fmt.Fprintf(tw, "ERROR\t%s\n", errStr)
	tw.Flush()

	type offsets struct {
		err          error
		endOffset    int64
		commitOffset int64
		member       *consumerGroupMember
	}
	allOffsets := make(map[string]map[int32]offsets)

	// For the group, first get the last committed offsets. This will
	// also tell us the topics and partitions in the group, which we
	// will use for listing offsets.
	{
		req := &kmsg.OffsetFetchRequest{
			Group:         group.Group,
			RequireStable: false,
		}

		kresp, err := cl.Client().Request(context.Background(), req)
		out.MaybeDie(err, "unable to fetch offsets: %v", err)
		resp := kresp.(*kmsg.OffsetFetchResponse)

		for _, topic := range resp.Topics {
			topicPartitions := allOffsets[topic.Topic]
			if topicPartitions == nil {
				topicPartitions = make(map[int32]offsets)
				allOffsets[topic.Topic] = topicPartitions
			}
			for _, partition := range topic.Partitions {
				topicPartitions[partition.Partition] = offsets{
					err:          kerr.ErrorForCode(partition.ErrorCode),
					commitOffset: partition.Offset,
				}
			}
		}
	}

	// Now we load the partition's latest offsets with list offsets.
	{
		req := &kmsg.ListOffsetsRequest{
			ReplicaID:      -1,
			IsolationLevel: 0,
		}
		if readCommitted {
			req.IsolationLevel = 1
		}
		for topic, partitions := range allOffsets {
			reqTopic := kmsg.ListOffsetsRequestTopic{
				Topic: topic,
			}
			for partition, _ := range partitions {
				reqTopic.Partitions = append(reqTopic.Partitions, kmsg.ListOffsetsRequestTopicPartition{
					Partition:          partition,
					CurrentLeaderEpoch: -1,
					Timestamp:          -1,
				})
			}
			req.Topics = append(req.Topics, reqTopic)
		}

		kresp, err := cl.Client().Request(context.Background(), req)
		out.MaybeDie(err, "unable to list offsets: %v", err)
		resp := kresp.(*kmsg.ListOffsetsResponse)

		for _, topic := range resp.Topics {
			for _, partition := range topic.Partitions {
				offsets := allOffsets[topic.Topic][partition.Partition]
				if offsets.err == nil {
					offsets.err = kerr.ErrorForCode(partition.ErrorCode)
				}
				offsets.endOffset = partition.Offset
				allOffsets[topic.Topic][partition.Partition] = offsets
			}
		}
	}

	// Now we pair the member to the partitions.
	for i := range group.Members {
		member := &group.Members[i]
		for _, topic := range member.MemberAssignment.Topics {
			if allOffsets[topic.Topic] == nil {
				allOffsets[topic.Topic] = make(map[int32]offsets)
			}
			topicOffsets := allOffsets[topic.Topic]
			for _, partition := range topic.Partitions {
				partOffsets, exists := topicOffsets[partition]
				if !exists {
					// assigned, but partition is empty
					partOffsets = offsets{
						err:          nil,
						endOffset:    -1,
						commitOffset: -1,
					}
				}
				partOffsets.member = member
				topicOffsets[partition] = partOffsets
			}
		}
	}

	// Now we sort the map so that our output will be nice.
	type partOffsets struct {
		part int32
		offsets
	}
	type topicOffsets struct {
		topic string
		parts []partOffsets
	}
	var sortedOffsets []topicOffsets
	for topic, partitions := range allOffsets {
		to := topicOffsets{
			topic: topic,
		}
		for partition, offsets := range partitions {
			to.parts = append(to.parts, partOffsets{
				part:    partition,
				offsets: offsets,
			})
		}
		sort.Slice(to.parts, func(i, j int) bool { return to.parts[i].part < to.parts[j].part })
		sortedOffsets = append(sortedOffsets, to)
	}
	sort.Slice(sortedOffsets, func(i, j int) bool { return sortedOffsets[i].topic < sortedOffsets[j].topic })

	// Finally, we can print everything.
	tw = out.BeginTabWrite()
	defer tw.Flush()
	fmt.Fprintf(tw, "TOPIC\tPARTITION\tCURRENT OFFSET\tLOG END OFFSET\tLAG\tMEMBER ID\tINSTANCE ID\tCLIENT ID\tHOST\tUSER DATA\tLOAD ERR\n")
	for _, topic := range sortedOffsets {
		for _, part := range topic.parts {
			memberID, instanceID, clientID, host, userData := "", "", "", "", ""
			loadErr := ""
			if part.member != nil {
				loadErr = ""
				m := part.member
				memberID = m.MemberID
				if m.InstanceID != nil {
					instanceID = *m.InstanceID
				}
				clientID = m.ClientID
				host = m.ClientHost
				userData = string(m.MemberAssignment.UserData)
			}
			if part.err != nil {
				loadErr = part.err.Error()
			}

			lag := part.endOffset - part.commitOffset
			lagstr := fmt.Sprintf("%d", lag)
			if lag < 0 {
				lagstr = "???"
			}

			fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				topic.topic,
				part.part,
				part.commitOffset,
				part.endOffset,
				lagstr,
				memberID,
				instanceID,
				clientID,
				host,
				userData,
				loadErr,
			)
		}
	}
}

type consumerGroupMember struct {
	MemberID         string
	InstanceID       *string
	ClientID         string
	ClientHost       string
	MemberMetadata   kmsg.GroupMemberMetadata
	MemberAssignment kmsg.GroupMemberAssignment
}
type consumerGroup struct {
	ErrorCode            int16
	Group                string
	State                string
	ProtocolType         string
	Protocol             string
	Members              []consumerGroupMember
	AuthorizedOperations int32
}
type consumerGroups struct {
	ThrottleMillis int32
	Groups         []consumerGroup
}

// unmarshals and prints the standard java protocol type
func unbinaryGroupDescribeMembers(resp *kmsg.DescribeGroupsResponse) *consumerGroups {
	cresp := &consumerGroups{
		ThrottleMillis: resp.ThrottleMillis,
	}
	for _, group := range resp.Groups {
		cgroup := consumerGroup{
			ErrorCode:            group.ErrorCode,
			Group:                group.Group,
			State:                group.State,
			ProtocolType:         group.ProtocolType,
			Protocol:             group.Protocol,
			AuthorizedOperations: group.AuthorizedOperations,
		}
		for _, member := range group.Members {
			cmember := consumerGroupMember{
				MemberID:   member.MemberID,
				InstanceID: member.InstanceID,
				ClientID:   member.ClientID,
				ClientHost: member.ClientHost,
			}
			cmember.MemberMetadata.ReadFrom(member.ProtocolMetadata)
			cmember.MemberAssignment.ReadFrom(member.MemberAssignment)

			cgroup.Members = append(cgroup.Members, cmember)
		}
		cresp.Groups = append(cresp.Groups, cgroup)
	}

	return cresp
}
