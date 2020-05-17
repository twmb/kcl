package group

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func describeCommand(cl *client.Client) *cobra.Command {
	var req kmsg.DescribeGroupsRequest
	var verbose bool

	// TODO --lag to also issue fetch offsets & list offsets
	// TODO include authorized options
	cmd := &cobra.Command{
		Use:   "describe GROUPS...",
		Short: "Describe Kafka groups",
		Args:  cobra.MinimumNArgs(1),
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
					describeGroupVerbose(&group)
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

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose printing including client id, host, user data")

	return cmd
}

func describeGroupVerbose(group *consumerGroup) {
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
	fmt.Println()

	var sb strings.Builder
	tw = out.BeginTabWriteTo(&sb)
	fmt.Fprintf(tw, "MEMBER ID\tINSTANCE ID\tCLIENT ID\tCLIENT HOST\tUSER DATA\n")
	for _, member := range group.Members {
		instanceID := ""
		if member.InstanceID != nil {
			instanceID = *member.InstanceID
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			member.MemberID,
			instanceID,
			member.ClientID,
			member.ClientHost,
			string(member.MemberAssignment.UserData))
	}
	tw.Flush()

	lines := strings.Split(sb.String(), "\n")
	lines = lines[:len(lines)-1] // trim trailing empty line
	fmt.Println(lines[0])

	for i, line := range lines[1:] {
		fmt.Println(line)

		member := group.Members[i]
		assignment := &member.MemberAssignment
		if len(assignment.Topics) == 0 {
			continue
		}

		sort.Slice(assignment.Topics, func(i, j int) bool {
			return assignment.Topics[i].Topic < assignment.Topics[j].Topic
		})

		for _, topic := range assignment.Topics {
			sort.Slice(topic.Partitions, func(i, j int) bool { return topic.Partitions[i] < topic.Partitions[j] })
			fmt.Printf("\t%s => %v\n", topic.Topic, topic.Partitions)
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
