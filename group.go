package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

func describeGroupCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "groups GROUP_ID...",
		Short: "Describe Kafka groups",
		Long: `Describe Kafka groups.

This command supports JSON output.
`,
		Run: func(_ *cobra.Command, args []string) {
			req := kmsg.DescribeGroupsRequest{
				GroupIDs: args,
			}

			kresp, err := client().Request(&req)
			maybeDie(err, "unable to describe groups: %v", err)

			if asJSON {
				dumpJSON(kresp)
				return
			}

			resp := kresp.(*kmsg.DescribeGroupsResponse)
			fmt.Fprintf(tw, "ID\tSTATE\tPROTO TYPE\tPROTO\tERROR\n")
			for _, group := range resp.Groups {
				errMsg := ""
				if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
					errMsg = err.Error()
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
					group.GroupID,
					group.State,
					group.ProtocolType,
					group.Protocol,
					errMsg,
				)
			}
			tw.Flush()
		},
	}
}

func describeGroupsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "group GROUP_ID",
		Short: "Describe an individual Kafka group and its members",
		Long: `Describe a single lKafka group and its members.

This command supports JSON output.
`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			req := kmsg.DescribeGroupsRequest{
				GroupIDs: args,
			}

			kresp, err := client().Request(&req)
			maybeDie(err, "unable to describe groups: %v", err)

			if asJSON {
				dumpJSON(kresp)
				return
			}

			resp := kresp.(*kmsg.DescribeGroupsResponse)
			if len(resp.Groups) != 1 {
				die("quitting; one group requested but received %d", len(resp.Groups))
			}
			group := resp.Groups[0]
			if err = kerr.ErrorForCode(group.ErrorCode); err != nil {
				die("%s", err)
			}

			fmt.Fprintf(tw, "ID\t%s\n", group.GroupID)
			fmt.Fprintf(tw, "STATE\t%s\n", group.State)
			fmt.Fprintf(tw, "PROTO TYPE\t%s\n", group.ProtocolType)
			fmt.Fprintf(tw, "PROTO\t%s\n", group.Protocol)
			fmt.Fprintf(tw, "MEMBERS\t\n")

			tw.Flush()
			reinitTW()
			fmt.Fprintf(tw, "\tID\tCLIENT ID\tCLIENT HOST\tMETADATA\tASSIGNMENT\n")
			for _, member := range group.Members {

				fmt.Fprintf(tw, "\t%s\t%s\t%s\t%s\t%s\n",
					member.MemberID,
					member.ClientID,
					member.ClientHost,
					member.MemberMetadata,
					member.MemberAssignment)
			}
			tw.Flush()

		},
	}
}
