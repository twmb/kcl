package main

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	"github.com/twmb/kgo/kmsg"
)

func init() {
	root.AddCommand(metadataCmd())
	root.AddCommand(brokersCmd())
}

// kcl metadata (--no-topics) (--topics foo,bar,biz,baz) (--allow-autocreate)
func metadataCmd() *cobra.Command {
	var req kmsg.MetadataRequest
	var noTopics bool

	cmd := &cobra.Command{
		Use:       "metadata",
		Short:     "issue a metadata command and JSON dump the results",
		ValidArgs: []string{"--no-topics", "--topics", "brokers"},
		Run: func(_ *cobra.Command, _ []string) {
			if noTopics {
				req.Topics = []string{}
			}

			resp, err := client.Request(&req)
			maybeDie(err, "unable to get metadata: %v", err)
			dumpJSON(resp)
		},
	}

	cmd.Flags().BoolVar(&noTopics, "no-topics", false, "fetch only broker metadata, no topics")
	cmd.Flags().StringSliceVarP(&req.Topics, "topics", "t", nil, "comma separated list of topics to fetch")
	cmd.Flags().BoolVar(&req.AllowAutoTopicCreation, "allow-auto-topic-creation", false, "allow auto topic creation if topics do not exist")

	return cmd
}

// kcl brokers
func brokersCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "brokers",
		Short: "nicely print all brokers from a metadata command (the controller is marked with *)",
		Run: func(_ *cobra.Command, _ []string) {
			kresp, err := client.Request(new(kmsg.MetadataRequest))
			maybeDie(err, "unable to get metadata: %v", err)
			resp := kresp.(*kmsg.MetadataResponse)

			brokers := resp.Brokers
			sort.Slice(brokers, func(i, j int) bool {
				return brokers[i].NodeID < brokers[j].NodeID
			})

			fmt.Fprintf(tw, "ID\tHOST\tPORT\tRACK\n")
			for _, broker := range brokers {
				var controllerStar string
				if broker.NodeID == resp.ControllerID {
					controllerStar = "*"
				}

				var rack string
				if broker.Rack != nil {
					rack = *broker.Rack
				}

				fmt.Fprintf(tw, "%d%s\t%s\t%d\t%s\n",
					broker.NodeID, controllerStar, broker.Host, broker.Port, rack)
			}

			tw.Flush()
		},
	}
}
