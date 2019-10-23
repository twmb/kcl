// Package broker contains broker related utilities and subcommands.
package broker

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"

	"github.com/twmb/kafka-go/pkg/kmsg"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "broker",
		Short: "Perform broker related actions",
	}

	cmd.AddCommand(brokerListCommand(cl))

	return cmd
}

func brokerListCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all brokers (the controller is marked with *)",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			kresp, err := cl.Client().Request(context.Background(), new(kmsg.MetadataRequest))
			out.MaybeDie(err, "unable to get metadata: %v", err)
			resp := kresp.(*kmsg.MetadataResponse)

			if cl.AsJSON() {
				out.ExitJSON(resp.Brokers)
			}

			PrintBrokers(resp.ControllerID, resp.Brokers)
		},
	}
}

// PrintBrokers prints tab written brokers to stdout.
func PrintBrokers(controllerID int32, brokers []kmsg.MetadataResponseBroker) {
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].NodeID < brokers[j].NodeID
	})

	tw := out.BeginTabWrite()
	defer tw.Flush()

	fmt.Fprintf(tw, "ID\tHOST\tPORT\tRACK\n")
	for _, broker := range brokers {
		var controllerStar string
		if broker.NodeID == controllerID {
			controllerStar = "*"
		}

		var rack string
		if broker.Rack != nil {
			rack = *broker.Rack
		}

		fmt.Fprintf(tw, "%d%s\t%s\t%d\t%s\n",
			broker.NodeID, controllerStar, broker.Host, broker.Port, rack)
	}
}
