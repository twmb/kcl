package main

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	"github.com/twmb/kgo/kmsg"
)

func init() {
	root.AddCommand(brokerCmd())
}

func brokerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "broker",
		Short: "Perform broker related actions",
	}

	cmd.AddCommand(brokerListCmd())
	cmd.AddCommand(brokerDescribeConfigCmd())
	cmd.AddCommand(brokerAlterConfigCmd())

	return cmd
}

func brokerListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all brokers (the controller is marked with *)",
		Run: func(_ *cobra.Command, _ []string) {
			kresp, err := client().Request(new(kmsg.MetadataRequest))
			maybeDie(err, "unable to get metadata: %v", err)
			resp := kresp.(*kmsg.MetadataResponse)

			if asJSON {
				dumpJSON(resp.Brokers)
				return
			}

			printBrokers(resp.ControllerID, resp.Brokers)
		},
	}
}

func printBrokers(controllerID int32, brokers []kmsg.MetadataResponseBroker) {
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].NodeID < brokers[j].NodeID
	})

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

	tw.Flush()
}

func brokerDescribeConfigCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "describe [BROKER_ID]",
		Short: "Describe a broker",
		Long: `Print key/value config pairs for a broker.

This command prints all key/value config values for a broker, as well
as the value's source. Read only keys are suffixed with *.

If no broker ID is used, only dynamic (manually sete) key/value pairs are
printed. If you wish to describe the full config for a specific broker,
be sure to pass a broker ID.

The JSON output option includes all config synonyms
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			describeConfig(args, true)
		},
	}
}

func brokerAlterConfigCmd() *cobra.Command {
	return alterConfigsCmd(
		"alter-config [BROKER_ID]",
		"Alter all broker configurations or a single broker configuration",
		`Alter all broker configurations or a single broker configuration.

Updating an individual broker allows for reloading the broker's password
files (even if the file path has not changed) and for setting password fields.
Broker-wide updates do neither of these.

Altering configs requires listing all dynamic config options for any alter.
The incremental alter, added in Kafka 2.3.0, allows adding or removing
individual values.

Since an alter may inadvertently lose existing config values, this command
by default checks for existing config key/value pairs that may be lost in
the alter and prompts if it is OK to lose those values. To skip this check,
use the --no-confirm flag.
`,
		true,
	)
}
