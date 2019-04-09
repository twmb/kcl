package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/twmb/kgo"
)

var (
	tw = tabwriter.NewWriter(os.Stdout, 6, 4, 2, ' ', 0) // used for writing anything pretty

	reqTimeoutMillis int32 = 1000 // used for all requests, configurable by flag

	client *kgo.Client // used for all requests

	root = cobra.Command{
		Use:   "kcl",
		Short: "Kafka Command Line command for commanding Kafka on the command line",
	}

	completionCmd = cobra.Command{
		Use:   "autocomplete",
		Short: "Generates bash completion scripts",
		Long: `To load completion run

. <(kcl autocomplete)

To configure your bash shell to load completions for each session add to your bashrc

# ~/.bashrc or ~/.profile
. <(kcl autocomplete)
`,
		Run: func(_ *cobra.Command, _ []string) { root.GenBashCompletion(os.Stdout) },
	}
)

func main() {
	var seedBrokers []string
	root.PersistentFlags().StringSliceVarP(&seedBrokers, "brokers", "b", []string{"127.0.0.1"}, "comma delimited list of seed brokers")
	root.PersistentFlags().Int32Var(&reqTimeoutMillis, "timeout-ms", 1000, "millisecond timeout for requests that have timeouts")

	cobra.OnInitialize(func() {
		var err error
		client, err = kgo.NewClient(seedBrokers)
		if err != nil {
			fmt.Printf("unable to create client: %v", err)
			os.Exit(1)
		}
	})
	root.AddCommand(&completionCmd)

	if err := root.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
