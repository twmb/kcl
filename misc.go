package main

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

func init() {
	root.AddCommand(miscCmd())
}

func miscCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "misc",
		Short: "Miscellaneous utilities",
	}

	cmd.AddCommand(code2errCmd())
	cmd.AddCommand(metadataCmd())
	cmd.AddCommand(genAutocompleteCmd())
	cmd.AddCommand(apiVersionsCmd())

	return cmd
}

func code2errCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "code2err CODE",
		Short: "Print the name for an error code",
		Args:  cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			code, err := strconv.Atoi(args[0])
			if err != nil {
				die("unable to parse error code: %v", err)
			}
			if code == 0 {
				fmt.Println("NONE")
				return
			}
			kerr := kerr.ErrorForCode(int16(code)).(*kerr.Error)
			fmt.Printf("%s\n%s\n", kerr.Message, kerr.Description)
		},
	}
}

func metadataCmd() *cobra.Command {
	var req kmsg.MetadataRequest
	var noTopics bool

	cmd := &cobra.Command{
		Use:   "metadata",
		Short: "Issue a metadata command and dump the results",
		ValidArgs: []string{
			"--no-topics",
			"--topics",
		},
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			if noTopics { // nil means everything, empty means nothing
				req.Topics = []string{}
			}

			kresp, err := client.Request(&req)
			maybeDie(err, "unable to get metadata: %v", err)
			if asJSON {
				dumpJSON(kresp)
			}
			resp := kresp.(*kmsg.MetadataResponse)
			if resp.ClusterID != nil {
				fmt.Printf("CLUSTER\n=======\n%s\n\n", *resp.ClusterID)
			}

			fmt.Printf("BROKERS\n=======\n")
			printBrokers(resp.ControllerID, resp.Brokers)

			fmt.Printf("\nTOPICS\n======\n")
			printTopics(resp.TopicMetadata, true) // detailed does not use tw so no need to reinit

		},
	}

	// TODO --broker to query a specific broker?
	cmd.Flags().BoolVar(&noTopics, "no-topics", false, "fetch only broker metadata, no topics")
	cmd.Flags().StringSliceVarP(&req.Topics, "topics", "t", nil, "list of topics to fetch (comma separated or repeated flag)")

	return cmd
}

func genAutocompleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "gen-autocomplete",
		Short: "Generates bash completion scripts",
		Long: `To load completion run

. <(kcl misc gen-autocomplete)

To configure your bash shell to load completions for each session add to your bashrc

# ~/.bashrc or ~/.profile
. <(kcl misc gen-autocomplete)
`,
		Args: cobra.ExactArgs(0),
		Run:  func(_ *cobra.Command, _ []string) { root.GenBashCompletion(os.Stdout) },
	}
}

func apiVersionsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "api-versions",
		Short: "Print broker API versions for each Kafka request type",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			kresp, err := client.Request(new(kmsg.ApiVersionsRequest))
			maybeDie(err, "unable to request API versions: %v", err)

			if asJSON {
				dumpJSON(kresp)
				return
			}

			resp := kresp.(*kmsg.ApiVersionsResponse)
			fmt.Fprintf(tw, "NAME\tMAX\n")
			for _, k := range resp.ApiVersions {
				kind := "Unknown"
				req := kmsg.RequestForKey(k.ApiKey)
				if req != nil {
					kind = reflect.Indirect(reflect.ValueOf(req)).Type().Name()
					kind = strings.TrimSuffix(kind, "Request")
				}

				fmt.Fprintf(tw, "%s\t%d\n", kind, k.MaxVersion)
			}
			tw.Flush()
		},
	}
}
