package main

import (
	"fmt"
	"math"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/twmb/kgo"
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

	cmd.AddCommand(errcodeCmd())
	cmd.AddCommand(metadataCmd())
	cmd.AddCommand(genAutocompleteCmd())
	cmd.AddCommand(apiVersionsCmd())
	cmd.AddCommand(probeVersionCmd())

	return cmd
}

func errcodeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "errcode CODE",
		Short: "Print the name and message for an error code",
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

			kresp, err := client().Request(&req)
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
			kresp, err := client().Request(new(kmsg.ApiVersionsRequest))
			maybeDie(err, "unable to request API versions: %v", err)

			if asJSON {
				dumpJSON(kresp)
				return
			}

			resp := kresp.(*kmsg.ApiVersionsResponse)
			fmt.Fprintf(tw, "NAME\tMAX\n")
			for _, k := range resp.ApiVersions {
				kind := kmsg.NameForKey(k.ApiKey)
				if kind == "" {
					kind = "Unknown"
				}
				fmt.Fprintf(tw, "%s\t%d\n", kind, k.MaxVersion)
			}
			tw.Flush()
		},
	}
}

func probeVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "probe-version",
		Short: "Probe and print the version of Kafka running",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			// If we request against a Kafka older than ApiVersions,
			// Kafka will close the connection. ErrConnDead is
			// retried automatically, so we must stop that.
			clientOpts = append(clientOpts,
				kgo.WithRetries(0),
			)
			kresp, err := client().Request(new(kmsg.ApiVersionsRequest))
			if err != nil {
				// pre 0.10.0 had no api versions
				// 0.9.0 has list groups
				// 0.8.2 has find coordinator
				// 0.8.1 has offset fetch
				if _, err = client().Request(new(kmsg.ListGroupsRequest)); err == nil {
					fmt.Println("Kafka 0.9.0")
					return
				}
				if _, err = client().Request(new(kmsg.FindCoordinatorRequest)); err == nil {
					fmt.Println("Kafka 0.8.2")
					return
				}
				// OffsetFetch triggers FindCoordinator, which does not
				// exist and will fail. We need to directly ask a broker.
				// kgo's seed brokers start at MinInt32, so we know we are
				// getting a broker with this number.
				// TODO maybe not make specific to impl...
				if _, err = client().Broker(math.MinInt32).Request(new(kmsg.OffsetFetchRequest)); err == nil {
					fmt.Println("Kafka 0.8.1")
					return
				}
				fmt.Println("Kafka 0.8.0")
				return
			}

			resp := kresp.(*kmsg.ApiVersionsResponse)
			keys := resp.ApiVersions
			num := len(resp.ApiVersions)
			switch {
			case num == 18:
				fmt.Println("Kafka 0.10.0")
			case num == 20 && keys[6].MaxVersion == 2:
				fmt.Println("Kafka 0.10.1")
			case num == 20 && keys[6].MaxVersion == 3:
				fmt.Println("Kafka 0.10.2")
			case num == 33:
				fmt.Println("Kafka 0.11.1")
			case num == 37:
				fmt.Println("Kafka 1.0.0")
			case num == 42 && keys[0].MaxVersion == 5:
				fmt.Println("Kafka 1.1.0")
			case num == 42 && keys[0].MaxVersion == 6:
				fmt.Println("Kafka 2.0.0")
			case num == 42 && keys[0].MaxVersion == 7:
				fmt.Println("Kafka 2.1.0")
			case num == 43:
				fmt.Println("Kafka 2.2.0")
			case num == 44:
				fmt.Println("Kafka 2.3.0")
			case num == 46:
				fmt.Println("Kafka 2.4.0")
			default:
				fmt.Println("Unknown version: either tip or between releases")
			}
		},
	}
}
