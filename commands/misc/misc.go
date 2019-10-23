// Package misc contains miscellaneous, unspecific commands.
package misc

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kgo"
	"github.com/twmb/kafka-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "misc",
		Short: "Miscellaneous utilities",
	}

	cmd.AddCommand(errcodeCommand())
	cmd.AddCommand(genAutocompleteCommand())
	cmd.AddCommand(apiVersionsCommand(cl))
	cmd.AddCommand(probeVersionCommand(cl))
	cmd.AddCommand(electLeaderCommand(cl))
	cmd.AddCommand(offsetForLeaderEpoch(cl))
	cmd.AddCommand(rawCommand(cl))
	cmd.AddCommand(deleteRecordsCommand(cl))

	return cmd
}

func errcodeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "errcode CODE",
		Short: "Print the name and message for an error code",
		Args:  cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			code, err := strconv.Atoi(args[0])
			if err != nil {
				out.Die("unable to parse error code: %v", err)
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

func genAutocompleteCommand() *cobra.Command {
	var kind string

	cmd := &cobra.Command{
		Use:   "gen-autocomplete",
		Short: "Generates bash completion scripts",
		Long: `To load completion run

. <(kcl misc gen-autocomplete -kbash)

To configure your shell to load completions for each session add to your bashrc
(or equivalent, for your shell depending on support):

# ~/.bashrc or ~/.profile
if [ -f /etc/bash_completion ] && ! shopt -oq posix; then
    . /etc/bash_completion
    . <(kcl misc gen-autocomplete -kbash)
fi

This command supports completion for bash, zsh, and powershell.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			switch kind {
			case "bash":
				cmd.Root().GenBashCompletion(os.Stdout)
			case "zsh":
				cmd.Root().GenZshCompletion(os.Stdout)
			case "powershell":
				cmd.Root().GenPowerShellCompletion(os.Stdout)
			default:
				out.Die("unrecognized autocomplete kind %q", kind)
			}
		},
	}

	cmd.Flags().StringVarP(&kind, "kind", "k", "bash", "autocomplete kind (bash, zsh, powershell)")

	return cmd
}

func apiVersionsCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "api-versions",
		Short: "Print broker API versions for each Kafka request type",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			kresp, err := cl.Client().Request(context.Background(), new(kmsg.ApiVersionsRequest))
			out.MaybeDie(err, "unable to request API versions: %v", err)

			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

			resp := kresp.(*kmsg.ApiVersionsResponse)
			fmt.Fprintf(tw, "NAME\tMAX\n")
			for _, k := range resp.ApiKeys {
				kind := kmsg.NameForKey(k.ApiKey)
				if kind == "" {
					kind = "Unknown"
				}
				fmt.Fprintf(tw, "%s\t%d\n", kind, k.MaxVersion)
			}
		},
	}
}

func probeVersionCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "probe-version",
		Short: "Probe and print the version of Kafka running",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			probeVersion(cl)
		},
	}
}

// probeVersion prints what version of Kafka the client is interacting with.
func probeVersion(cl *client.Client) {
	// If we request against a Kafka older than ApiVersions,
	// Kafka will close the connection. ErrConnDead is
	// retried automatically, so we must stop that.
	cl.AddOpt(kgo.WithRetries(0))
	kresp, err := cl.Client().Request(context.Background(), new(kmsg.ApiVersionsRequest))
	if err != nil { // pre 0.10.0 had no api versions
		// 0.9.0 has list groups
		if _, err = cl.Client().Request(context.Background(), new(kmsg.ListGroupsRequest)); err == nil {
			fmt.Println("Kafka 0.9.0")
			return
		}
		// 0.8.2 has find coordinator
		if _, err = cl.Client().Request(context.Background(), new(kmsg.FindCoordinatorRequest)); err == nil {
			fmt.Println("Kafka 0.8.2")
			return
		}
		// 0.8.1 has offset fetch
		// OffsetFetch triggers FindCoordinator, which does not
		// exist and will fail. We need to directly ask a broker.
		// kgo's seed brokers start at MinInt32, so we know we are
		// getting a broker with this number.
		// TODO maybe not make specific to impl...
		if _, err = cl.Client().Broker(math.MinInt32).Request(context.Background(), new(kmsg.OffsetFetchRequest)); err == nil {
			fmt.Println("Kafka 0.8.1")
			return
		}
		fmt.Println("Kafka 0.8.0")
		return
	}

	resp := kresp.(*kmsg.ApiVersionsResponse)
	keys := resp.ApiKeys
	num := len(resp.ApiKeys)
	switch {
	case num == 19:
		fmt.Println("Kafka 0.10.0")
	case num == 21 && keys[6].MaxVersion == 2:
		fmt.Println("Kafka 0.10.1")
	case num == 21 && keys[6].MaxVersion == 3:
		fmt.Println("Kafka 0.10.2")
	case num == 34:
		fmt.Println("Kafka 0.11.1")
	case num == 38:
		fmt.Println("Kafka 1.0.0")
	case num == 43 && keys[0].MaxVersion == 5:
		fmt.Println("Kafka 1.1.0")
	case num == 43 && keys[0].MaxVersion == 6:
		fmt.Println("Kafka 2.0.0")
	case num == 43 && keys[0].MaxVersion == 7:
		fmt.Println("Kafka 2.1.0")
	case num == 44:
		fmt.Println("Kafka 2.2.0")
	case num == 45:
		fmt.Println("Kafka 2.3.0")
	case num == 48:
		fmt.Println("Kafka 2.4.0")
	default:
		fmt.Println("Unknown version: either tip or between releases")
	}
}

func electLeaderCommand(cl *client.Client) *cobra.Command {
	var topic string
	var partitions []int32
	var unclean bool
	var run bool

	cmd := &cobra.Command{
		Use:   "elect-leaders",
		Short: "Trigger leader elections for partitions",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			if !run {
				out.Die("use --run to run this command")
			}
			req := &kmsg.ElectLeadersRequest{
				ElectionType: func() int8 {
					if unclean {
						return 1
					}
					return 0
				}(),
				TimeoutMillis: cl.TimeoutMillis(),
			}
			if topic != "" {
				req.Topics = append(req.Topics, kmsg.ElectLeadersRequestTopic{
					Topic:      topic,
					Partitions: partitions,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to elect leaders: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			resp := kresp.(*kmsg.ElectLeadersResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.Die("%v", err)
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					errKind := "OK"
					var msg string
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						errKind = err.Error()
					}
					if partition.ErrorMessage != nil {
						msg = *partition.ErrorMessage
					}
					fmt.Fprintf(tw, "%s\t%d\t%v\t%s\n",
						topic.Topic,
						partition.Partition,
						errKind,
						msg,
					)
				}
			}
		},
	}

	cmd.Flags().StringVarP(&topic, "topic", "t", "", "topic to trigger elections for")
	cmd.Flags().Int32SliceVarP(&partitions, "partitions", "p", nil, "comma delimited list of specific partitions to trigger elections for")
	cmd.Flags().BoolVar(&unclean, "unclean", false, "allow unclean leader election (requires 2.4.0)")
	cmd.Flags().BoolVarP(&run, "run", "r", false, "actually run the command (avoids accidental elections without this flag)")

	return cmd
}

func offsetForLeaderEpoch(cl *client.Client) *cobra.Command {
	var topic string
	var partitions []int32
	var currentLeaderEpoch int32
	var leaderEpoch int32
	var broker int32

	cmd := &cobra.Command{
		Use:   "offset-for-leader-epoch",
		Short: "See the offsets for a leader epoch.",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			req := &kmsg.OffsetForLeaderEpochRequest{
				ReplicaID: -1,
			}
			reqTopic := kmsg.OffsetForLeaderEpochRequestTopic{
				Topic: topic,
			}
			for _, partition := range partitions {
				reqTopic.Partitions = append(reqTopic.Partitions, kmsg.OffsetForLeaderEpochRequestTopicPartition{
					Partition:          partition,
					CurrentLeaderEpoch: currentLeaderEpoch,
					LeaderEpoch:        leaderEpoch,
				})
			}
			req.Topics = append(req.Topics, reqTopic)
			kresp, err := cl.Client().Broker(int(broker)).Request(context.Background(), req)
			out.MaybeDie(err, "unable to to get offsets for leader epoch: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			resp := kresp.(*kmsg.OffsetForLeaderEpochResponse)

			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "TOPIC\tPARTITION\tLEADER EPOCH\tEND OFFSET\tERROR\n")
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					var msg string
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						msg = err.Error()
					}
					fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%s\n",
						topic.Topic,
						partition.Partition,
						partition.LeaderEpoch,
						partition.EndOffset,
						msg,
					)
				}
			}
		},
	}

	cmd.Flags().StringVarP(&topic, "topic", "t", "", "topic to trigger elections for")
	cmd.Flags().Int32SliceVarP(&partitions, "partitions", "p", nil, "comma delimited list of specific partitions to trigger elections for")
	cmd.Flags().Int32VarP(&currentLeaderEpoch, "current-leader-epoch", "c", -1, "current leader epoch to use in the request")
	cmd.Flags().Int32VarP(&leaderEpoch, "leader-epoch", "e", 0, "leader epoch to ask for")
	cmd.Flags().Int32VarP(&broker, "broker", "b", 1, "broker to query")

	return cmd
}

func rawCommand(cl *client.Client) *cobra.Command {
	var key int16
	cmd := &cobra.Command{
		Use:   "raw-req",
		Short: "Issue an arbitrary request as parsed from JSON STDIN.",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			req := kmsg.RequestForKey(key)
			if req == nil {
				out.Die("request key %d unknown", key)
			}
			raw, err := ioutil.ReadAll(os.Stdin)
			out.MaybeDie(err, "unable to read stdin: %v", err)
			err = json.Unmarshal(raw, req)
			out.MaybeDie(err, "unable to unmarshal stdin: %v", err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			kresp, err := cl.Client().Request(ctx, req)
			out.MaybeDie(err, "response error: %v", err)
			out.ExitJSON(kresp)
		},
	}
	cmd.Flags().Int16VarP(&key, "key", "k", -1, "request key")
	return cmd
}

func deleteRecordsCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete-records",
		Short: "Delete topic foo partition 0 offset 10.",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			req := &kmsg.DeleteRecordsRequest{
				Topics: []kmsg.DeleteRecordsRequestTopic{{
					Topic: "foo",
					Partitions: []kmsg.DeleteRecordsRequestTopicPartition{{
						Partition: 0,
						Offset:    10,
					}},
				}},
				TimeoutMillis: cl.TimeoutMillis(),
			}
			kresp, err := cl.Client().Broker(2).Request(context.Background(), req)
			out.MaybeDie(err, "unable to to get delete records: %v", err)
			out.ExitJSON(kresp)
		},
	}
	return cmd
}
