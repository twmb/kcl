// Package misc contains miscellaneous, unspecific commands.
package misc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/commands/admin/topic"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

func apiVersionsRequest() *kmsg.ApiVersionsRequest {
	req := kmsg.NewPtrApiVersionsRequest()
	req.ClientSoftwareName = "kcl"
	req.ClientSoftwareVersion = "v0.0.0"
	return req
}

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "misc",
		Short: "Miscellaneous utilities (version probing, error code/text, offset listing).",
	}

	cmd.AddCommand(errcodeCommand())
	cmd.AddCommand(errtextCommand())
	cmd.AddCommand(genAutocompleteCommand())
	cmd.AddCommand(apiVersionsCommand(cl))
	cmd.AddCommand(probeVersionCommand(cl))
	cmd.AddCommand(rawCommand(cl))
	cmd.AddCommand(listOffsetsCommand(cl))
	cmd.AddCommand(offsetForLeaderEpochCommand(cl))

	return cmd
}

// printKerr prints one Kafka error in the requested format; text is the
// text form, which differs between errcode and errtext.
func printKerr(format, command string, code int16, name, description, text string) {
	switch format {
	case out.FormatJSON:
		out.MarshalJSON(command, 1, map[string]any{"code": code, "name": name, "description": description})
	case out.FormatAWK:
		fmt.Printf("%s\t%d\t%s\n", name, code, description)
	default:
		fmt.Print(text)
	}
}

func errcodeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "errcode CODE",
		Short: "Print the name and description for an error code.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			code, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("unable to parse error code: %v", err)
			}
			format, _ := cmd.Flags().GetString("format")
			if code == 0 {
				printKerr(format, out.CommandName(cmd.CommandPath()), 0, "NONE", "", "NONE\n")
				return nil
			}
			kerr := kerr.ErrorForCode(int16(code)).(*kerr.Error)
			printKerr(format, out.CommandName(cmd.CommandPath()), kerr.Code, kerr.Message, kerr.Description, fmt.Sprintf("%s\n%s\n", kerr.Message, kerr.Description))
			return nil
		},
	}
}

func errtextCommand() *cobra.Command {
	var list, verbose bool
	cmd := &cobra.Command{
		Use:   "errtext [ERROR_NAME]",
		Short: "Print the name, code and description for an error name or all errors.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, _ := cmd.Flags().GetString("format")
			var text string
			if list {
				if len(args) != 0 {
					return out.Errf(out.ExitUsage, "invalid extra args while list is set")
				}
			} else {
				if len(args) != 1 {
					return out.Errf(out.ExitUsage, "missing error text to search for")
				} else {
					text = client.Strnorm(args[0])
				}
			}

			var table *out.FormattedTable
			if list && format != out.FormatText {
				table = out.NewFormattedTable(format, out.CommandName(cmd.CommandPath()), 1, "errors", "NAME", "CODE", "DESCRIPTION")
				defer table.Flush()
			}
			var err error
			for code := int16(1); err != kerr.UnknownServerError; code++ {
				err = kerr.ErrorForCode(code)
				kerr := err.(*kerr.Error)
				if list {
					if table != nil {
						table.Row(kerr.Message, kerr.Code, kerr.Description)
					} else {
						fmt.Printf("%s (%d)\n%s\n\n", kerr.Message, kerr.Code, kerr.Description)
					}
					continue
				}

				if verbose {
					fmt.Fprintf(os.Stderr, "trying %s...\n", kerr.Message)
				}
				if client.Strnorm(kerr.Message) == text {
					printKerr(format, out.CommandName(cmd.CommandPath()), kerr.Code, kerr.Message, kerr.Description, fmt.Sprintf("%s (%d)\n%s\n", kerr.Message, kerr.Code, kerr.Description))
					return nil
				}
			}
			if !list {
				return fmt.Errorf("Unknown error text.")
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&list, "list", false, "rather than comparing, list all errors and their descriptions")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "verbosely print errors compared against")
	return cmd
}

func genAutocompleteCommand() *cobra.Command {
	var kind string

	cmd := &cobra.Command{
		Use:   "gen-autocomplete",
		Short: "Generates bash completion scripts.",
		Long: `Generates bash completion scripts.

To load completion run

. <(kcl misc gen-autocomplete -kbash)

To configure your shell to load completions for each session add to your bashrc
(or equivalent, for your shell depending on support):

# ~/.bashrc or ~/.profile
if [ -f /etc/bash_completion ] && ! shopt -oq posix; then
    . /etc/bash_completion
    . <(kcl misc gen-autocomplete -kbash)
fi

This command supports completion for bash, zsh, fish, and powershell.
`,
		Args: cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, _ []string) error {
			switch kind {
			case "bash":
				cmd.Root().GenBashCompletion(os.Stdout)
			case "zsh":
				cmd.Root().GenZshCompletion(os.Stdout)
			case "fish":
				cmd.Root().GenFishCompletion(os.Stdout, true)
			case "powershell":
				cmd.Root().GenPowerShellCompletion(os.Stdout)
			default:
				return out.Errf(out.ExitUsage, "unrecognized autocomplete kind %q", kind)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&kind, "kind", "k", "bash", "autocomplete kind (bash, zsh, fish, powershell)")

	return cmd
}

func apiVersionsCommand(cl *client.Client) *cobra.Command {
	var keys bool
	var version string

	cmd := &cobra.Command{
		Use:   "api-versions",
		Short: "Print broker API versions for each Kafka request type (Kafka 0.10.0+).",
		Args:  cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			var v *kversion.Versions
			if version == "" {
				kresp, err := cl.Client().Request(context.Background(), apiVersionsRequest())
				if err != nil {
					return fmt.Errorf("unable to request API versions: %v", err)
				}
				resp := kresp.(*kmsg.ApiVersionsResponse)
				v = kversion.FromApiVersionsResponse(resp)
			} else {
				v = kversion.FromString(version)
				if v == nil {
					return out.Errf(out.ExitUsage, "unknown version %q", version)
				}
			}

			if keys {
				table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "api_versions",
					"NAME", "KEY", "MAX")
				v.EachMaxKeyVersion(func(k, ver int16) {
					kind := kmsg.NameForKey(k)
					if kind == "" {
						kind = "Unknown"
					}
					table.Row(kind, k, ver)
				})
				if err := table.Flush(); err != nil {
					return err
				}
			} else {
				table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "api_versions",
					"NAME", "MAX")
				v.EachMaxKeyVersion(func(k, ver int16) {
					kind := kmsg.NameForKey(k)
					if kind == "" {
						kind = "Unknown"
					}
					table.Row(kind, ver)
				})
				if err := table.Flush(); err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&version, "version", "v", "", "if non-empty, print the api versions for a specific version rather than the broker's version")
	cmd.Flags().BoolVar(&keys, "with-key-nums", false, "include key numbers in the output; useful if the output contains Unknown")

	return cmd
}

func probeVersionCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "probe-version",
		Short: "Probe and print the version of Kafka running (incompatible with --as-version).",
		Args:  cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			return probeVersion(cl)
		},
	}
}

// probeVersion prints what version of Kafka the client is interacting with.
func probeVersion(cl *client.Client) error {
	// If we request against a Kafka older than ApiVersions,
	// Kafka will close the connection. ErrConnDead is
	// retried automatically, so we must stop that.
	cl.AddOpt(kgo.RequestRetries(0))
	kresp, err := cl.Client().Request(context.Background(), apiVersionsRequest())
	if err != nil { // pre 0.10.0 had no api versions
		cl.RemakeWithOpts(kgo.MaxVersions(kversion.V0_9_0()))
		// 0.9.0 has list groups
		if _, err = cl.Client().SeedBrokers()[0].Request(context.Background(), new(kmsg.ListGroupsRequest)); err == nil {
			printVersionGuess(cl.Format(), cl.Command(), "0.9.0")
			return nil
		}
		cl.RemakeWithOpts(kgo.MaxVersions(kversion.V0_8_2()))
		// 0.8.2 has find coordinator
		if _, err = cl.Client().SeedBrokers()[0].Request(context.Background(), new(kmsg.FindCoordinatorRequest)); err == nil {
			printVersionGuess(cl.Format(), cl.Command(), "0.8.2")
			return nil
		}
		cl.RemakeWithOpts(kgo.MaxVersions(kversion.V0_8_1()))
		// 0.8.1 has offset fetch
		if _, err = cl.Client().SeedBrokers()[0].Request(context.Background(), new(kmsg.OffsetFetchRequest)); err == nil {
			printVersionGuess(cl.Format(), cl.Command(), "0.8.1")
			return nil
		}
		printVersionGuess(cl.Format(), cl.Command(), "0.8.0")
		return nil
	}

	resp := kresp.(*kmsg.ApiVersionsResponse)
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return fmt.Errorf("ApiVersions request failed: %v", err)
	}

	v := kversion.FromApiVersionsResponse(resp)
	printVersionGuess(cl.Format(), cl.Command(), v.VersionGuess())
	return nil
}

// printVersionGuess prints kversion's guess in the requested format. The
// guess is a sentence, "between v1.0 and v1.1" or "at least v4.0" or a bare
// "v3.7", and that is what text prints. json and awk get the ends of the range
// it names instead, and an end the guess leaves open is empty.
func printVersionGuess(format, command, guess string) {
	min, max := splitVersionGuess(guess)
	switch format {
	case out.FormatJSON:
		out.MarshalJSON(command, 1, map[string]any{
			"guess": guess,
			"min":   min,
			"max":   max,
		})
	case out.FormatAWK:
		fmt.Printf("%s\t%s\n", min, max)
	default:
		fmt.Println("Kafka " + guess)
	}
}

// splitVersionGuess pulls the version or versions out of kversion's sentence.
// An exact guess is both ends of a range of one.
func splitVersionGuess(guess string) (min, max string) {
	switch {
	case guess == "unknown custom version":
		return "", ""
	case strings.HasPrefix(guess, "between "):
		lo, hi, ok := strings.Cut(strings.TrimPrefix(guess, "between "), " and ")
		if !ok {
			return "", ""
		}
		return lo, hi
	case strings.HasPrefix(guess, "not even "):
		return "", strings.TrimPrefix(guess, "not even ")
	case strings.Contains(guess, "at least "):
		_, v, _ := strings.Cut(guess, "at least ")
		return v, ""
	}
	return guess, guess
}

func rawCommand(cl *client.Client) *cobra.Command {
	var key int16
	var version int16
	var b int
	cmd := &cobra.Command{
		Use:   "raw-req",
		Short: "Issue an arbitrary request parsed from JSON read from STDIN.",
		Long: `Issue an arbitrary request parsed from JSON read from STDIN.

The request body is unmarshaled from STDIN JSON. Empty input ({}) is
valid for requests with no required fields.

The wire version used is:
  1. --version if set (pins both min and max to that version)
  2. the JSON body's "Version" field if non-negative
  3. the client's negotiated max version otherwise
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			if key < 0 {
				return out.Errf(out.ExitUsage, "--key is required")
			}
			req := kmsg.RequestForKey(key)
			if req == nil {
				return out.Errf(out.ExitUsage, "request key %d unknown", key)
			}
			req.SetVersion(-1)
			raw, err := io.ReadAll(os.Stdin)
			if err != nil {
				return fmt.Errorf("unable to read stdin: %v", err)
			}
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, req); err != nil {
					return fmt.Errorf("unable to unmarshal stdin: %v", err)
				}
			}
			// Flag --version wins over JSON "Version" wins over
			// default (no pin).
			pinVersion := int16(-1)
			if version >= 0 {
				pinVersion = version
			} else if v := req.GetVersion(); v >= 0 {
				pinVersion = v
			}
			if pinVersion >= 0 {
				req.SetVersion(pinVersion)
				maxVers := kversion.Stable()
				maxVers.SetMaxKeyVersion(req.Key(), pinVersion)
				cl.AddOpt(kgo.MaxVersions(maxVers))
				// MinVersions is consulted per-request, including
				// the internal requests franz-go issues (e.g.
				// Metadata for routing). Only pin the user's key;
				// leave other keys unset so they negotiate normally
				// against the broker's actual supported versions.
				minVers := &kversion.Versions{}
				minVers.SetMaxKeyVersion(req.Key(), pinVersion)
				cl.AddOpt(kgo.MinVersions(minVers))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var r interface {
				Request(context.Context, kmsg.Request) (kmsg.Response, error)
			}
			r = cl.Client()
			if b >= 0 {
				r = cl.Client().Broker(b)
			}
			kresp, err := r.Request(ctx, req)
			if err != nil {
				return fmt.Errorf("response error: %v", err)
			}
			out.MarshalJSON(cl.Command(), 1, map[string]any{
				"response": kresp,
			})
			return nil
		},
	}
	cmd.Flags().Int16VarP(&key, "key", "k", -1, "request key")
	cmd.Flags().Int16VarP(&version, "version", "v", -1, "pin the request to a specific version; overrides any Version in STDIN JSON")
	cmd.Flags().IntVarP(&b, "broker", "b", -1, "specific broker to issue this request to, if non-negative")
	return cmd
}

// listOffsetsCommand is the old home of "kcl topic list-offsets", kept so
// that a script naming "kcl misc list-offsets" keeps working.
func listOffsetsCommand(cl *client.Client) *cobra.Command {
	cmd := topic.ListOffsetsCommand(cl)
	cmd.Hidden = true
	cmd.Deprecated = "use 'kcl topic list-offsets' instead"
	return cmd
}

func offsetForLeaderEpochCommand(cl *client.Client) *cobra.Command {
	var currentLeaderEpoch int32
	var leaderEpoch int32

	cmd := &cobra.Command{
		Use:   "offset-for-leader-epoch",
		Short: "See the offsets for a leader epoch.",
		Long: `See the offsets for a leader epoch.

This is an advanced command strictly for debugging purposes. To discover what
it does, read the documentation for kmsg.OffsetForLeaderEpochRequest.
`,

		Example: "kcl misc offset-for-leader-epoch foo bar biz:0,1,2",
		RunE: func(_ *cobra.Command, topicParts []string) error {
			tps, err := loadTopicParts(cl, topicParts)
			if err != nil {
				return err
			}

			req := &kmsg.OffsetForLeaderEpochRequest{
				ReplicaID: -1,
			}
			for topic, parts := range tps {
				reqTopic := kmsg.OffsetForLeaderEpochRequestTopic{
					Topic: topic,
				}
				for _, partition := range parts {
					reqTopic.Partitions = append(reqTopic.Partitions, kmsg.OffsetForLeaderEpochRequestTopicPartition{
						Partition:          partition,
						CurrentLeaderEpoch: currentLeaderEpoch,
						LeaderEpoch:        leaderEpoch,
					})
				}
				req.Topics = append(req.Topics, reqTopic)
			}

			shards := cl.Client().RequestSharded(context.Background(), req)
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "epochs",
				"BROKER", "TOPIC", "PARTITION", "LEADER-EPOCH", "END-OFFSET", "ERROR")

			for _, shard := range shards {
				if shard.Err != nil {
					fmt.Fprintf(os.Stderr, "unable to issue request to broker %d (%s:%d): %v\n", shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, shard.Err)
					continue
				}

				resp := shard.Resp.(*kmsg.OffsetForLeaderEpochResponse)

				sort.Slice(resp.Topics, func(i, j int) bool { return resp.Topics[i].Topic < resp.Topics[j].Topic })
				for _, topic := range resp.Topics {
					sort.Slice(topic.Partitions, func(i, j int) bool { return topic.Partitions[i].Partition < topic.Partitions[j].Partition })
					for _, partition := range topic.Partitions {
						var msg string
						if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
							msg = err.Error()
						}
						table.Row(
							shard.Meta.NodeID,
							topic.Topic,
							partition.Partition,
							partition.LeaderEpoch,
							partition.EndOffset,
							msg,
						)
					}
				}
			}
			return table.Flush()
		},
	}

	cmd.Flags().Int32VarP(&currentLeaderEpoch, "current-leader-epoch", "c", -1, "current leader epoch to use in the request")
	cmd.Flags().Int32VarP(&leaderEpoch, "leader-epoch", "e", 0, "leader epoch to ask for")

	return cmd
}

func loadTopicParts(cl *client.Client, topicParts []string) (map[string][]int32, error) {
	tps, err := flagutil.ParseTopicPartitions(topicParts)
	if err != nil {
		return nil, fmt.Errorf("unable to parse topic partitions: %v", err)
	}

	var metaTopics []kmsg.MetadataRequestTopic
	for topic, partitions := range tps {
		if len(partitions) == 0 {
			t := topic
			metaTopics = append(metaTopics, kmsg.MetadataRequestTopic{Topic: &t})
		}
	}
	if len(metaTopics) > 0 || len(tps) == 0 {
		req := &kmsg.MetadataRequest{Topics: metaTopics}
		if len(tps) == 0 {
			req.Topics = nil
		}
		kresp, err := cl.Client().Request(context.Background(), req)
		if err != nil {
			return nil, fmt.Errorf("unable to get metadata: %v", err)
		}
		resp := kresp.(*kmsg.MetadataResponse)
		for _, topic := range resp.Topics {
			if topic.Topic == nil {
				return nil, fmt.Errorf("metadata returned nil topic when we did not fetch with topic IDs")
			}
			if req.Topics == nil && topic.IsInternal {
				continue
			}
			for _, partition := range topic.Partitions {
				tps[*topic.Topic] = append(tps[*topic.Topic], partition.Partition)
			}
		}
	}
	return tps, nil
}
