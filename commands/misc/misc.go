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

// errorHeaders are the columns of an error lookup. CODE is error_code in
// JSON, since code is what an error document uses for the exit code.
var errorHeaders = []string{"NAME", "CODE", "DESCRIPTION"}

// printKerr prints one Kafka error in the requested format. Text is NAME
// (CODE) and then the description on its own line.
func printKerr(format, command string, code int16, name, description string) {
	switch format {
	case out.FormatJSON:
		out.MarshalJSON(command, 1, map[string]any{"error_code": code, "name": name, "description": description})
	case out.FormatAWK:
		out.AwkRow(name, code, description)
	default:
		fmt.Printf("%s (%d)\n%s\n", name, code, description)
	}
}

// lookupCode is the Kafka error for code, or false when no error has it.
// kerr answers UNKNOWN_SERVER_ERROR for a code it does not know, so the
// answer's code is checked against the one asked for.
func lookupCode(code int16) (*kerr.Error, bool) {
	if code == 0 {
		return &kerr.Error{Message: "NONE", Code: 0, Description: "No error."}, true
	}
	e := kerr.TypedErrorForCode(code)
	return e, e.Code == code
}

// allErrors is every Kafka error kerr knows, by code, ascending, from
// UNKNOWN_SERVER_ERROR at -1. A code that answers UNKNOWN_SERVER_ERROR
// without being -1 is not one of them.
func allErrors() []*kerr.Error {
	errs := []*kerr.Error{kerr.UnknownServerError}
	for code := int16(1); code < 1000; code++ {
		if e, ok := lookupCode(code); ok {
			errs = append(errs, e)
		}
	}
	return errs
}

func errcodeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "errcode CODE",
		Short: "Print the name and description for an error code.",
		Long: `Print the name and description for an error code.

Text prints NAME (CODE) and then the description. JSON is one document
{name, error_code, description}; awk is one row NAME CODE DESCRIPTION. A
code no Kafka error has is an error, exit 1.

EXAMPLES:
  kcl misc errcode 3         # UNKNOWN_TOPIC_OR_PARTITION
  kcl misc errcode 0         # NONE

SEE ALSO:
  kcl misc errtext    look up an error by name, or list them all
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			code, err := strconv.ParseInt(args[0], 10, 16)
			if err != nil {
				return out.Errf(out.ExitUsage, "unable to parse error code %q: %v", args[0], err)
			}
			format, _ := cmd.Flags().GetString("format")
			e, ok := lookupCode(int16(code))
			if !ok {
				return out.Errf(out.ExitError, "no Kafka error has code %d", code)
			}
			printKerr(format, out.CommandName(cmd.CommandPath()), e.Code, e.Message, e.Description)
			return nil
		},
	}
	out.Columns(cmd, errorHeaders...)
	return cmd
}

func errtextCommand() *cobra.Command {
	var list, verbose bool
	cmd := &cobra.Command{
		Use:   "errtext [ERROR_NAME]",
		Short: "Print the name, code and description for an error name or all errors.",
		Long: `Print the name, code and description for an error name or all errors.

The name is matched ignoring case, underscores, and dashes. Text prints
NAME (CODE) and then the description. JSON is one document {name,
error_code, description}; awk is one row NAME CODE DESCRIPTION. A name no
Kafka error has is an error, exit 1.

--list prints every error instead, one row each, by code.

EXAMPLES:
  kcl misc errtext UNKNOWN_TOPIC_OR_PARTITION
  kcl misc errtext not-leader-for-partition
  kcl misc errtext --list --format awk

SEE ALSO:
  kcl misc errcode    look up an error by code
`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, _ := cmd.Flags().GetString("format")
			command := out.CommandName(cmd.CommandPath())
			if list {
				if len(args) != 0 {
					return out.Errf(out.ExitUsage, "invalid extra args while list is set")
				}
				table := out.NewFormattedTable(format, command, 1, "errors", errorHeaders...).
					WithKeys(map[string]string{"CODE": "error_code"})
				for _, e := range allErrors() {
					table.Row(e.Message, e.Code, e.Description)
				}
				return table.Flush()
			}
			if len(args) != 1 {
				return out.Errf(out.ExitUsage, "missing error name to look up")
			}
			text := client.Strnorm(args[0])
			for _, e := range allErrors() {
				if verbose {
					fmt.Fprintf(os.Stderr, "trying %s...\n", e.Message)
				}
				if client.Strnorm(e.Message) == text {
					printKerr(format, command, e.Code, e.Message, e.Description)
					return nil
				}
			}
			return out.Errf(out.ExitError, "no Kafka error is named %q", args[0])
		},
	}
	out.Columns(cmd, errorHeaders...)
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

var apiVersionsHeaders = []string{"NAME", "KEY", "MAX"}

func apiVersionsCommand(cl *client.Client) *cobra.Command {
	var keys bool
	var version string

	cmd := &cobra.Command{
		Use:   "api-versions",
		Short: "Print broker API versions for each Kafka request type (Kafka 0.10.0+).",
		Long: `Print broker API versions for each Kafka request type (Kafka 0.10.0+).

Each row is a request the broker supports and the maximum version it speaks.
A request kcl does not know prints as Unknown; --with-key-nums shows the
request key in text so you can tell which one it is. json and awk always
carry KEY.

EXAMPLES:
  kcl misc api-versions
  kcl misc api-versions --with-key-nums
  kcl misc api-versions -v 3.5.0       # what a Kafka 3.5.0 broker speaks, offline

SEE ALSO:
  kcl misc probe-version    guess the broker's Kafka version
`,
		Args: cobra.ExactArgs(0),
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

			headers := apiVersionsHeaders
			text := cl.Format() == out.FormatText
			if text && !keys {
				headers = []string{"NAME", "MAX"}
			}
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "api_versions", headers...)
			v.EachMaxKeyVersion(func(k, ver int16) {
				kind := kmsg.NameForKey(k)
				if kind == "" {
					kind = "Unknown"
				}
				if text && !keys {
					table.Row(kind, ver)
					return
				}
				table.Row(kind, k, ver)
			})
			return table.Flush()
		},
	}
	out.Columns(cmd, apiVersionsHeaders...)

	cmd.Flags().StringVarP(&version, "version", "v", "", "if non-empty, print the api versions for a specific version rather than the broker's version")
	cmd.Flags().BoolVar(&keys, "with-key-nums", false, "include key numbers in the output; useful if the output contains Unknown")

	return cmd
}

var probeHeaders = []string{"MIN", "MAX"}

func probeVersionCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "probe-version",
		Short: "Probe and print the version of Kafka running (incompatible with --as-version).",
		Long: `Probe and print the version of Kafka running (incompatible with --as-version).

The guess comes from the broker's ApiVersions response, or from which
requests a broker too old for ApiVersions answers. Text prints the guess as
a sentence; JSON is {guess, min, max} and awk is one row MIN MAX, the ends
of the range the guess names, empty where the guess leaves an end open.

EXAMPLES:
  kcl misc probe-version

SEE ALSO:
  kcl misc api-versions    the request versions the broker speaks
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			return probeVersion(cl)
		},
	}
	out.Columns(cmd, probeHeaders...)
	return cmd
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
		out.AwkRow(min, max)
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

var epochHeaders = []string{"BROKER", "TOPIC", "PARTITION", "LEADER-EPOCH", "END-OFFSET", "ERROR"}

func offsetForLeaderEpochCommand(cl *client.Client) *cobra.Command {
	var currentLeaderEpoch int32
	var leaderEpoch int32

	cmd := &cobra.Command{
		Use:   "offset-for-leader-epoch TOPIC:P...",
		Short: "See the offsets for a leader epoch.",
		Long: `See the offsets for a leader epoch.

This is an advanced command strictly for debugging purposes. To discover what
it does, read the documentation for kmsg.OffsetForLeaderEpochRequest.

A topic given without partitions is every partition of the topic. Rows are
sorted by broker, topic, and partition.

EXAMPLES:
  kcl misc offset-for-leader-epoch foo bar biz:0,1,2
  kcl misc offset-for-leader-epoch foo:0 -e 3       # the end offset of epoch 3
`,
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
			sort.Slice(shards, func(i, j int) bool { return shards[i].Meta.NodeID < shards[j].Meta.NodeID })
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "epochs", epochHeaders...).ErrorColumn()

			// A broker that could not be asked is reported on stderr,
			// since its partitions are unknown, and the command exits 1.
			var failed bool
			for _, shard := range shards {
				if shard.Err != nil {
					fmt.Fprintf(os.Stderr, "unable to issue request to broker %d (%s:%d): %v\n", shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, shard.Err)
					failed = true
					continue
				}

				resp := shard.Resp.(*kmsg.OffsetForLeaderEpochResponse)

				sort.Slice(resp.Topics, func(i, j int) bool { return resp.Topics[i].Topic < resp.Topics[j].Topic })
				for _, topic := range resp.Topics {
					sort.Slice(topic.Partitions, func(i, j int) bool { return topic.Partitions[i].Partition < topic.Partitions[j].Partition })
					for _, partition := range topic.Partitions {
						table.Row(
							shard.Meta.NodeID,
							topic.Topic,
							partition.Partition,
							partition.LeaderEpoch,
							partition.EndOffset,
							out.ErrName(partition.ErrorCode),
						)
					}
				}
			}
			if err := table.Flush(); err != nil || failed {
				return out.ErrSilent
			}
			return nil
		},
	}

	out.Columns(cmd, epochHeaders...)
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
