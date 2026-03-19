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
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

func apiVersionsRequest() *kmsg.ApiVersionsRequest {
	return &kmsg.ApiVersionsRequest{
		ClientSoftwareName:    "kcl",
		ClientSoftwareVersion: "v0.0.0",
	}
}

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "misc",
		Short: "Miscellaneous utilities (version probing, error code/text, offset listing)",
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

func errcodeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "errcode CODE",
		Short: "Print the name and description for an error code",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			code, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("unable to parse error code: %v", err)
			}
			if code == 0 {
				fmt.Println("NONE")
				return nil
			}
			kerr := kerr.ErrorForCode(int16(code)).(*kerr.Error)
			fmt.Printf("%s\n%s\n", kerr.Message, kerr.Description)
			return nil
		},
	}
}

func errtextCommand() *cobra.Command {
	var list, verbose bool
	cmd := &cobra.Command{
		Use:   "errtext [ERROR_NAME]",
		Short: "Print the name, code and description for an error name or all errors",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
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

			var err error
			for code := int16(1); err != kerr.UnknownServerError; code++ {
				err = kerr.ErrorForCode(code)
				kerr := err.(*kerr.Error)
				if list {
					fmt.Printf("%s (%d)\n%s\n\n", kerr.Message, kerr.Code, kerr.Description)
					continue
				}

				if verbose {
					fmt.Printf("trying %s...\n", kerr.Message)
				}
				if client.Strnorm(kerr.Message) == text {
					fmt.Printf("%s (%d)\n%s\n", kerr.Message, kerr.Code, kerr.Description)
					return nil
				}
			}
			if !list {
				return fmt.Errorf( "Unknown error text.")
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
				table := out.NewFormattedTable(cl.Format(), "misc.api-versions", 1, "api_versions",
					"NAME", "KEY", "MAX")
				v.EachMaxKeyVersion(func(k, ver int16) {
					kind := kmsg.NameForKey(k)
					if kind == "" {
						kind = "Unknown"
					}
					table.Row(kind, k, ver)
				})
				table.Flush()
			} else {
				table := out.NewFormattedTable(cl.Format(), "misc.api-versions", 1, "api_versions",
					"NAME", "MAX")
				v.EachMaxKeyVersion(func(k, ver int16) {
					kind := kmsg.NameForKey(k)
					if kind == "" {
						kind = "Unknown"
					}
					table.Row(kind, ver)
				})
				table.Flush()
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
		Short: "Probe and print the version of Kafka running (incompatible with --as-version)",
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
	cl.AddOpt(kgo.RequestRetries(0))
	kresp, err := cl.Client().Request(context.Background(), apiVersionsRequest())
	if err != nil { // pre 0.10.0 had no api versions
		cl.RemakeWithOpts(kgo.MaxVersions(kversion.V0_9_0()))
		// 0.9.0 has list groups
		if _, err = cl.Client().SeedBrokers()[0].Request(context.Background(), new(kmsg.ListGroupsRequest)); err == nil {
			fmt.Println("Kafka 0.9.0")
			return
		}
		cl.RemakeWithOpts(kgo.MaxVersions(kversion.V0_8_2()))
		// 0.8.2 has find coordinator
		if _, err = cl.Client().SeedBrokers()[0].Request(context.Background(), new(kmsg.FindCoordinatorRequest)); err == nil {
			fmt.Println("Kafka 0.8.2")
			return
		}
		cl.RemakeWithOpts(kgo.MaxVersions(kversion.V0_8_1()))
		// 0.8.1 has offset fetch
		if _, err = cl.Client().SeedBrokers()[0].Request(context.Background(), new(kmsg.OffsetFetchRequest)); err == nil {
			fmt.Println("Kafka 0.8.1")
			return
		}
		fmt.Println("Kafka 0.8.0")
		return
	}

	resp := kresp.(*kmsg.ApiVersionsResponse)

	v := kversion.FromApiVersionsResponse(resp)
	fmt.Println("Kafka " + v.VersionGuess())
}

func rawCommand(cl *client.Client) *cobra.Command {
	var key int16
	var b int
	cmd := &cobra.Command{
		Use:   "raw-req",
		Short: "Issue an arbitrary request parsed from JSON read from STDIN.",
		Args:  cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			req := kmsg.RequestForKey(key)
			if req == nil {
				return out.Errf(out.ExitUsage, "request key %d unknown", key)
			}
			req.SetVersion(-1)
			raw, err := io.ReadAll(os.Stdin)
			if err != nil {
				return fmt.Errorf("unable to read stdin: %v", err)
			}
			err = json.Unmarshal(raw, req)
			// If the raw json specified a Version field, it
			// overwrote our -1. We want to pin the version to
			// what the user specified.
			if req.GetVersion() != -1 {
				vers := kversion.Stable()
				vers.SetMaxKeyVersion(req.Key(), req.GetVersion())
				cl.AddOpt(kgo.MinVersions(vers))
				cl.AddOpt(kgo.MaxVersions(vers))
			}
			if err != nil {
				return fmt.Errorf("unable to unmarshal stdin: %v", err)
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
			out.ExitJSON(kresp)
			return nil
		},
	}
	cmd.Flags().Int16VarP(&key, "key", "k", -1, "request key")
	cmd.Flags().IntVarP(&b, "broker", "b", -1, "specific broker to issue this request to, if non-negative")
	return cmd
}

func listOffsetsCommand(cl *client.Client) *cobra.Command {
	var withEpochs bool
	var readCommitted bool

	cmd := &cobra.Command{
		Use:   "list-offsets",
		Short: "List start, stable, and end offsets for partitions.",
		Long: `List start, stable, and end offsets for topics or partitions (Kafka 0.10.0+).

The input format is topic:#,#,# or just topic. If a topic is given without
partitions, a metadata request is issued to figure out all partitions for the
topic and the output will include the offsets for all partitions.

Multiple topics can be listed, and multiple partitions per topic can be listed.

The STABLE column shows the last stable offset (the offset up to which consumers
with read_committed isolation can read). If STABLE differs from END, there is an
open transaction on that partition.

If --with-epochs is true, the start and end offsets will have /### following
the offset number, where ### corresponds to the broker epoch at that given
offset.
`,
		Example: "list-offsets foo:1,2,3 bar:0",
		RunE: func(_ *cobra.Command, topicParts []string) error {
			tps, err := loadTopicParts(cl, topicParts)
			if err != nil {
				return err
			}

			reqStart := &kmsg.ListOffsetsRequest{
				ReplicaID:      -1,
				IsolationLevel: 0,
			}
			reqEnd := &kmsg.ListOffsetsRequest{
				ReplicaID:      -1,
				IsolationLevel: 0,
			}
			reqStable := &kmsg.ListOffsetsRequest{
				ReplicaID:      -1,
				IsolationLevel: 1, // read_committed => last stable offset
			}
			if readCommitted {
				reqStart.IsolationLevel = 1
				reqEnd.IsolationLevel = 1
			}
			for topic, partitions := range tps {
				topicReqStart := kmsg.ListOffsetsRequestTopic{Topic: topic}
				topicReqEnd := kmsg.ListOffsetsRequestTopic{Topic: topic}
				topicReqStable := kmsg.ListOffsetsRequestTopic{Topic: topic}
				for _, partition := range partitions {
					topicReqStart.Partitions = append(topicReqStart.Partitions, kmsg.ListOffsetsRequestTopicPartition{
						Partition:          partition,
						CurrentLeaderEpoch: -1,
						Timestamp:          -2, // earliest
						MaxNumOffsets:      1,  // just in case <= 0.10.0
					})
					topicReqEnd.Partitions = append(topicReqEnd.Partitions, kmsg.ListOffsetsRequestTopicPartition{
						Partition:          partition,
						CurrentLeaderEpoch: -1,
						Timestamp:          -1, // latest
						MaxNumOffsets:      1,
					})
					topicReqStable.Partitions = append(topicReqStable.Partitions, kmsg.ListOffsetsRequestTopicPartition{
						Partition:          partition,
						CurrentLeaderEpoch: -1,
						Timestamp:          -1, // latest with read_committed = last stable
						MaxNumOffsets:      1,
					})
				}
				reqStart.Topics = append(reqStart.Topics, topicReqStart)
				reqEnd.Topics = append(reqEnd.Topics, topicReqEnd)
				reqStable.Topics = append(reqStable.Topics, topicReqStable)
			}

			var startResps, endResps, stableResps []kgo.ResponseShard
			var wg sync.WaitGroup
			wg.Add(3)
			go func() {
				defer wg.Done()
				startResps = cl.Client().RequestSharded(context.Background(), reqStart)
			}()
			go func() {
				defer wg.Done()
				endResps = cl.Client().RequestSharded(context.Background(), reqEnd)
			}()
			go func() {
				defer wg.Done()
				stableResps = cl.Client().RequestSharded(context.Background(), reqStable)
			}()
			wg.Wait()

			type startEnd struct {
				err              error
				broker           int32
				startOffset      int64
				startLeaderEpoch int32
				stableOffset     int64
				endOffset        int64
				endLeaderEpoch   int32
			}

			startEnds := make(map[string]map[int32]startEnd)

			for _, brokerResp := range startResps {
				if brokerResp.Err != nil {
					fmt.Printf("unable to list start offsets from broker %d (%s:%d): %v\n", brokerResp.Meta.NodeID, brokerResp.Meta.Host, brokerResp.Meta.Port, brokerResp.Err)
					continue
				}
				startResp := brokerResp.Resp.(*kmsg.ListOffsetsResponse)
				for _, topic := range startResp.Topics {
					topicStartEnds := startEnds[topic.Topic]
					if topicStartEnds == nil {
						topicStartEnds = make(map[int32]startEnd)
						startEnds[topic.Topic] = topicStartEnds
					}
					for _, partition := range topic.Partitions {
						if startResp.Version == 0 && len(partition.OldStyleOffsets) > 0 {
							partition.Offset = partition.OldStyleOffsets[0]
						}
						topicStartEnds[partition.Partition] = startEnd{
							err:              kerr.ErrorForCode(partition.ErrorCode),
							broker:           brokerResp.Meta.NodeID,
							startOffset:      partition.Offset,
							startLeaderEpoch: partition.LeaderEpoch,
						}
					}
				}
			}

			for _, brokerResp := range endResps {
				if brokerResp.Err != nil {
					fmt.Printf("unable to list end offsets from broker %d (%s:%d): %v\n", brokerResp.Meta.NodeID, brokerResp.Meta.Host, brokerResp.Meta.Port, brokerResp.Err)
					continue
				}
				endResp := brokerResp.Resp.(*kmsg.ListOffsetsResponse)
				for _, topic := range endResp.Topics {
					topicStartEnds := startEnds[topic.Topic]
					var startErr bool
					if topicStartEnds == nil {
						topicStartEnds = make(map[int32]startEnd)
						startEnds[topic.Topic] = topicStartEnds
						startErr = true
					}
					for _, partition := range topic.Partitions {
						partStartEnd, ok := topicStartEnds[partition.Partition]
						if !ok {
							startErr = true
						}
						if endResp.Version == 0 && len(partition.OldStyleOffsets) > 0 {
							partition.Offset = partition.OldStyleOffsets[0]
						}
						partStartEnd.endOffset = partition.Offset
						partStartEnd.endLeaderEpoch = partition.LeaderEpoch
						partStartEnd.broker = brokerResp.Meta.NodeID

						if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
							partStartEnd.err = err
						} else if startErr {
							partStartEnd.err = kerr.UnknownServerError
						}

						topicStartEnds[partition.Partition] = partStartEnd
					}
				}
			}

			for _, brokerResp := range stableResps {
				if brokerResp.Err != nil {
					fmt.Printf("unable to list stable offsets from broker %d (%s:%d): %v\n", brokerResp.Meta.NodeID, brokerResp.Meta.Host, brokerResp.Meta.Port, brokerResp.Err)
					continue
				}
				stableResp := brokerResp.Resp.(*kmsg.ListOffsetsResponse)
				for _, topic := range stableResp.Topics {
					topicStartEnds := startEnds[topic.Topic]
					if topicStartEnds == nil {
						continue
					}
					for _, partition := range topic.Partitions {
						partStartEnd, ok := topicStartEnds[partition.Partition]
						if !ok {
							continue
						}
						partStartEnd.stableOffset = partition.Offset
						topicStartEnds[partition.Partition] = partStartEnd
					}
				}
			}

			type partStartEnd struct {
				part int32
				startEnd
			}
			type sortedTopic struct {
				topic string
				parts []partStartEnd
			}
			var sorted []sortedTopic
			for topic, partitions := range startEnds {
				st := sortedTopic{topic: topic}
				for part, startEnd := range partitions {
					st.parts = append(st.parts, partStartEnd{part: part, startEnd: startEnd})
				}
				sort.Slice(st.parts, func(i, j int) bool { return st.parts[i].part < st.parts[j].part })
				sorted = append(sorted, st)
			}
			sort.Slice(sorted, func(i, j int) bool { return sorted[i].topic < sorted[j].topic })

			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "BROKER\tTOPIC\tPARTITION\tSTART\tSTABLE\tEND\tERROR\n")

			for _, topic := range sorted {
				for _, part := range topic.parts {
					if part.err != nil {
						fmt.Fprintf(tw, "%d\t%s\t%d\t\t\t\t%v\n", part.broker, topic.topic, part.part, part.err)
						continue
					}
					if withEpochs {
						fmt.Fprintf(tw, "%d\t%s\t%d\t%d/%d\t%d\t%d/%d\t\n",
							part.broker,
							topic.topic,
							part.part,
							part.startOffset,
							part.startLeaderEpoch,
							part.stableOffset,
							part.endOffset,
							part.endLeaderEpoch,
						)
					} else {
						fmt.Fprintf(tw, "%d\t%s\t%d\t%d\t%d\t%d\t\n",
							part.broker,
							topic.topic,
							part.part,
							part.startOffset,
							part.stableOffset,
							part.endOffset,
						)
					}
				}
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&readCommitted, "committed", false, "whether to list only committed offsets as opposed to latest (Kafka 0.11.0+)")
	cmd.Flags().BoolVar(&withEpochs, "with-epochs", false, "whether to include the epoch for the start and end offsets (Kafka 2.1.0+)")

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

		Example: "offset-for-leader-epoch foo bar biz:0,1,2",
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
			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "BROKER\tTOPIC\tPARTITION\tLEADER-EPOCH\tEND-OFFSET\tERROR\n")

			for _, shard := range shards {
				if shard.Err != nil {
					fmt.Printf("unable to issue request to broker %d (%s:%d): %v\n", shard.Meta.NodeID, shard.Meta.Host, shard.Meta.Port, shard.Err)
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
						fmt.Fprintf(tw, "%d\t%s\t%d\t%d\t%d\t%s\n",
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
			return nil
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
