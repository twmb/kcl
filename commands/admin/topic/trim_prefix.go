package topic

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/offsetparse"
	"github.com/twmb/kcl/out"
)

func topicTrimPrefixCommand(cl *client.Client) *cobra.Command {
	var (
		offsetFlag string
		partitions []int32
		noConfirm  bool
		fromFile   string
	)

	cmd := &cobra.Command{
		Use:   "trim-prefix TOPIC",
		Short: "Delete records before a given offset or timestamp.",
		Long: `Delete records before a given offset or timestamp (Kafka 0.11.0+).

This is a user-friendly wrapper around DeleteRecords. It resolves symbolic
offsets (timestamps, 'end', relative) via ListOffsets before issuing the
delete. Records before the resolved offset become inaccessible.

The --offset flag accepts the same syntax as consume --offset:
  N             delete records before exact offset N
  end           delete all records (trim to high watermark)
  @TIMESTAMP    delete records before the timestamp

EXAMPLES:
  kcl topic trim-prefix foo --offset 1000
  kcl topic trim-prefix foo --offset end
  kcl topic trim-prefix foo --offset @-7d
  kcl topic trim-prefix foo --offset @2024-01-15 --partitions 0,1,2

SEE ALSO:
  kcl topic describe     describe topic partitions
  kcl topic list         list topics
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			topicName := args[0]

			kclClient := cl.Client()
			adm := kadm.NewClient(kclClient)
			ctx := context.Background()

			type trimTarget struct {
				partition int32
				offset    int64
			}
			var targets []trimTarget

			if fromFile != "" {
				if offsetFlag != "" {
					return fmt.Errorf("--offset and --from-file are mutually exclusive")
				}
				type fileEntry struct {
					Topic     string `json:"topic"`
					Partition int32  `json:"partition"`
					Offset    int64  `json:"offset"`
				}
				raw, err := os.ReadFile(fromFile)
				if err != nil {
					return fmt.Errorf("unable to read --from-file: %v", err)
				}
				var entries []fileEntry
				if err := json.Unmarshal(raw, &entries); err != nil {
					return fmt.Errorf("unable to parse --from-file: %v", err)
				}
				for _, e := range entries {
					if e.Topic != topicName {
						continue
					}
					targets = append(targets, trimTarget{e.Partition, e.Offset})
				}
			} else {
				if offsetFlag == "" {
					return fmt.Errorf("one of --offset or --from-file is required")
				}
				spec, err := offsetparse.Parse(offsetFlag, time.Now())
				if err != nil {
					return fmt.Errorf("unable to parse --offset %q: %v", offsetFlag, err)
				}
				if spec.End != nil {
					return fmt.Errorf("--offset does not accept range syntax for trim-prefix")
				}

				switch spec.Start.Kind {
				case offsetparse.KindExact:
					listed, err := adm.ListEndOffsets(ctx, topicName)
					if err != nil {
						return fmt.Errorf("unable to list offsets: %v", err)
					}
					listed.Each(func(lo kadm.ListedOffset) {
						if lo.Err == nil && filterPartition(lo.Partition, partitions) {
							targets = append(targets, trimTarget{lo.Partition, spec.Start.Value})
						}
					})
				case offsetparse.KindEnd:
					listed, err := adm.ListEndOffsets(ctx, topicName)
					if err != nil {
						return fmt.Errorf("unable to list end offsets: %v", err)
					}
					listed.Each(func(lo kadm.ListedOffset) {
						if lo.Err == nil && filterPartition(lo.Partition, partitions) {
							targets = append(targets, trimTarget{lo.Partition, lo.Offset})
						}
					})
				case offsetparse.KindTimestamp:
					listed, err := adm.ListOffsetsAfterMilli(ctx, spec.Start.Value, topicName)
					if err != nil {
						return fmt.Errorf("unable to resolve timestamp: %v", err)
					}
					listed.Each(func(lo kadm.ListedOffset) {
						if lo.Err == nil && filterPartition(lo.Partition, partitions) {
							targets = append(targets, trimTarget{lo.Partition, lo.Offset})
						}
					})
				case offsetparse.KindStart:
					fmt.Println("Nothing to trim: offset is already at start.")
					return nil
				default:
					return fmt.Errorf("unsupported offset kind for trim-prefix: %v", spec.Start.Kind)
				}
			}

			if len(targets) == 0 {
				fmt.Println("No partitions to trim.")
				return nil
			}

			sort.Slice(targets, func(i, j int) bool {
				return targets[i].partition < targets[j].partition
			})

			// Preview.
			tw := out.NewTable("PARTITION", "DELETE-BEFORE-OFFSET")
			for _, t := range targets {
				tw.Print(t.partition, t.offset)
			}
			tw.Flush()

			if !noConfirm {
				fmt.Print("\nDelete records before these offsets? [y/N] ")
				scanner := bufio.NewScanner(os.Stdin)
				scanner.Scan()
				answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
				if answer != "y" && answer != "yes" {
					fmt.Println("Aborted.")
					return nil
				}
			}

			// Issue DeleteRecords.
			req := &kmsg.DeleteRecordsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			rt := kmsg.NewDeleteRecordsRequestTopic()
			rt.Topic = topicName
			for _, t := range targets {
				rp := kmsg.NewDeleteRecordsRequestTopicPartition()
				rp.Partition = t.partition
				rp.Offset = t.offset
				rt.Partitions = append(rt.Partitions, rp)
			}
			req.Topics = append(req.Topics, rt)

			shards := kclClient.RequestSharded(ctx, req)
			fmt.Println()
			resultTw := out.NewTable("PARTITION", "NEW-LOW-WATERMARK", "ERROR")
			for _, shard := range shards {
				if shard.Err != nil {
					fmt.Printf("error from broker %d: %v\n", shard.Meta.NodeID, shard.Err)
					continue
				}
				resp := shard.Resp.(*kmsg.DeleteRecordsResponse)
				for _, topic := range resp.Topics {
					for _, p := range topic.Partitions {
						errMsg := ""
						if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
							errMsg = err.Error()
						}
						resultTw.Print(p.Partition, p.LowWatermark, errMsg)
					}
				}
			}
			resultTw.Flush()
			return nil
		},
	}

	cmd.Flags().StringVar(&offsetFlag, "offset", "", "offset or timestamp to trim before (N, end, @timestamp)")
	cmd.Flags().Int32SliceVar(&partitions, "partitions", nil, "limit to specific partitions (default: all)")
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "skip confirmation prompt")
	cmd.Flags().StringVar(&fromFile, "from-file", "", "JSON file of [{topic, partition, offset}, ...] to trim")

	return cmd
}

func filterPartition(p int32, allowed []int32) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, a := range allowed {
		if a == p {
			return true
		}
	}
	return false
}
