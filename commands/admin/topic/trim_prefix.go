package topic

import (
	"bufio"
	"context"
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
		Run: func(_ *cobra.Command, args []string) {
			topicName := args[0]

			spec, err := offsetparse.Parse(offsetFlag, time.Now())
			out.MaybeDie(err, "unable to parse --offset %q: %v", offsetFlag, err)
			if spec.End != nil {
				out.Die("--offset does not accept range syntax for trim-prefix")
			}

			kclClient := cl.Client()
			adm := kadm.NewClient(kclClient)
			ctx := context.Background()

			// Resolve the target offsets per partition.
			type trimTarget struct {
				partition int32
				offset    int64
			}
			var targets []trimTarget

			switch spec.Start.Kind {
			case offsetparse.KindExact:
				// Need partition discovery.
				listed, err := adm.ListEndOffsets(ctx, topicName)
				out.MaybeDie(err, "unable to list offsets: %v", err)
				listed.Each(func(lo kadm.ListedOffset) {
					if lo.Err == nil && filterPartition(lo.Partition, partitions) {
						targets = append(targets, trimTarget{lo.Partition, spec.Start.Value})
					}
				})

			case offsetparse.KindEnd:
				listed, err := adm.ListEndOffsets(ctx, topicName)
				out.MaybeDie(err, "unable to list end offsets: %v", err)
				listed.Each(func(lo kadm.ListedOffset) {
					if lo.Err == nil && filterPartition(lo.Partition, partitions) {
						targets = append(targets, trimTarget{lo.Partition, lo.Offset})
					}
				})

			case offsetparse.KindTimestamp:
				listed, err := adm.ListOffsetsAfterMilli(ctx, spec.Start.Value, topicName)
				out.MaybeDie(err, "unable to resolve timestamp: %v", err)
				listed.Each(func(lo kadm.ListedOffset) {
					if lo.Err == nil && filterPartition(lo.Partition, partitions) {
						targets = append(targets, trimTarget{lo.Partition, lo.Offset})
					}
				})

			case offsetparse.KindStart:
				// Trim to start = no-op.
				fmt.Println("Nothing to trim: offset is already at start.")
				return

			default:
				out.Die("unsupported offset kind for trim-prefix: %v", spec.Start.Kind)
			}

			if len(targets) == 0 {
				fmt.Println("No partitions to trim.")
				return
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
					return
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
		},
	}

	cmd.Flags().StringVar(&offsetFlag, "offset", "", "offset or timestamp to trim before (N, end, @timestamp)")
	cmd.Flags().Int32SliceVar(&partitions, "partitions", nil, "limit to specific partitions (default: all)")
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "skip confirmation prompt")
	cmd.MarkFlagRequired("offset")

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
