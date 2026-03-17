package sharegroup

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

func seekCommand(cl *client.Client) *cobra.Command {
	var (
		to      string
		topics  []string
		dryRun  bool
		execute bool
	)

	cmd := &cobra.Command{
		Use:   "seek GROUP",
		Short: "Reset share group start offsets.",
		Long: `Reset share group start offsets (KIP-932, Kafka 4.0+).

Seek adjusts the start offsets for a share group. The group must be
empty (no active consumers).

The --to flag accepts offset specifications:
  start              earliest offset
  end                latest offset
  N                  exact offset N
  @TIMESTAMP         seek to a timestamp (unix ms/s/ns, date, RFC3339, -duration)

Note: +N/-N (relative to committed) are not supported for share groups
because share groups have start offsets, not committed offsets.

EXAMPLES:
  kcl share-group seek mygroup --to start
  kcl share-group seek mygroup --to end --topics foo,bar
  kcl share-group seek mygroup --to @-1h
  kcl share-group seek mygroup --to 100 --dry-run

SEE ALSO:
  kcl share-group describe-offsets    describe share group start offsets
  kcl share-group alter-offsets       alter offsets with explicit values
`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			groupName := args[0]

			if to == "" {
				out.Die("--to is required")
			}

			spec, err := offsetparse.Parse(to, time.Now())
			out.MaybeDie(err, "unable to parse --to %q: %v", to, err)
			if spec.End != nil {
				out.Die("--to does not accept range offsets; use a single target value")
			}
			if spec.Start.Kind == offsetparse.KindRelative {
				out.Die("+N/-N relative offsets are not supported for share groups (use start, end, N, or @timestamp)")
			}

			kclClient := cl.Client()
			adm := kadm.NewClient(kclClient)
			ctx := context.Background()

			// Determine target topics.
			var targetTopics []string
			if len(topics) > 0 {
				targetTopics = topics
			} else {
				out.Die("--topics is required for share-group seek")
			}

			// Resolve target offsets.
			type targetOffset struct {
				topic     string
				partition int32
				offset    int64
			}
			var targets []targetOffset

			switch spec.Start.Kind {
			case offsetparse.KindStart:
				listed, err := adm.ListStartOffsets(ctx, targetTopics...)
				out.MaybeDie(err, "unable to list start offsets: %v", err)
				listed.Each(func(lo kadm.ListedOffset) {
					if lo.Err == nil {
						targets = append(targets, targetOffset{lo.Topic, lo.Partition, lo.Offset})
					}
				})

			case offsetparse.KindEnd:
				listed, err := adm.ListEndOffsets(ctx, targetTopics...)
				out.MaybeDie(err, "unable to list end offsets: %v", err)
				listed.Each(func(lo kadm.ListedOffset) {
					if lo.Err == nil {
						targets = append(targets, targetOffset{lo.Topic, lo.Partition, lo.Offset})
					}
				})

			case offsetparse.KindExact:
				listed, err := adm.ListEndOffsets(ctx, targetTopics...)
				out.MaybeDie(err, "unable to list offsets: %v", err)
				listed.Each(func(lo kadm.ListedOffset) {
					if lo.Err == nil {
						targets = append(targets, targetOffset{lo.Topic, lo.Partition, spec.Start.Value})
					}
				})

			case offsetparse.KindTimestamp:
				listed, err := adm.ListOffsetsAfterMilli(ctx, spec.Start.Value, targetTopics...)
				out.MaybeDie(err, "unable to resolve timestamp to offsets: %v", err)
				listed.Each(func(lo kadm.ListedOffset) {
					if lo.Err == nil {
						targets = append(targets, targetOffset{lo.Topic, lo.Partition, lo.Offset})
					}
				})

			default:
				out.Die("unsupported seek target kind: %v", spec.Start.Kind)
			}

			if len(targets) == 0 {
				fmt.Println("No offsets to change.")
				return
			}

			sort.Slice(targets, func(i, j int) bool {
				if targets[i].topic != targets[j].topic {
					return targets[i].topic < targets[j].topic
				}
				return targets[i].partition < targets[j].partition
			})

			// Print preview table.
			fmt.Printf("GROUP: %s\n\n", groupName)
			tw := out.NewTable("TOPIC", "PARTITION", "NEW-START-OFFSET")
			for _, t := range targets {
				tw.Print(t.topic, t.partition, t.offset)
			}
			tw.Flush()

			// Approval phase.
			if dryRun {
				return
			}
			if !execute {
				fmt.Print("\nApply these offset changes? [y/N] ")
				scanner := bufio.NewScanner(os.Stdin)
				scanner.Scan()
				answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
				if answer != "y" && answer != "yes" {
					fmt.Println("Aborted.")
					return
				}
			}

			// Commit via AlterShareGroupOffsets.
			fmt.Println()
			req := kmsg.NewPtrAlterShareGroupOffsetsRequest()
			req.GroupID = groupName

			topicMap := make(map[string]*kmsg.AlterShareGroupOffsetsRequestTopic)
			for _, t := range targets {
				rt, ok := topicMap[t.topic]
				if !ok {
					topic := kmsg.NewAlterShareGroupOffsetsRequestTopic()
					topic.Topic = t.topic
					rt = &topic
					topicMap[t.topic] = rt
				}
				rp := kmsg.NewAlterShareGroupOffsetsRequestTopicPartition()
				rp.Partition = t.partition
				rp.StartOffset = t.offset
				rt.Partitions = append(rt.Partitions, rp)
			}
			for _, rt := range topicMap {
				req.Topics = append(req.Topics, *rt)
			}

			kresp, err := req.RequestWith(ctx, kclClient)
			out.MaybeDie(err, "unable to alter share group offsets: %v", err)

			if err := kerr.ErrorForCode(kresp.ErrorCode); err != nil {
				msg := err.Error()
				if kresp.ErrorMessage != nil {
					msg += ": " + *kresp.ErrorMessage
				}
				out.Die(msg)
			}

			// Print results.
			resultTw := out.NewTable("TOPIC", "PARTITION", "ERROR")
			for _, topic := range kresp.Topics {
				for _, partition := range topic.Partitions {
					errMsg := "OK"
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						errMsg = err.Error()
						if partition.ErrorMessage != nil {
							errMsg += ": " + *partition.ErrorMessage
						}
					}
					resultTw.Print(topic.Topic, partition.Partition, errMsg)
				}
			}
			resultTw.Flush()
		},
	}

	cmd.Flags().StringVar(&to, "to", "", "target offset (start, end, N, @timestamp)")
	cmd.Flags().StringSliceVar(&topics, "topics", nil, "topics to seek (required; comma-separated, repeatable)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "preview offset changes without applying")
	cmd.Flags().BoolVar(&execute, "execute", false, "apply changes without interactive confirmation")
	cmd.MarkFlagRequired("to")
	cmd.MarkFlagRequired("topics")

	return cmd
}
