package sharegroup

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

func seekCommand(cl *client.Client) *cobra.Command {
	var (
		to      string
		toFile  string
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

Exactly one of --to or --to-file must be specified.

The --to flag accepts offset specifications:
  start              earliest offset
  end                latest offset
  N                  exact offset N
  @TIMESTAMP         seek to a timestamp (unix ms/s/ns, date, RFC3339, -duration)

Note: +N/-N (relative to committed) are not supported for share groups
because share groups have start offsets, not committed offsets.

The --to-file flag reads target offsets from a JSON file with format:
  [{"topic": "foo", "partition": 0, "offset": 100}, ...]

EXAMPLES:
  kcl share-group seek mygroup --to start --topics foo,bar
  kcl share-group seek mygroup --to end --topics foo,bar
  kcl share-group seek mygroup --to @-1h --topics foo,bar
  kcl share-group seek mygroup --to 100 --topics foo --dry-run
  kcl share-group seek mygroup --to-file offsets.json
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			groupName := args[0]

			hasTo := to != ""
			hasToFile := toFile != ""
			if !hasTo && !hasToFile {
				return fmt.Errorf("one of --to or --to-file is required")
			}
			if hasTo && hasToFile {
				return fmt.Errorf("--to and --to-file are mutually exclusive")
			}

			kclClient := cl.Client()
			adm := kadm.NewClient(kclClient)
			ctx := context.Background()

			type targetOffset struct {
				topic     string
				partition int32
				offset    int64
			}
			var targets []targetOffset

			if hasToFile {
				type fileEntry struct {
					Topic     string `json:"topic"`
					Partition int32  `json:"partition"`
					Offset    int64  `json:"offset"`
				}
				data, err := os.ReadFile(toFile)
				if err != nil {
					return fmt.Errorf("unable to read --to-file %q: %v", toFile, err)
				}
				var entries []fileEntry
				err = json.Unmarshal(data, &entries)
				if err != nil {
					return fmt.Errorf("unable to parse --to-file %q: %v", toFile, err)
				}
				for _, e := range entries {
					if len(topics) > 0 {
						found := false
						for _, t := range topics {
							if t == e.Topic {
								found = true
								break
							}
						}
						if !found {
							continue
						}
					}
					targets = append(targets, targetOffset{e.Topic, e.Partition, e.Offset})
				}
			} else {
				spec, err := offsetparse.Parse(to, time.Now())
				if err != nil {
					return fmt.Errorf("unable to parse --to %q: %v", to, err)
				}
				if spec.End != nil {
					return fmt.Errorf("--to does not accept range offsets; use a single target value")
				}
				if spec.Start.Kind == offsetparse.KindRelative {
					return fmt.Errorf("+N/-N relative offsets are not supported for share groups (use start, end, N, or @timestamp)")
				}

				if len(topics) == 0 {
					return fmt.Errorf("--topics is required when using --to")
				}

				switch spec.Start.Kind {
				case offsetparse.KindStart:
					listed, err := adm.ListStartOffsets(ctx, topics...)
					if err != nil {
						return fmt.Errorf("unable to list start offsets: %v", err)
					}
					listed.Each(func(lo kadm.ListedOffset) {
						if lo.Err == nil {
							targets = append(targets, targetOffset{lo.Topic, lo.Partition, lo.Offset})
						}
					})

				case offsetparse.KindEnd:
					listed, err := adm.ListEndOffsets(ctx, topics...)
					if err != nil {
						return fmt.Errorf("unable to list end offsets: %v", err)
					}
					listed.Each(func(lo kadm.ListedOffset) {
						if lo.Err == nil {
							targets = append(targets, targetOffset{lo.Topic, lo.Partition, lo.Offset})
						}
					})

				case offsetparse.KindExact:
					listed, err := adm.ListEndOffsets(ctx, topics...)
					if err != nil {
						return fmt.Errorf("unable to list offsets: %v", err)
					}
					listed.Each(func(lo kadm.ListedOffset) {
						if lo.Err == nil {
							targets = append(targets, targetOffset{lo.Topic, lo.Partition, spec.Start.Value})
						}
					})

				case offsetparse.KindTimestamp:
					listed, err := adm.ListOffsetsAfterMilli(ctx, spec.Start.Value, topics...)
					if err != nil {
						return fmt.Errorf("unable to resolve timestamp to offsets: %v", err)
					}
					listed.Each(func(lo kadm.ListedOffset) {
						if lo.Err == nil {
							targets = append(targets, targetOffset{lo.Topic, lo.Partition, lo.Offset})
						}
					})

				default:
					return fmt.Errorf("unsupported seek target kind: %v", spec.Start.Kind)
				}
			}

			if len(targets) == 0 {
				fmt.Println("No offsets to change.")
				return nil
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
				return nil
			}
			if !execute {
				fmt.Print("\nApply these offset changes? [y/N] ")
				scanner := bufio.NewScanner(os.Stdin)
				scanner.Scan()
				answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
				if answer != "y" && answer != "yes" {
					fmt.Println("Aborted.")
					return nil
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
			if err != nil {
				return fmt.Errorf("unable to alter share group offsets: %v", err)
			}

			if err := kerr.ErrorForCode(kresp.ErrorCode); err != nil {
				msg := err.Error()
				if kresp.ErrorMessage != nil {
					msg += ": " + *kresp.ErrorMessage
				}
				return fmt.Errorf("%s", msg)
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
			return nil
		},
	}

	cmd.Flags().StringVar(&to, "to", "", "target offset (start, end, N, @timestamp; mutually exclusive with --to-file)")
	cmd.Flags().StringVar(&toFile, "to-file", "", "JSON file with per-partition offsets (mutually exclusive with --to)")
	cmd.Flags().StringSliceVar(&topics, "topics", nil, "topics to seek (required with --to; optional filter with --to-file)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "preview offset changes without applying")
	cmd.Flags().BoolVar(&execute, "execute", false, "apply changes without interactive confirmation")

	return cmd
}
