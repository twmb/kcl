package group

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
		Short: "Reset consumer group offsets.",
		Long: `Reset consumer group offsets (Kafka 0.11.0+).

Seek adjusts the committed offsets for a consumer group. The group must
have no active members (state Empty or Dead).

The --to flag accepts offset specifications:
  start              earliest offset
  end                latest offset
  +N                 shift forward N from current committed offset
  -N                 shift backward N from current committed offset
  N                  exact offset N
  @TIMESTAMP         seek to a timestamp (unix ms/s/ns, date, RFC3339, -duration)

EXAMPLES:
  kcl group seek mygroup --to start
  kcl group seek mygroup --to end --topics foo,bar
  kcl group seek mygroup --to @-1h
  kcl group seek mygroup --to @2024-01-15
  kcl group seek mygroup --to +0 --execute
  kcl group seek mygroup --to -1000 --dry-run

SEE ALSO:
  kcl group describe    describe consumer groups with lag
  kcl group list        list all groups
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

			kclClient := cl.Client()
			adm := kadm.NewClient(kclClient)
			ctx := context.Background()

			// 1. Check group state.
			described, err := adm.DescribeGroups(ctx, groupName)
			out.MaybeDie(err, "unable to describe group: %v", err)
			g, ok := described[groupName]
			if !ok {
				out.Die("group %q not found in describe response", groupName)
			}
			if g.Err != nil {
				out.Die("error describing group %q: %v", groupName, g.Err)
			}
			state := strings.ToLower(g.State)
			if state != "empty" && state != "dead" {
				out.Die("group %q is in state %q; must be Empty or Dead to seek offsets (stop all consumers first)", groupName, g.State)
			}

			// 2. Fetch current committed offsets.
			fetched, err := adm.FetchOffsets(ctx, groupName)
			out.MaybeDie(err, "unable to fetch offsets for group %q: %v", groupName, err)

			// Determine target topics: either from --topics flag or from existing commits.
			var targetTopics []string
			if len(topics) > 0 {
				targetTopics = topics
			} else {
				seen := make(map[string]bool)
				fetched.Each(func(o kadm.OffsetResponse) {
					if !seen[o.Topic] {
						seen[o.Topic] = true
						targetTopics = append(targetTopics, o.Topic)
					}
				})
			}
			if len(targetTopics) == 0 {
				out.Die("no topics to seek; the group has no committed offsets and --topics was not specified")
			}

			// 3. Resolve target offsets.
			newOffsets := make(kadm.Offsets)
			switch spec.Start.Kind {
			case offsetparse.KindStart:
				listed, err := adm.ListStartOffsets(ctx, targetTopics...)
				out.MaybeDie(err, "unable to list start offsets: %v", err)
				listed.Each(func(lo kadm.ListedOffset) {
					if lo.Err == nil {
						newOffsets.Add(kadm.Offset{
							Topic:       lo.Topic,
							Partition:   lo.Partition,
							At:          lo.Offset,
							LeaderEpoch: lo.LeaderEpoch,
						})
					}
				})

			case offsetparse.KindEnd:
				listed, err := adm.ListEndOffsets(ctx, targetTopics...)
				out.MaybeDie(err, "unable to list end offsets: %v", err)
				listed.Each(func(lo kadm.ListedOffset) {
					if lo.Err == nil {
						newOffsets.Add(kadm.Offset{
							Topic:       lo.Topic,
							Partition:   lo.Partition,
							At:          lo.Offset,
							LeaderEpoch: lo.LeaderEpoch,
						})
					}
				})

			case offsetparse.KindExact:
				// Need to know partitions. List end offsets to discover them.
				listed, err := adm.ListEndOffsets(ctx, targetTopics...)
				out.MaybeDie(err, "unable to list offsets: %v", err)
				listed.Each(func(lo kadm.ListedOffset) {
					if lo.Err == nil {
						newOffsets.Add(kadm.Offset{
							Topic:       lo.Topic,
							Partition:   lo.Partition,
							At:          spec.Start.Value,
							LeaderEpoch: -1,
						})
					}
				})

			case offsetparse.KindTimestamp:
				listed, err := adm.ListOffsetsAfterMilli(ctx, spec.Start.Value, targetTopics...)
				out.MaybeDie(err, "unable to resolve timestamp to offsets: %v", err)
				listed.Each(func(lo kadm.ListedOffset) {
					if lo.Err == nil {
						newOffsets.Add(kadm.Offset{
							Topic:       lo.Topic,
							Partition:   lo.Partition,
							At:          lo.Offset,
							LeaderEpoch: lo.LeaderEpoch,
						})
					}
				})

			case offsetparse.KindRelative:
				// +N/-N relative to current committed offsets.
				fetched.Each(func(o kadm.OffsetResponse) {
					if o.Err != nil {
						return
					}
					// Filter to target topics.
					if len(topics) > 0 {
						found := false
						for _, t := range topics {
							if t == o.Topic {
								found = true
								break
							}
						}
						if !found {
							return
						}
					}
					newAt := o.Offset.At + spec.Start.Value
					if newAt < 0 {
						newAt = 0
					}
					newOffsets.Add(kadm.Offset{
						Topic:       o.Topic,
						Partition:   o.Partition,
						At:          newAt,
						LeaderEpoch: -1,
					})
				})

			default:
				out.Die("unsupported seek target kind: %v", spec.Start.Kind)
			}

			if len(newOffsets) == 0 {
				fmt.Println("No offsets to change.")
				return
			}

			// 4. Build preview rows.
			type seekRow struct {
				topic     string
				partition int32
				priorAt   int64
				newAt     int64
			}
			var rows []seekRow
			newOffsets.Each(func(o kadm.Offset) {
				prior := int64(-1)
				if fr, ok := fetched.Lookup(o.Topic, o.Partition); ok && fr.Err == nil {
					prior = fr.Offset.At
				}
				rows = append(rows, seekRow{
					topic:     o.Topic,
					partition: o.Partition,
					priorAt:   prior,
					newAt:     o.At,
				})
			})
			sort.Slice(rows, func(i, j int) bool {
				if rows[i].topic != rows[j].topic {
					return rows[i].topic < rows[j].topic
				}
				return rows[i].partition < rows[j].partition
			})

			// 5. Print preview table.
			fmt.Printf("GROUP: %s\n\n", groupName)
			tw := out.NewTable("TOPIC", "PARTITION", "PRIOR-OFFSET", "NEW-OFFSET")
			for _, r := range rows {
				priorStr := fmt.Sprintf("%d", r.priorAt)
				if r.priorAt < 0 {
					priorStr = "-"
				}
				tw.Print(r.topic, r.partition, priorStr, r.newAt)
			}
			tw.Flush()

			// 6. Approval phase.
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

			// 7. Commit offsets.
			fmt.Println()
			committed, err := adm.CommitOffsets(ctx, groupName, newOffsets)
			out.MaybeDie(err, "unable to commit offsets: %v", err)

			// 8. Print results.
			resultTw := out.NewTable("TOPIC", "PARTITION", "PRIOR-OFFSET", "NEW-OFFSET", "ERROR")
			for _, r := range rows {
				priorStr := fmt.Sprintf("%d", r.priorAt)
				if r.priorAt < 0 {
					priorStr = "-"
				}
				errStr := ""
				if cr, ok := committed.Lookup(r.topic, r.partition); ok && cr.Err != nil {
					errMsg := cr.Err.Error()
					if cr.Err == kerr.UnknownMemberID {
						errMsg = "group is not empty (a consumer may have joined)"
					}
					errStr = errMsg
				}
				resultTw.Print(r.topic, r.partition, priorStr, r.newAt, errStr)
			}
			resultTw.Flush()
		},
	}

	cmd.Flags().StringVar(&to, "to", "", "target offset (start, end, +N, -N, N, @timestamp)")
	cmd.Flags().StringSliceVar(&topics, "topics", nil, "filter to specific topics (comma-separated, repeatable)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "preview offset changes without applying")
	cmd.Flags().BoolVar(&execute, "execute", false, "apply changes without interactive confirmation")
	cmd.MarkFlagRequired("to")

	return cmd
}
