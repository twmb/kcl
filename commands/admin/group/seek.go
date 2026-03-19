package group

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

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/offsetparse"
	"github.com/twmb/kcl/out"
)

func seekCommand(cl *client.Client) *cobra.Command {
	var (
		to             string
		toGroup        string
		toFile         string
		topics         []string
		dryRun         bool
		execute        bool
		allowNewTopics bool
	)

	cmd := &cobra.Command{
		Use:   "seek GROUP",
		Short: "Reset consumer group offsets.",
		Long: `Reset consumer group offsets (Kafka 0.11.0+).

Seek adjusts the committed offsets for a consumer group. The group must
have no active members (state Empty or Dead).

Exactly one of --to, --to-group, or --to-file must be specified.

The --to flag accepts offset specifications:
  start              earliest offset
  end                latest offset
  +N                 shift forward N from current committed offset
  -N                 shift backward N from current committed offset
  N                  exact offset N
  @TIMESTAMP         seek to a timestamp (unix ms/s/ns, date, RFC3339, -duration)

The --to-group flag copies committed offsets from another group.

The --to-file flag reads target offsets from a JSON file with format:
  [{"topic": "foo", "partition": 0, "offset": 100}, ...]

By default, seeking will only commit offsets for topics already present
in the group's committed offsets. Use --allow-new-topics to also commit
offsets for topics not currently in the group.

EXAMPLES:
  kcl group seek mygroup --to start
  kcl group seek mygroup --to end --topics foo,bar
  kcl group seek mygroup --to @-1h
  kcl group seek mygroup --to @2024-01-15
  kcl group seek mygroup --to +0 --execute
  kcl group seek mygroup --to -1000 --dry-run
  kcl group seek mygroup --to-group othergroup
  kcl group seek mygroup --to-file offsets.json
  kcl group seek mygroup --to-group othergroup --allow-new-topics

SEE ALSO:
  kcl group describe    describe consumer groups with lag
  kcl group list        list all groups
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			groupName := args[0]

			// Mutual exclusivity: exactly one of --to, --to-group, --to-file.
			nSources := 0
			if to != "" {
				nSources++
			}
			if toGroup != "" {
				nSources++
			}
			if toFile != "" {
				nSources++
			}
			if nSources == 0 {
				return out.Errf(out.ExitUsage, "one of --to, --to-group, or --to-file is required")
			}
			if nSources > 1 {
				return out.Errf(out.ExitUsage, "--to, --to-group, and --to-file are mutually exclusive")
			}

			kclClient := cl.Client()
			adm := kadm.NewClient(kclClient)
			ctx := context.Background()

			// 1. Check group state.
			described, err := adm.DescribeGroups(ctx, groupName)
			if err != nil {
				return fmt.Errorf("unable to describe group: %v", err)
			}
			g, ok := described[groupName]
			if !ok {
				return fmt.Errorf( "group %q not found in describe response", groupName)
			}
			if g.Err != nil {
				return fmt.Errorf("error describing group %q: %v", groupName, g.Err)
			}
			state := strings.ToLower(g.State)
			if state != "empty" && state != "dead" {
				return fmt.Errorf("group %q is in state %q; must be Empty or Dead to seek offsets (stop all consumers first)", groupName, g.State)
			}

			// 2. Fetch current committed offsets.
			fetched, err := adm.FetchOffsets(ctx, groupName)
			if err != nil {
				return fmt.Errorf("unable to fetch offsets for group %q: %v", groupName, err)
			}

			// 3. Resolve target offsets.
			newOffsets := make(kadm.Offsets)

			switch {
			case toGroup != "":
				// --to-group: fetch offsets from the source group.
				srcFetched, err := adm.FetchOffsets(ctx, toGroup)
				if err != nil {
					return fmt.Errorf("unable to fetch offsets for source group %q: %v", toGroup, err)
				}
				srcFetched.Each(func(o kadm.OffsetResponse) {
					if o.Err != nil {
						return
					}
					// Filter to target topics if --topics is specified.
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
					newOffsets.Add(kadm.Offset{
						Topic:       o.Topic,
						Partition:   o.Partition,
						At:          o.Offset.At,
						LeaderEpoch: o.Offset.LeaderEpoch,
					})
				})

			case toFile != "":
				// --to-file: read offsets from a JSON file.
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
					// Filter to target topics if --topics is specified.
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
					newOffsets.Add(kadm.Offset{
						Topic:       e.Topic,
						Partition:   e.Partition,
						At:          e.Offset,
						LeaderEpoch: -1,
					})
				}

			default:
				// --to: parse offset specification.
				spec, err := offsetparse.Parse(to, time.Now())
				if err != nil {
					return fmt.Errorf("unable to parse --to %q: %v", to, err)
				}
				if spec.End != nil {
					return out.Errf(out.ExitUsage, "--to does not accept range offsets; use a single target value")
				}

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
					return fmt.Errorf( "no topics to seek; the group has no committed offsets and --topics was not specified")
				}

				switch spec.Start.Kind {
				case offsetparse.KindStart:
					listed, err := adm.ListStartOffsets(ctx, targetTopics...)
					if err != nil {
						return fmt.Errorf("unable to list start offsets: %v", err)
					}
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
					if err != nil {
						return fmt.Errorf("unable to list end offsets: %v", err)
					}
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
					if err != nil {
						return fmt.Errorf("unable to list offsets: %v", err)
					}
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
					if err != nil {
						return fmt.Errorf("unable to resolve timestamp to offsets: %v", err)
					}
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
					return out.Errf(out.ExitUsage, "unsupported seek target kind: %v", spec.Start.Kind)
				}
			}

			// Filter out topics not in current commits unless --allow-new-topics.
			if !allowNewTopics {
				existingTopics := make(map[string]bool)
				fetched.Each(func(o kadm.OffsetResponse) {
					existingTopics[o.Topic] = true
				})
				filtered := make(kadm.Offsets)
				skippedTopics := make(map[string]bool)
				newOffsets.Each(func(o kadm.Offset) {
					if existingTopics[o.Topic] {
						filtered.Add(o)
					} else if !skippedTopics[o.Topic] {
						skippedTopics[o.Topic] = true
						fmt.Fprintf(os.Stderr, "WARNING: skipping topic %q: not in group's current committed offsets (use --allow-new-topics to include)\n", o.Topic)
					}
				})
				newOffsets = filtered
			}

			if len(newOffsets) == 0 {
				fmt.Fprintln(os.Stderr, "No offsets to change.")
				return nil
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
			fmt.Fprintf(os.Stderr, "GROUP: %s\n\n", groupName)
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
				return nil
			}
			if !execute {
				fmt.Fprint(os.Stderr, "\nApply these offset changes? [y/N] ")
				scanner := bufio.NewScanner(os.Stdin)
				scanner.Scan()
				answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
				if answer != "y" && answer != "yes" {
					fmt.Fprintln(os.Stderr, "Aborted.")
					return nil
				}
			}

			// 7. Commit offsets.
			fmt.Fprintln(os.Stderr)
			committed, err := adm.CommitOffsets(ctx, groupName, newOffsets)
			if err != nil {
				return fmt.Errorf("unable to commit offsets: %v", err)
			}

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
			return nil
		},
	}

	cmd.Flags().StringVar(&to, "to", "", "target offset (start, end, +N, -N, N, @timestamp)")
	cmd.Flags().StringVar(&toGroup, "to-group", "", "seek to another group's committed offsets (mutually exclusive with --to and --to-file)")
	cmd.Flags().StringVar(&toFile, "to-file", "", "seek to offsets from a JSON file (mutually exclusive with --to and --to-group)")
	cmd.Flags().StringSliceVar(&topics, "topics", nil, "filter to specific topics (comma-separated, repeatable)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "preview offset changes without applying")
	cmd.Flags().BoolVar(&execute, "execute", false, "apply changes without interactive confirmation")
	cmd.Flags().BoolVar(&allowNewTopics, "allow-new-topics", false, "allow committing offsets for topics not in the group's current commits")

	return cmd
}
