package group

import (
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
	"github.com/twmb/kcl/flagutil"
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
		yes            bool
		allowNewTopics bool
	)

	cmd := &cobra.Command{
		Use:   "seek GROUP",
		Short: "Reset consumer group offsets.",
		Long: `Reset consumer group offsets.

Requires Kafka 0.11.0+.

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

-t accepts plain names or topic:partitions pairs:
  foo              all partitions of foo
  foo:0,2          only partitions 0 and 2 of foo

The plan is printed first, one row per partition with the offset committed
now and the offset the seek commits, then a [y/N] prompt unless --yes, then
the same rows with how each commit went. --dry-run stops after the plan, as
does a "no", or a stdin that is not a terminal, and exits 0. Under --format
json the whole seek is one document: {group, dry_run, plan, results}, with
results empty when nothing was committed.

EXAMPLES:
  kcl group seek mygroup --to start
  kcl group seek mygroup --to end -t foo,bar
  kcl group seek mygroup --to 100 -t foo:0,1,2
  kcl group seek mygroup --to @-1h
  kcl group seek mygroup --to @2024-01-15
  kcl group seek mygroup --to +0 --yes
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

			topicFilter, err := flagutil.ParseTopicPartitions(flagutil.SplitTopicPartitionEntries(topics))
			if err != nil {
				return out.Errf(out.ExitUsage, "invalid --topics: %v", err)
			}
			keepPartition := func(topic string, partition int32) bool {
				if len(topicFilter) == 0 {
					return true
				}
				parts, ok := topicFilter[topic]
				if !ok {
					return false
				}
				if parts == nil {
					return true
				}
				for _, p := range parts {
					if p == partition {
						return true
					}
				}
				return false
			}
			topicNames := make([]string, 0, len(topicFilter))
			for t := range topicFilter {
				topicNames = append(topicNames, t)
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
				return fmt.Errorf("group %q not found in describe response", groupName)
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
					if !keepPartition(o.Topic, o.Partition) {
						return
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
					return out.Errf(out.ExitUsage, "unable to read --to-file %q: %v", toFile, err)
				}
				var entries []fileEntry
				err = json.Unmarshal(data, &entries)
				if err != nil {
					return out.Errf(out.ExitUsage, "unable to parse --to-file %q: %v", toFile, err)
				}
				for _, e := range entries {
					if !keepPartition(e.Topic, e.Partition) {
						continue
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
					return out.Errf(out.ExitUsage, "unable to parse --to %q: %v", to, err)
				}
				if spec.End != nil {
					return out.Errf(out.ExitUsage, "--to does not accept range offsets; use a single target value")
				}

				// Determine target topics: either from -t flag or from existing commits.
				var targetTopics []string
				if len(topicNames) > 0 {
					targetTopics = topicNames
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
					return fmt.Errorf("no topics to seek; the group has no committed offsets and -t was not specified")
				}

				keepListed := func(lo kadm.ListedOffset) bool {
					return lo.Err == nil && keepPartition(lo.Topic, lo.Partition)
				}

				switch spec.Start.Kind {
				case offsetparse.KindStart:
					listed, err := adm.ListStartOffsets(ctx, targetTopics...)
					if err != nil {
						return fmt.Errorf("unable to list start offsets: %v", err)
					}
					listed.Each(func(lo kadm.ListedOffset) {
						if keepListed(lo) {
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
						if keepListed(lo) {
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
						if keepListed(lo) {
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
						if keepListed(lo) {
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
						if !keepPartition(o.Topic, o.Partition) {
							return
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
				return printSeek(cl, groupName, nil, nil, dryRun)
			}

			// 4. Build the plan.
			var rows []seekRow
			newOffsets.Each(func(o kadm.Offset) {
				prior := int64(-1)
				if fr, ok := fetched.Lookup(o.Topic, o.Partition); ok && fr.Err == nil {
					prior = fr.Offset.At
				}
				rows = append(rows, seekRow{
					topic:     o.Topic,
					partition: o.Partition,
					prior:     prior,
					at:        o.At,
				})
			})
			sortSeekRows(rows)

			// 5. Print the plan and ask. Under json the plan is part of
			// the one document printed at the end.
			if cl.Format() != out.FormatJSON {
				printSeekPlan(cl, rows, dryRun)
			}
			if dryRun {
				return printSeek(cl, groupName, rows, nil, true)
			}
			if !yes && out.Confirm(fmt.Sprintf("Apply these offset changes to group %s?", groupName)) != out.Yes {
				return printSeek(cl, groupName, rows, nil, true)
			}

			// 6. Commit offsets.
			committed, err := adm.CommitOffsets(ctx, groupName, newOffsets)
			if err != nil {
				return fmt.Errorf("unable to commit offsets: %v", err)
			}
			results := make([]seekRow, len(rows))
			for i, r := range rows {
				if cr, ok := committed.Lookup(r.topic, r.partition); ok && cr.Err != nil {
					r.err = out.ErrCell(cr.Err)
					if cr.Err == kerr.UnknownMemberID {
						r.message = "group is not empty (a consumer may have joined)"
					}
				}
				results[i] = r
			}
			return printSeek(cl, groupName, rows, results, false)
		},
	}
	out.Columns(cmd, seekHeaders...)

	cmd.Flags().StringVar(&to, "to", "", "target offset (start, end, +N, -N, N, @timestamp)")
	cmd.Flags().StringVar(&toGroup, "to-group", "", "seek to another group's committed offsets (mutually exclusive with --to and --to-file)")
	cmd.Flags().StringVar(&toFile, "to-file", "", "seek to offsets from a JSON file (mutually exclusive with --to and --to-group)")
	cmd.Flags().StringArrayVarP(&topics, "topic", "t", nil, "filter to specific topics; repeatable or comma-separated, entries may be topic:p1,p2")
	cmd.Flags().StringArrayVar(&topics, "topics", nil, "old name of --topic")
	cmd.Flags().MarkHidden("topics")
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "preview offset changes without applying")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "apply changes without interactive confirmation")
	cmd.Flags().BoolVar(&allowNewTopics, "allow-new-topics", false, "allow committing offsets for topics not in the group's current commits")

	return cmd
}

// seekHeaders is the one shape of a plan row and a result row: a plan row
// has not run, so its ERROR and MESSAGE are Unknown.
var seekHeaders = []string{"TOPIC", "PARTITION", "PRIOR-OFFSET", "NEW-OFFSET", "ERROR", "MESSAGE"}

// seekRow is one partition of a seek: what was committed before, what the
// seek commits, and, once run, how the commit went.
type seekRow struct {
	topic     string
	partition int32
	prior     int64 // -1 when the group had nothing committed
	at        int64
	err       string
	message   string
}

func sortSeekRows(rows []seekRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].topic != rows[j].topic {
			return rows[i].topic < rows[j].topic
		}
		return rows[i].partition < rows[j].partition
	})
}

func (r seekRow) priorNum() any {
	if r.prior < 0 {
		return out.Unknown
	}
	return r.prior
}

// planValues is the row before the seek runs; resultValues is the row after.
func (r seekRow) planValues() []any {
	return []any{r.topic, r.partition, r.priorNum(), r.at, out.Unknown, out.Unknown}
}

func (r seekRow) resultValues() []any {
	return []any{r.topic, r.partition, r.priorNum(), r.at, r.err, r.message}
}

func (r seekRow) json(ran bool) map[string]any {
	var errV, msgV any = out.Unknown, out.Unknown
	if ran {
		errV, msgV = r.err, r.message
	}
	return map[string]any{
		"topic":        r.topic,
		"partition":    r.partition,
		"prior_offset": r.priorNum(),
		"new_offset":   r.at,
		"error":        errV,
		"message":      msgV,
	}
}

// printSeekPlan prints the plan in text or awk, before the prompt. Text
// leaves out the ERROR and MESSAGE columns, which nothing has filled yet;
// awk prints the full row with them Unknown, so a plan row and a result row
// have the same fields.
func printSeekPlan(cl *client.Client, rows []seekRow, dryRun bool) {
	if cl.Format() == out.FormatAWK {
		table := out.NewFormattedTable(out.FormatAWK, cl.Command(), 1, "plan", seekHeaders...)
		for _, r := range rows {
			table.Row(r.planValues()...)
		}
		table.Flush()
		return
	}
	if dryRun {
		out.PrintDryRun()
	}
	table := out.NewFormattedTable(out.FormatText, cl.Command(), 1, "plan", seekHeaders[:4]...)
	for _, r := range rows {
		table.Row(r.planValues()[:4]...)
	}
	table.Flush()
}

// printSeek prints what the seek did. JSON is one document, {group, dry_run,
// plan, results}, with results empty on a dry run or a declined prompt;
// text and awk have printed the plan already and print the results table,
// and nothing more when there are none. It returns ErrSilent when a result
// carries an error, so the command exits 1.
func printSeek(cl *client.Client, group string, plan, results []seekRow, dryRun bool) error {
	if cl.Format() == out.FormatJSON {
		planJSON := make([]map[string]any, 0, len(plan))
		for _, r := range plan {
			planJSON = append(planJSON, r.json(false))
		}
		resultsJSON := make([]map[string]any, 0, len(results))
		for _, r := range results {
			resultsJSON = append(resultsJSON, r.json(true))
		}
		out.MarshalJSON(cl.Command(), 1, map[string]any{
			"group":   group,
			"plan":    planJSON,
			"results": resultsJSON,
		}, out.DryRun(dryRun))
		for _, r := range results {
			if r.err != "" {
				return out.ErrSilent
			}
		}
		return nil
	}
	if results == nil {
		return nil
	}
	if cl.Format() == out.FormatText {
		fmt.Println()
	}
	table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", seekHeaders...).ResultColumns()
	for _, r := range results {
		table.Row(r.resultValues()...)
	}
	return table.Flush()
}
