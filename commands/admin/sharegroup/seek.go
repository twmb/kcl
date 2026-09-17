package sharegroup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/offsetparse"
	"github.com/twmb/kcl/out"
)

func seekCommand(cl *client.Client) *cobra.Command {
	var (
		to     string
		toFile string
		topics []string
		dryRun bool
		yes    bool
	)

	cmd := &cobra.Command{
		Use:   "seek GROUP",
		Short: "Reset share group start offsets.",
		Long: `Reset share group start offsets.

Requires Kafka 4.0+ (KIP-932).

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

-t accepts plain names or topic:partitions pairs (matching the
kafka-share-groups.sh --topic syntax):
  foo              all partitions of foo
  foo:0,2          only partitions 0 and 2 of foo

The plan is printed first, one row per partition with the group's start
offset now and the offset the seek sets, then a [y/N] prompt unless --yes,
then the same rows with how each alter went. --dry-run stops after the
plan, as does a "no", or a stdin that is not a terminal, and exits 0. Under
--format json the whole seek is one document: {group, dry_run, plan,
results}, with results empty when nothing was altered.

EXAMPLES:
  kcl share-group seek mygroup --to start -t foo,bar
  kcl share-group seek mygroup --to end -t foo:0,1,2
  kcl share-group seek mygroup --to @-1h -t foo,bar
  kcl share-group seek mygroup --to 100 -t foo --dry-run
  kcl share-group seek mygroup --to-file offsets.json

SEE ALSO:
  kcl share-group describe    describe share groups with offsets and lag
  kcl group seek              reset a consumer group's committed offsets
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			groupName := args[0]

			hasTo := to != ""
			hasToFile := toFile != ""
			if !hasTo && !hasToFile {
				return out.Errf(out.ExitUsage, "one of --to or --to-file is required")
			}
			if hasTo && hasToFile {
				return out.Errf(out.ExitUsage, "--to and --to-file are mutually exclusive")
			}

			topicFilter, err := flagutil.ParseTopicPartitions(flagutil.SplitTopicPartitionEntries(topics))
			if err != nil {
				return out.Errf(out.ExitUsage, "invalid --topics: %v", err)
			}
			// keepPartition returns true if (topic, partition) is
			// in the filter. An empty filter matches everything; a
			// topic entry with nil partitions matches all of that
			// topic's partitions.
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
					targets = append(targets, targetOffset{e.Topic, e.Partition, e.Offset})
				}
			} else {
				spec, err := offsetparse.Parse(to, time.Now())
				if err != nil {
					return out.Errf(out.ExitUsage, "unable to parse --to %q: %v", to, err)
				}
				if spec.End != nil {
					return out.Errf(out.ExitUsage, "--to does not accept range offsets; use a single target value")
				}
				if spec.Start.Kind == offsetparse.KindRelative {
					return out.Errf(out.ExitUsage, "+N/-N relative offsets are not supported for share groups (use start, end, N, or @timestamp)")
				}

				if len(topicNames) == 0 {
					return out.Errf(out.ExitUsage, "-t is required when using --to")
				}

				keepListed := func(lo kadm.ListedOffset) bool {
					return lo.Err == nil && keepPartition(lo.Topic, lo.Partition)
				}

				switch spec.Start.Kind {
				case offsetparse.KindStart:
					listed, err := adm.ListStartOffsets(ctx, topicNames...)
					if err != nil {
						return fmt.Errorf("unable to list start offsets: %v", err)
					}
					listed.Each(func(lo kadm.ListedOffset) {
						if keepListed(lo) {
							targets = append(targets, targetOffset{lo.Topic, lo.Partition, lo.Offset})
						}
					})

				case offsetparse.KindEnd:
					listed, err := adm.ListEndOffsets(ctx, topicNames...)
					if err != nil {
						return fmt.Errorf("unable to list end offsets: %v", err)
					}
					listed.Each(func(lo kadm.ListedOffset) {
						if keepListed(lo) {
							targets = append(targets, targetOffset{lo.Topic, lo.Partition, lo.Offset})
						}
					})

				case offsetparse.KindExact:
					listed, err := adm.ListEndOffsets(ctx, topicNames...)
					if err != nil {
						return fmt.Errorf("unable to list offsets: %v", err)
					}
					listed.Each(func(lo kadm.ListedOffset) {
						if keepListed(lo) {
							targets = append(targets, targetOffset{lo.Topic, lo.Partition, spec.Start.Value})
						}
					})

				case offsetparse.KindTimestamp:
					listed, err := adm.ListOffsetsAfterMilli(ctx, spec.Start.Value, topicNames...)
					if err != nil {
						return fmt.Errorf("unable to resolve timestamp to offsets: %v", err)
					}
					listed.Each(func(lo kadm.ListedOffset) {
						if keepListed(lo) {
							targets = append(targets, targetOffset{lo.Topic, lo.Partition, lo.Offset})
						}
					})

				default:
					return out.Errf(out.ExitUsage, "unsupported seek target kind: %v", spec.Start.Kind)
				}
			}

			if len(targets) == 0 {
				fmt.Fprintln(os.Stderr, "No offsets to change.")
				return printSeek(cl, groupName, nil, nil, dryRun)
			}

			// The plan: the start offset each partition has now, from
			// DescribeShareGroupOffsets, beside the one the seek sets. A
			// partition the group has no offset for has no prior.
			rows := make([]seekRow, 0, len(targets))
			prior := make(map[string]map[int32]int64)
			if offsets, ok := fetchShareGroupOffsets(cl, []string{groupName})[groupName]; ok {
				for _, topic := range offsets.Topics {
					for _, p := range topic.Partitions {
						if prior[topic.Topic] == nil {
							prior[topic.Topic] = make(map[int32]int64)
						}
						if p.ErrorCode == 0 {
							prior[topic.Topic][p.Partition] = p.StartOffset
						}
					}
				}
			}
			for _, t := range targets {
				r := seekRow{topic: t.topic, partition: t.partition, prior: -1, at: t.offset}
				if at, ok := prior[t.topic][t.partition]; ok {
					r.prior = at
				}
				rows = append(rows, r)
			}
			sortSeekRows(rows)

			// Print the plan and ask. Under json the plan is part of the
			// one document printed at the end.
			if cl.Format() != out.FormatJSON {
				printSeekPlan(cl, rows, dryRun)
			}
			if dryRun {
				return printSeek(cl, groupName, rows, nil, true)
			}
			if !yes && out.Confirm(fmt.Sprintf("Apply these offset changes to share group %s?", groupName)) != out.Yes {
				return printSeek(cl, groupName, rows, nil, true)
			}

			// Commit via AlterShareGroupOffsets.
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

			// The results: each plan row with how its alter went. A
			// partition the response does not name is reported as
			// missing rather than as fine.
			type result struct{ err, message string }
			answered := make(map[string]map[int32]result)
			for _, topic := range kresp.Topics {
				if answered[topic.Topic] == nil {
					answered[topic.Topic] = make(map[int32]result)
				}
				for _, p := range topic.Partitions {
					var r result
					if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
						r.err = err.Error()
						if p.ErrorMessage != nil {
							r.message = *p.ErrorMessage
						}
					}
					answered[topic.Topic][p.Partition] = r
				}
			}
			results := make([]seekRow, len(rows))
			for i, r := range rows {
				res, ok := answered[r.topic][r.partition]
				if !ok {
					res.err = "not in the AlterShareGroupOffsets response"
				}
				r.err, r.message = res.err, res.message
				results[i] = r
			}
			return printSeek(cl, groupName, rows, results, false)
		},
	}
	out.Columns(cmd, seekHeaders...)

	cmd.Flags().StringVar(&to, "to", "", "target offset (start, end, N, @timestamp; mutually exclusive with --to-file)")
	cmd.Flags().StringVar(&toFile, "to-file", "", "JSON file with per-partition offsets (mutually exclusive with --to)")
	cmd.Flags().StringArrayVarP(&topics, "topic", "t", nil, "topics to seek; repeatable or comma-separated, entries may be topic:p1,p2 (required with --to; optional filter with --to-file)")
	cmd.Flags().StringArrayVar(&topics, "topics", nil, "old name of --topic")
	cmd.Flags().MarkHidden("topics")
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "preview offset changes without applying")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "apply changes without interactive confirmation")

	return cmd
}

// seekHeaders is the one shape of a plan row and a result row: a plan row
// has not run, so its ERROR and MESSAGE are Unknown.
var seekHeaders = []string{"TOPIC", "PARTITION", "PRIOR-OFFSET", "NEW-OFFSET", "ERROR", "MESSAGE"}

// seekRow is one partition of a seek: the start offset before, the one the
// seek sets, and, once run, how the alter went.
type seekRow struct {
	topic     string
	partition int32
	prior     int64 // -1 when the group had no start offset
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
