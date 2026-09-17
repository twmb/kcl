package topic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
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

var (
	trimPrefixHeaders = []string{"TOPIC", "PARTITION", "PRIOR-OFFSET", "NEW-OFFSET", "ERROR", "MESSAGE"}
	trimPrefixKeys    = []string{"topic", "partition", "prior_offset", "new_offset", "error", "message"}
)

func topicTrimPrefixCommand(cl *client.Client) *cobra.Command {
	var (
		offsetFlag string
		partitions []int32
		yes        bool
		fromFile   string
	)

	cmd := &cobra.Command{
		Use:   "trim-prefix TOPIC",
		Short: "Delete records before a given offset or timestamp.",
		Long: `Delete records before a given offset or timestamp.

Requires Kafka 0.11.0+.

This is a user-friendly wrapper around DeleteRecords. It resolves symbolic
offsets (timestamps, 'end', relative) via ListOffsets before issuing the
delete. Records before the resolved offset become inaccessible.

The --offset flag accepts the same syntax as consume --offset:
  N             delete records before exact offset N
  end           delete all records (trim to high watermark)
  @TIMESTAMP    delete records before the timestamp

The plan is printed first, one row per partition: TOPIC PARTITION
PRIOR-OFFSET NEW-OFFSET, the low watermark now and the offset records are
deleted before. Without -y you are asked to confirm; answering no, or
running with stdin not a terminal, prints the plan as a dry run and exits 0.
The results have the same shape with ERROR and MESSAGE, NEW-OFFSET being the
low watermark the broker reports after the delete. In json the document is
{dry_run, plan, results}; awk prints the result rows, or the plan rows when
nothing was deleted.

EXAMPLES:
  kcl topic trim-prefix foo --offset 1000
  kcl topic trim-prefix foo --offset end
  kcl topic trim-prefix foo --offset @-7d
  kcl topic trim-prefix foo --offset @2024-01-15 --partitions 0,1,2
  kcl topic trim-prefix foo --offset end < /dev/null   # the plan alone

SEE ALSO:
  kcl topic describe     describe topic partitions
  kcl topic list         list topics
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			cl.SetCommand("topic.trim-prefix")
			if fromFile != "" && offsetFlag != "" {
				return out.Errf(out.ExitUsage, "--offset and --from-file are mutually exclusive")
			}
			if fromFile == "" && offsetFlag == "" {
				return out.Errf(out.ExitUsage, "one of --offset or --from-file is required")
			}
			var spec offsetparse.Spec
			if offsetFlag != "" {
				var err error
				spec, err = offsetparse.Parse(offsetFlag, time.Now())
				if err != nil {
					return out.Errf(out.ExitUsage, "unable to parse --offset %q: %v", offsetFlag, err)
				}
				if spec.End != nil {
					return out.Errf(out.ExitUsage, "--offset does not accept range syntax for trim-prefix")
				}
				switch spec.Start.Kind {
				case offsetparse.KindExact, offsetparse.KindEnd, offsetparse.KindTimestamp, offsetparse.KindStart:
				default:
					return out.Errf(out.ExitUsage, "unsupported offset kind for trim-prefix: %v", spec.Start.Kind)
				}
			}

			resolved, err := flagutil.ResolveTopics(context.Background(), cl.Client(), args)
			if err != nil {
				return err
			}
			topicName := resolved[0]

			kclClient := cl.Client()
			ctx := context.Background()

			// The plan: the partitions to trim and where to.
			type trimTarget struct {
				partition int32
				offset    int64
			}
			var targets []trimTarget

			// The wanted partitions and their low watermarks, which are
			// PRIOR-OFFSET.
			tps, topicErrs, err := partitionsOf(ctx, kclClient, []string{topicName})
			if err != nil {
				return fmt.Errorf("unable to get metadata: %v", err)
			}
			if err := topicErrs[topicName]; err != nil {
				return out.Errf(out.ExitError, "topic %s: %v", topicName, err)
			}
			wanted := slices.DeleteFunc(tps[topicName], func(p int32) bool {
				return len(partitions) > 0 && !slices.Contains(partitions, p)
			})
			tps[topicName] = wanted
			starts := listOffsets(ctx, kclClient, readUncommitted, tsStart, tps)

			if fromFile != "" {
				type fileEntry struct {
					Topic     string `json:"topic"`
					Partition int32  `json:"partition"`
					Offset    int64  `json:"offset"`
				}
				raw, err := os.ReadFile(fromFile)
				if err != nil {
					return out.Errf(out.ExitUsage, "unable to read --from-file: %v", err)
				}
				var entries []fileEntry
				if err := json.Unmarshal(raw, &entries); err != nil {
					return out.Errf(out.ExitUsage, "unable to parse --from-file: %v", err)
				}
				for _, e := range entries {
					if e.Topic != topicName {
						continue
					}
					targets = append(targets, trimTarget{e.Partition, e.Offset})
				}
			} else {
				switch spec.Start.Kind {
				case offsetparse.KindExact:
					for _, p := range wanted {
						targets = append(targets, trimTarget{p, spec.Start.Value})
					}
				case offsetparse.KindEnd:
					ends := listOffsets(ctx, kclClient, readUncommitted, tsEnd, tps)
					for _, p := range wanted {
						if lo := ends.get(topicName, p); lo.err == nil {
							targets = append(targets, trimTarget{p, lo.offset})
						}
					}
				case offsetparse.KindTimestamp:
					listed, err := kadm.NewClient(kclClient).ListOffsetsAfterMilli(ctx, spec.Start.Value, topicName)
					if err != nil {
						return fmt.Errorf("unable to resolve timestamp: %v", err)
					}
					for _, p := range wanted {
						if lo, ok := listed.Lookup(topicName, p); ok && lo.Err == nil {
							targets = append(targets, trimTarget{p, lo.Offset})
						}
					}
				}
			}
			slices.SortFunc(targets, func(l, r trimTarget) int { return int(l.partition - r.partition) })

			plan := make([][]any, 0, len(targets))
			for _, t := range targets {
				plan = append(plan, []any{topicName, t.partition, starts.get(topicName, t.partition).cell(), t.offset, "", ""})
			}

			// printPlan prints the plan as the whole document: json under
			// plan with no results, awk as the rows, text as the dry run
			// line, since text printed the table before asking.
			printPlan := func() error {
				switch cl.Format() {
				case out.FormatJSON:
					out.MarshalJSON(cl.Command(), 1, map[string]any{
						"plan":    rowMaps(trimPrefixKeys, plan),
						"results": []map[string]any{},
					}, out.DryRun(true))
				case out.FormatAWK:
					table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "plan", trimPrefixHeaders...)
					for _, row := range plan {
						table.Row(row...)
					}
					return table.Flush()
				default:
					out.PrintDryRun()
				}
				return nil
			}

			if len(targets) == 0 {
				if spec.Start.Kind == offsetparse.KindStart && fromFile == "" {
					fmt.Fprintln(os.Stderr, "Nothing to trim: the offset is already the start.")
				} else {
					fmt.Fprintln(os.Stderr, "No partitions to trim.")
				}
				return printPlan()
			}

			if cl.Format() == out.FormatText {
				table := out.NewTable("TOPIC", "PARTITION", "PRIOR-OFFSET", "NEW-OFFSET")
				for _, row := range plan {
					table.Print(row[:4]...)
				}
				table.Flush()
			}
			if !yes && out.Confirm("Delete records before these offsets?") != out.Yes {
				return printPlan()
			}

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

			// Results by partition, so that they print in plan order and
			// a broker we could not ask answers for every partition it
			// was asked about.
			results := make(map[int32][]any)
			for _, shard := range kclClient.RequestSharded(ctx, req) {
				if shard.Err != nil {
					for _, t := range shard.Req.(*kmsg.DeleteRecordsRequest).Topics {
						for _, p := range t.Partitions {
							results[p.Partition] = []any{topicName, p.Partition, starts.get(topicName, p.Partition).cell(), out.Unknown, shard.Err.Error(), ""}
						}
					}
					continue
				}
				for _, t := range shard.Resp.(*kmsg.DeleteRecordsResponse).Topics {
					for _, p := range t.Partitions {
						var low any = p.LowWatermark
						errStr := ""
						if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
							errStr = err.Error()
							low = out.Unknown
						}
						results[p.Partition] = []any{topicName, p.Partition, starts.get(topicName, p.Partition).cell(), low, errStr, ""}
					}
				}
			}
			rows := make([][]any, 0, len(targets))
			for _, t := range targets {
				row, ok := results[t.partition]
				if !ok {
					row = []any{topicName, t.partition, starts.get(topicName, t.partition).cell(), out.Unknown, "the broker did not answer for this partition", ""}
				}
				rows = append(rows, row)
			}

			if cl.Format() == out.FormatJSON {
				out.MarshalJSON(cl.Command(), 1, map[string]any{
					"plan":    rowMaps(trimPrefixKeys, plan),
					"results": rowMaps(trimPrefixKeys, rows),
				})
				for _, row := range rows {
					if row[4] != "" {
						return out.ErrSilent
					}
				}
				return nil
			}
			if cl.Format() == out.FormatText {
				fmt.Println()
			}
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", trimPrefixHeaders...).ResultColumns()
			for _, row := range rows {
				table.Row(row...)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, trimPrefixHeaders...)

	cmd.Flags().StringVarP(&offsetFlag, "offset", "o", "", "offset or timestamp to trim before (N, end, @timestamp)")
	cmd.Flags().Int32SliceVarP(&partitions, "partitions", "p", nil, "limit to specific partitions (default: all)")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "skip confirmation prompt")
	cmd.Flags().StringVar(&fromFile, "from-file", "", "JSON file of [{topic, partition, offset}, ...] to trim")

	return cmd
}
