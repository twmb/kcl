package topic

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/offsetparse"
	"github.com/twmb/kcl/out"
)

var listOffsetsHeaders = []string{
	"BROKER", "TOPIC", "PARTITION",
	"START", "STABLE", "END",
	"START-EPOCH", "STABLE-EPOCH", "END-EPOCH",
	"AT", "ERROR",
}

// ListOffsetsCommand is "kcl topic list-offsets". It is also mounted, hidden,
// where it used to live as "kcl misc list-offsets", and names topic.list-offsets
// from either path.
func ListOffsetsCommand(cl *client.Client) *cobra.Command {
	var (
		withEpochs bool
		committed  bool
		at         string
	)

	cmd := &cobra.Command{
		Use:   "list-offsets [TOPICS...]",
		Short: "List start, stable, and end offsets for partitions.",
		Long: `List start, stable, and end offsets for partitions.

Issues ListOffsets (Kafka 0.10.0+) for every partition named. An argument is
topic:#,#,# for some partitions of a topic or topic for all of them, resolved
from metadata; with no argument, every non-internal topic is listed.

START is the log start offset, END the high watermark, and STABLE the last
stable offset, up to which a read_committed consumer can read. If STABLE is
below END, a transaction is open on that partition. Each offset carries the
leader epoch it was written under (Kafka 2.1.0+); --with-epochs shows the
three epoch columns in text, and json and awk always carry them.

--at TIMESTAMP fills AT with the first offset at or after the timestamp, or
the end offset when no record is that recent. The timestamp is one of:
  2024-01-15                 a date, UTC
  2024-01-15T10:30:00Z       RFC3339
  -1h, -7d                   a duration before now
  1705312200, 1705312200000  unix seconds or milliseconds

Rows are one per partition: BROKER TOPIC PARTITION START STABLE END
START-EPOCH STABLE-EPOCH END-EPOCH AT ERROR.

EXAMPLES:
  kcl topic list-offsets                    # every partition of every topic
  kcl topic list-offsets foo:1,2,3 bar:0    # some partitions
  kcl topic list-offsets foo --at -1h       # the offset an hour ago
  kcl topic list-offsets foo --with-epochs  # leader epochs in text too

SEE ALSO:
  kcl topic describe     partition leaders, replicas, and offsets
  kcl consume            read records from an offset or timestamp
`,
		RunE: func(_ *cobra.Command, args []string) error {
			cl.SetCommand("topic.list-offsets")

			var atMillis int64
			if at != "" {
				spec, err := offsetparse.Parse("@"+at, time.Now())
				if err != nil || spec.End != nil || spec.Start.Kind != offsetparse.KindTimestamp {
					return out.Errf(out.ExitUsage, "invalid --at %q: expected a date, an RFC3339 time, a -duration, or unix seconds or milliseconds", at)
				}
				atMillis = spec.Start.Value
			}

			ctx := context.Background()
			kcl := cl.Client()
			tps, topicErrs, err := resolveTopicPartitions(ctx, kcl, args)
			if err != nil {
				return err
			}

			isolation := int8(readUncommitted)
			if committed {
				isolation = readCommitted
			}
			listings := []listOffsetsAt{
				{isolation, tsStart},
				{isolation, tsEnd},
				{readCommitted, tsEnd},
			}
			if at != "" {
				listings = append(listings, listOffsetsAt{readUncommitted, atMillis})
			}
			listed := listOffsetsAll(ctx, kcl, tps, listings...)
			starts, ends, stables := listed[0], listed[1], listed[2]

			var rows [][]any
			var failed bool
			for _, topic := range sortedKeys(topicErrs) {
				tps[topic] = nil
			}
			for _, topic := range sortedKeys(tps) {
				if err := topicErrs[topic]; err != nil {
					failed = true
					rows = append(rows, []any{out.Unknown, topic, out.Unknown,
						out.Unknown, out.Unknown, out.Unknown,
						out.Unknown, out.Unknown, out.Unknown,
						out.Unknown, out.ErrCell(err)})
					continue
				}
				partitions := slices.Clone(tps[topic])
				slices.Sort(partitions)
				for _, p := range partitions {
					start, end, stable := starts.get(topic, p), ends.get(topic, p), stables.get(topic, p)
					var atCell any = out.Unknown
					var atErr error
					if at != "" {
						atOff := listed[3].get(topic, p)
						atErr = atOff.err
						switch {
						case atOff.err != nil:
						case atOff.offset < 0: // nothing that recent: the end
							atCell = end.cell()
						default:
							atCell = atOff.offset
						}
					}
					var errCell string
					for _, err := range []error{start.err, end.err, stable.err, atErr} {
						if err != nil {
							errCell = out.ErrCell(err)
							failed = true
							break
						}
					}
					broker := start
					for _, l := range []listedOffset{end, stable} {
						if broker.broker < 0 {
							broker = l
						}
					}
					rows = append(rows, []any{broker.brokerCell(), topic, p,
						start.cell(), stable.cell(), end.cell(),
						start.epochCell(), stable.epochCell(), end.epochCell(),
						atCell, errCell})
				}
			}

			// Text hides the columns no flag asked for.
			headers := listOffsetsHeaders
			if cl.Format() == out.FormatText {
				keep := []int{0, 1, 2, 3, 4, 5}
				if withEpochs {
					keep = append(keep, 6, 7, 8)
				}
				if at != "" {
					keep = append(keep, 9)
				}
				keep = append(keep, 10)
				headers = pick(headers, keep)
				for i, row := range rows {
					rows[i] = pick(row, keep)
				}
			}
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "offsets", headers...)
			for _, row := range rows {
				table.Row(row...)
			}
			if err := table.Flush(); err != nil {
				return err
			}
			if failed {
				return out.ErrSilent
			}
			return nil
		},
	}
	out.Columns(cmd, listOffsetsHeaders...)

	cmd.Flags().BoolVar(&committed, "committed", false, "list START and END under read_committed rather than read_uncommitted (Kafka 0.11.0+)")
	cmd.Flags().BoolVar(&withEpochs, "with-epochs", false, "show the START-EPOCH, STABLE-EPOCH, and END-EPOCH columns in text (json and awk always carry them)")
	cmd.Flags().StringVar(&at, "at", "", "fill AT with the first offset at or after this timestamp (a date, RFC3339, -duration, or unix seconds or millis)")

	return cmd
}

// resolveTopicPartitions parses topic:#,# arguments and asks metadata for the
// partitions of a topic given bare, or of every non-internal topic when no
// argument is given. A topic metadata answered with an error is returned
// under errs rather than tps.
func resolveTopicPartitions(ctx context.Context, cl *kgo.Client, args []string) (tps map[string][]int32, errs map[string]error, err error) {
	tps, err = flagutil.ParseTopicPartitions(args)
	if err != nil {
		return nil, nil, out.Errf(out.ExitUsage, "unable to parse topic partitions: %v", err)
	}
	var need []string
	for topic, ps := range tps {
		if len(ps) == 0 {
			need = append(need, topic)
		}
	}
	errs = make(map[string]error)
	if len(need) == 0 && len(tps) > 0 {
		return tps, errs, nil
	}
	found, errs, err := partitionsOf(ctx, cl, need)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get metadata: %v", err)
	}
	for _, topic := range need {
		delete(tps, topic)
	}
	for topic, ps := range found {
		tps[topic] = ps
	}
	return tps, errs, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// pick returns the elements of vals at idx, in that order.
func pick[T any](vals []T, idx []int) []T {
	picked := make([]T, 0, len(idx))
	for _, i := range idx {
		picked = append(picked, vals[i])
	}
	return picked
}
