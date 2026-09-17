package logdirs

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logdirs",
		Short: "Alter or describe partition log directories.",
	}
	cmd.AddCommand(describeCommand(cl))
	cmd.AddCommand(alterReplicasCommand(cl))
	return cmd
}

func humanSize(bytes int64) string {
	const (
		kb = 1024
		mb = kb * 1024
		gb = mb * 1024
		tb = gb * 1024
	)
	switch {
	case bytes >= tb:
		return fmt.Sprintf("%.1fTB", float64(bytes)/float64(tb))
	case bytes >= gb:
		return fmt.Sprintf("%.1fGB", float64(bytes)/float64(gb))
	case bytes >= mb:
		return fmt.Sprintf("%.1fMB", float64(bytes)/float64(mb))
	case bytes >= kb:
		return fmt.Sprintf("%.1fKB", float64(bytes)/float64(kb))
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}

// formatSize renders a byte count for a size column, human readable if
// --human-readable was used. A broker that does not report a size sends -1,
// which we print as a dash rather than as a number that looks like a size.
//
// -H is a display choice for text: JSON and awk always get the number, so
// that a size a script reads never changes type with a flag.
func formatSize(bytes int64, human bool) any {
	if bytes < 0 {
		return out.Unknown
	}
	if human {
		return humanSize(bytes)
	}
	return bytes
}

// The columns of a describe: one row per partition directory, and under
// --aggregate-into one row per broker, dir, or topic.
var (
	describeHeaders  = []string{"BROKER", "DIR", "TOPIC", "PARTITION", "SIZE", "OFFSET-LAG", "IS-FUTURE", "TOTAL", "USABLE", "CORDONED", "ERROR"}
	aggregateHeaders = map[string][]string{
		"broker": {"BROKER", "SIZE"},
		"dir":    {"DIR", "SIZE"},
		"topic":  {"TOPIC", "SIZE"},
	}
	alterHeaders = []string{"TOPIC", "PARTITION", "ERROR", "MESSAGE"}
)

func describeCommand(cl *client.Client) *cobra.Command {
	var broker int32
	var humanReadable bool
	var sortBySize bool
	var aggregateInto string
	cmd := &cobra.Command{
		Use:     "describe [TOPIC:P...]",
		Aliases: []string{"d"},
		Short:   "Describe log directories for topic partitions.",
		Long: `Describe log directories for topic partitions.

Requires Kafka 1.0.0+.

Log directories are partition specific. The size of a directory is the absolute
size of log segments of a partition, in bytes.

Offset lag is how far behind the log end offset is compared to the partition's
high watermark, or, if this dir is a "future" directory, how far behind
compared to the current replica's log end offset.

In math,

  OffsetLag = isFuture
              ? localLogEndOffset - futureLogEndOffset
              : max(localHighWaterMark - logEndOffset, 0)


A directory is a "future" directory if it was created with an alter command and
will replace the replica's current log directory in the future.

TOTAL and USABLE are the size and the free space of the volume the directory
lives on, and require Kafka 3.3+. They cover local storage only: whatever the
directory has tiered to remote storage is not counted. CORDONED is whether the
broker has cordoned the directory, and requires Kafka 4.3+. A size a broker
does not report prints as -. -H prints sizes as KB, MB, and GB in text; JSON
and awk always carry the bytes.

Input format is topic:1,2,3.

Alternatively, if you just specify a topic, this will describe all partitions
for that topic.

By default, this command will return log dirs for the partition leaders.

If describing everything, this will merge all in sync replicas into the same
response.

You can direct this request to a specific broker with --broker, which is the
broker to ask, not the broker to describe: it lets you ask a follower about
its replicas rather than the leader. Rows are sorted by broker, dir, topic,
and partition, or by size with --sort-by-size. A broker or directory that
errored is one row with ERROR set.

--aggregate-into sums sizes by broker, dir, or topic and prints that one
column and SIZE instead.

EXAMPLES:
  kcl logdirs describe foo:1,2,3 bar:3,4,5
  kcl logdirs describe foo
  kcl logdirs describe                        # describes all
  kcl logdirs describe --aggregate-into topic # bytes per topic

SEE ALSO:
  kcl logdirs alter    move replicas between directories
`,

		RunE: func(_ *cobra.Command, topics []string) error {
			if aggregateInto != "" && aggregateHeaders[aggregateInto] == nil {
				return out.Errf(out.ExitUsage, "--aggregate-into must be broker, dir, or topic")
			}
			var req kmsg.DescribeLogDirsRequest
			if topics != nil {
				tps, err := flagutil.ParseTopicPartitions(topics)
				if err != nil {
					return out.Errf(out.ExitUsage, "improper topic partitions format on: %v", err)
				}

				// For any topic that has no partitions
				// specified, we describe *all* partitions.
				metaReq := kmsg.NewMetadataRequest()
				for topic, partitions := range tps {
					if len(partitions) == 0 {
						metaReqTopic := kmsg.NewMetadataRequestTopic()
						t := topic
						metaReqTopic.Topic = &t
						metaReq.Topics = append(metaReq.Topics, metaReqTopic)
					}
				}
				if len(metaReq.Topics) > 0 {
					metaResp, err := metaReq.RequestWith(context.Background(), cl.Client())
					if err != nil {
						return fmt.Errorf("unable to request metadata: %v", err)
					}
					for _, topic := range metaResp.Topics {
						if topic.Topic == nil {
							return fmt.Errorf("metadata returned nil topic when we did not fetch with topic IDs")
						}
						for _, partition := range topic.Partitions {
							tps[*topic.Topic] = append(tps[*topic.Topic], partition.Partition)
						}
					}
				}

				for topic, partitions := range tps {
					req.Topics = append(req.Topics, kmsg.DescribeLogDirsRequestTopic{
						Topic:      topic,
						Partitions: partitions,
					})
				}
			}

			var kresps []kgo.ResponseShard
			if broker >= 0 {
				kresp, err := cl.Client().Broker(int(broker)).Request(context.Background(), &req)
				kresps = []kgo.ResponseShard{{Meta: kgo.BrokerMetadata{NodeID: broker}, Resp: kresp, Err: err}}
			} else {
				kresps = cl.Client().RequestSharded(context.Background(), &req)
			}

			// total, usable, and cordoned describe the whole directory
			// rather than the partition, so every row in a directory
			// repeats them.
			type logdirRow struct {
				broker    int32
				dir       string
				topic     string
				partition int32
				size      int64
				offsetLag int64
				isFuture  bool
				total     int64
				usable    int64
				cordoned  bool
				err       error
			}
			var rows []logdirRow

			for _, kresp := range kresps {
				if kresp.Err != nil {
					rows = append(rows, logdirRow{broker: kresp.Meta.NodeID, err: kresp.Err})
					continue
				}
				resp := kresp.Resp.(*kmsg.DescribeLogDirsResponse)
				for _, dir := range resp.Dirs {
					if dir.ErrorCode != 0 {
						rows = append(rows, logdirRow{broker: kresp.Meta.NodeID, dir: dir.Dir, err: kerr.TypedErrorForCode(dir.ErrorCode)})
						continue
					}
					for _, topic := range dir.Topics {
						for _, partition := range topic.Partitions {
							rows = append(rows, logdirRow{
								broker:    kresp.Meta.NodeID,
								dir:       dir.Dir,
								topic:     topic.Topic,
								partition: partition.Partition,
								size:      partition.Size,
								offsetLag: partition.OffsetLag,
								isFuture:  partition.IsFuture,
								total:     dir.TotalBytes,
								usable:    dir.UsableBytes,
								cordoned:  dir.IsCordoned,
							})
						}
					}
				}
			}

			if sortBySize {
				sort.Slice(rows, func(i, j int) bool { return rows[i].size > rows[j].size })
			} else {
				sort.Slice(rows, func(i, j int) bool {
					if rows[i].broker != rows[j].broker {
						return rows[i].broker < rows[j].broker
					}
					if rows[i].dir != rows[j].dir {
						return rows[i].dir < rows[j].dir
					}
					if rows[i].topic != rows[j].topic {
						return rows[i].topic < rows[j].topic
					}
					return rows[i].partition < rows[j].partition
				})
			}

			// -H is for people; a script always gets the bytes.
			human := humanReadable && cl.Format() == out.FormatText

			// Aggregate mode: sum sizes by broker, dir, or topic.
			if aggregateInto != "" {
				type aggEntry struct {
					key  string
					size int64
				}
				agg := make(map[string]int64)
				for _, r := range rows {
					if r.err != nil {
						continue
					}
					var key string
					switch aggregateInto {
					case "broker":
						key = fmt.Sprintf("%d", r.broker)
					case "dir":
						key = fmt.Sprintf("%d:%s", r.broker, r.dir)
					case "topic":
						key = r.topic
					}
					agg[key] += r.size
				}
				var entries []aggEntry
				for k, v := range agg {
					entries = append(entries, aggEntry{k, v})
				}
				sort.Slice(entries, func(i, j int) bool {
					if sortBySize {
						return entries[i].size > entries[j].size
					}
					return entries[i].key < entries[j].key
				})
				aggTable := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "dirs", aggregateHeaders[aggregateInto]...)
				for _, e := range entries {
					aggTable.Row(e.key, formatSize(e.size, human))
				}
				return aggTable.Flush()
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "dirs", describeHeaders...).ErrorColumn()
			for _, r := range rows {
				if r.err != nil {
					var dir any = out.Unknown
					if r.dir != "" {
						dir = r.dir
					}
					table.Row(r.broker, dir, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.ErrCell(r.err))
					continue
				}
				table.Row(r.broker, r.dir, r.topic, r.partition,
					formatSize(r.size, human), r.offsetLag, r.isFuture,
					formatSize(r.total, human), formatSize(r.usable, human), r.cordoned, "")
			}
			return table.Flush()
		},
	}
	out.ColumnsFunc(cmd, func() []string {
		if h := aggregateHeaders[aggregateInto]; h != nil {
			return h
		}
		return describeHeaders
	})

	cmd.Flags().Int32VarP(&broker, "broker", "b", -1, "a specific broker to direct the request to")
	cmd.Flags().BoolVarP(&humanReadable, "human-readable", "H", false, "print sizes as KB, MB, and GB in text")
	cmd.Flags().BoolVar(&sortBySize, "sort-by-size", false, "sort output by partition size (largest first)")
	cmd.Flags().StringVar(&aggregateInto, "aggregate-into", "", "aggregate sizes by dimension (broker, dir, topic)")
	return cmd
}

func alterReplicasCommand(cl *client.Client) *cobra.Command {
	var broker int32
	cmd := &cobra.Command{
		Use:   "alter TOPIC:P=DIR...",
		Short: "Move topic replicas to a destination directory.",
		Long: `Move topic replicas to a destination directory.

Move topic partitions to specified directories (Kafka 1.0.0+).

Introduced in Kafka 1.0.0, this command allows for moving replica log
directories. See KIP-113 for the motivation.

The input syntax is topic:1,2,3=/destination/directory.

By default, this command will alter log dirs for the partition leaders.
You can direct this request to specific brokers with the --broker argument,
which allows you to alter replicas.

The result prints one row per partition with ERROR and MESSAGE.

EXAMPLES:
  kcl logdirs alter foo:1,2,3=/dir bar:6=/dir2 baz:9=/dir

SEE ALSO:
  kcl logdirs describe    describe log directories
`,

		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, topics []string) error {
			dests := make(map[string]map[string][]int32)
			for _, topic := range topics {
				parts := strings.Split(topic, "=")
				if len(parts) != 2 {
					return out.Errf(out.ExitUsage, "improper format %q: want TOPIC:P=DIR", topic)
				}
				tps, err := flagutil.ParseTopicPartitions([]string{parts[0]})
				if err != nil {
					return out.Errf(out.ExitUsage, "improper topic partitions format on %q: %v", parts[0], err)
				}
				dest := parts[1]
				existing := dests[dest]
				if existing == nil {
					dests[dest] = make(map[string][]int32)
				}
				for topic, parts := range tps {
					dests[dest][topic] = append(dests[dest][topic], parts...)
				}
			}

			var req kmsg.AlterReplicaLogDirsRequest
			for dest, tps := range dests {
				reqDest := kmsg.AlterReplicaLogDirsRequestDir{
					Dir: dest,
				}
				for topic, partitions := range tps {
					reqDest.Topics = append(reqDest.Topics, kmsg.AlterReplicaLogDirsRequestDirTopic{
						Topic:      topic,
						Partitions: partitions,
					})
				}
				req.Dirs = append(req.Dirs, reqDest)
			}

			var kresp kmsg.Response
			var err error
			if broker >= 0 {
				kresp, err = cl.Client().Broker(int(broker)).Request(context.Background(), &req)
			} else {
				kresp, err = cl.Client().Request(context.Background(), &req)
			}
			if err != nil {
				return fmt.Errorf("unable to alter replica log dirs: %v", err)
			}

			resp := kresp.(*kmsg.AlterReplicaLogDirsResponse)
			type row struct {
				topic     string
				partition int32
				err       string
			}
			var rows []row
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					rows = append(rows, row{topic.Topic, partition.Partition, out.ErrName(partition.ErrorCode)})
				}
			}
			sort.Slice(rows, func(i, j int) bool {
				if rows[i].topic != rows[j].topic {
					return rows[i].topic < rows[j].topic
				}
				return rows[i].partition < rows[j].partition
			})
			// The response carries no message, so MESSAGE is always
			// empty; it is there so that every mutation reads alike.
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", alterHeaders...).ResultColumns()
			for _, r := range rows {
				table.Row(r.topic, r.partition, r.err, "")
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, alterHeaders...)
	cmd.Flags().Int32VarP(&broker, "broker", "b", -1, "a specific broker to direct the request to")
	return cmd
}
