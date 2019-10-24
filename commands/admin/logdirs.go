package admin

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"
	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

func logdirsDescribeCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "describe-log-dirs",
		Short: "Describe log directories for topic partitions.",
		Long: `Describe log directories for topic partitions.

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

Input format is topic:1,2,3.
`,

		Example: `describe-log-dirs foo:1,2,3

describe-log-dirs // describes all`,

		Run: func(_ *cobra.Command, topics []string) {
			var req kmsg.DescribeLogDirsRequest
			if topics != nil {
				tps, err := flagutil.ParseTopicPartitions(topics)
				out.MaybeDie(err, "improper topic partitions format on: %v", err)
				for topic, partitions := range tps {
					req.Topics = append(req.Topics, kmsg.DescribeLogDirsRequestTopic{
						Topic:      topic,
						Partitions: partitions,
					})
				}
			}

			kresp, err := cl.Client().Request(context.Background(), &req)
			out.MaybeDie(err, "unable to describe log dirs: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}
			resp := kresp.(*kmsg.DescribeLogDirsResponse)

			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "DIR\tDIR ERR\tTOPIC\tPARTITION\tSIZE\tOFFSET LAG\tIS FUTURE\n")
			for _, dir := range resp.Dirs {
				if err := kerr.ErrorForCode(dir.ErrorCode); err != nil {
					fmt.Fprintf(tw, "%s\t%s\t\t\t\t\t\n", dir.Dir, err.Error())
					continue
				}
				for _, topic := range dir.Topics {
					for _, partition := range topic.Partitions {
						fmt.Fprintf(tw, "%s\t\t%s\t%d\t%d\t%d\t%v\n",
							dir.Dir,
							topic.Topic,
							partition.Partition,
							partition.Size,
							partition.OffsetLag,
							partition.IsFuture,
						)
					}
				}
			}

		},
	}
}

func logdirsAlterReplicasCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "alter-replica-log-dirs",
		Short: "Move topic replicas to a destination directory",
		Long: `Move topic partitions to specified directories.

Introduced in Kafka 1.0.0, this command allows for moving replica log
directories. See KIP-113 for the motivation.

The input syntax is topic:1,2,3=/destination/directory.
`,

		Example: `alter-replica-log-dirs foo:1,2,3=/dir bar:6=/dir2 baz:9=/dir`,

		Run: func(_ *cobra.Command, topics []string) {
			dests := make(map[string]map[string][]int32)
			for _, topic := range topics {
				parts := strings.Split(topic, "=")
				if len(parts) != 2 {
					out.Die("improper format for dest-dir = split (expected two strings after split, got %d)", len(parts))
				}
				tps, err := flagutil.ParseTopicPartitions([]string{parts[0]})
				out.MaybeDie(err, "improper topic partitions format on %q: %v", parts[0], err)
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

			kresp, err := cl.Client().Request(context.Background(), &req)
			out.MaybeDie(err, "unable to alter replica log dirs: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			resp := kresp.(*kmsg.AlterReplicaLogDirsResponse)
			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "TOPIC\tPARTITION\tERROR\n")
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					msg := ""
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						msg = err.Error()
					}
					fmt.Fprintf(tw, "%s\t%d\t%s\n",
						topic.Topic,
						partition.Partition,
						msg,
					)
				}
			}
		},
	}
}
