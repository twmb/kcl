// Package logdirs contains logdir related subcommands.
package logdirs

import (
	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
	"github.com/twmb/kgo/kmsg"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logdirs",
		Short: "Perform log directory related actions",
	}

	cmd.AddCommand(logdirsDescribeCommand(cl))
	cmd.AddCommand(logdirsAlterReplicasCommand(cl))

	return cmd
}

func logdirsDescribeCommand(cl *client.Client) *cobra.Command {
	var topic string
	var partitions []int32

	cmd := &cobra.Command{
		Use:   "describe",
		Short: "Describe log directories for all topics or an individual topic",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			var req kmsg.DescribeLogDirsRequest

			if topic != "" {
				req.Topics = append(req.Topics, kmsg.DescribeLogDirsRequestTopic{
					Topic:      topic,
					Partitions: partitions,
				})
			}

			kresp, err := cl.Client().Request(&req)
			out.MaybeDie(err, "unable to describe log dirs: %v", err)

			out.DumpJSON(kresp)
		},
	}

	cmd.Flags().StringVarP(&topic, "topic", "t", "", "topic to describe the log dirs of; otherwise all topics")
	cmd.Flags().Int32SliceVarP(&partitions, "partitions", "p", nil, "partitions within an individual topic to describe the logs dirs of (comma separated or repeated flag)")

	return cmd
}

func logdirsAlterReplicasCommand(cl *client.Client) *cobra.Command {
	var destDir string
	var topic string
	var partitions []int32

	cmd := &cobra.Command{
		Use:   "alter-replicas",
		Short: "Move topic replicas to a destination directory",
		ValidArgs: []string{
			"--dest",
			"--topic",
			"--partitions",
		},
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			resp, err := cl.Client().Request(&kmsg.AlterReplicaLogDirsRequest{
				LogDirs: []kmsg.AlterReplicaLogDirsRequestLogDir{{
					LogDir: destDir,
					Topics: []kmsg.AlterReplicaLogDirsRequestLogDirTopic{{
						Topic:      topic,
						Partitions: partitions,
					}},
				}},
			})

			out.MaybeDie(err, "unable to alter replica log dirs: %v", err)
			out.DumpJSON(resp)
		},
	}

	cmd.Flags().StringVarP(&destDir, "dest", "d", "", "destination directory to move a topic's partitions to")
	cmd.Flags().StringVarP(&topic, "topic", "t", "", "topic whose partitions need moving")
	cmd.Flags().Int32SliceVarP(&partitions, "partitions", "p", nil, "partitions to move (comma separated or repeated flag)")

	return cmd
}
