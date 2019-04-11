package main

import (
	"github.com/spf13/cobra"

	"github.com/twmb/kgo/kmsg"
)

func init() {
	root.AddCommand(logdirsCmd())
}

func logdirsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logdirs",
		Short: "Perform log directory related actions",
	}

	cmd.AddCommand(logdirsDescribeCmd())
	cmd.AddCommand(logdirsAlterReplicasCmd())

	return cmd
}

func logdirsDescribeCmd() *cobra.Command {
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

			kresp, err := client.Request(&req)
			maybeDie(err, "unable to describe log dirs: %v", err)

			dumpJSON(kresp)
		},
	}

	cmd.Flags().StringVarP(&topic, "topic", "t", "", "topic to describe the log dirs of; otherwise all topics")
	cmd.Flags().Int32SliceVarP(&partitions, "partitions", "p", nil, "partitions within an individual topic to describe the logs dirs of")

	return cmd
}

func logdirsAlterReplicasCmd() *cobra.Command {
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
			resp, err := client.Request(&kmsg.AlterReplicaLogDirsRequest{
				LogDirs: []kmsg.AlterReplicaLogDirsRequestLogDir{{
					LogDir: destDir,
					Topics: []kmsg.AlterReplicaLogDirsRequestLogDirTopic{{
						Topic:      topic,
						Partitions: partitions,
					}},
				}},
			})

			maybeDie(err, "unable to alter replica log dirs: %v", err)
			dumpJSON(resp)
		},
	}

	cmd.Flags().StringVarP(&destDir, "dest", "d", "", "destination directory to move a topic's partitions to")
	cmd.Flags().StringVarP(&topic, "topic", "t", "", "topic whose partitions need moving")
	cmd.Flags().Int32SliceVarP(&partitions, "partitions", "p", nil, "partitions to move")

	return cmd
}
