package sharegroup

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

func describeOffsetsCommand(cl *client.Client) *cobra.Command {
	var topicParts []string
	cmd := &cobra.Command{
		Use:     "describe-offsets GROUPS...",
		Aliases: []string{"do"},
		Short:   "Describe share group offsets (Kafka 4.0+).",
		Long: `Describe share group offsets (KIP-932, Kafka 4.0+).

This command describes the start offsets for share group topic partitions. If
no groups are provided, all share groups are listed and described.

Use -t to optionally filter by topic and partitions. The format is
"foo:1,2,3" where foo is a topic and 1,2,3 are partitions. If no partitions
are given (just "foo"), all partitions for that topic are described.
`,
		Example: "describe-offsets mygroup -t foo:1,2,3 -t bar",
		Run: func(_ *cobra.Command, groups []string) {
			if len(groups) == 0 {
				groups = listShareGroups(cl)
			}
			if len(groups) == 0 {
				out.Die("no share groups to describe offsets for")
			}

			var topics []kmsg.DescribeShareGroupOffsetsRequestGroupTopic
			if len(topicParts) > 0 {
				tps, err := flagutil.ParseTopicPartitions(topicParts)
				out.MaybeDie(err, "unable to parse topic partitions: %v", err)
				for topic, partitions := range tps {
					topics = append(topics, kmsg.DescribeShareGroupOffsetsRequestGroupTopic{
						Topic:      topic,
						Partitions: partitions,
					})
				}
			}

			req := kmsg.NewPtrDescribeShareGroupOffsetsRequest()
			for _, g := range groups {
				req.Groups = append(req.Groups, kmsg.DescribeShareGroupOffsetsRequestGroup{
					GroupID: g,
					Topics:  topics,
				})
			}

			kresp, err := req.RequestWith(context.Background(), cl.Client())
			out.MaybeDie(err, "unable to describe share group offsets: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			for _, group := range kresp.Groups {
				fmt.Printf("GROUP: %s\n", group.GroupID)
				if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
					msg := err.Error()
					if group.ErrorMessage != nil {
						msg += ": " + *group.ErrorMessage
					}
					fmt.Printf("  ERROR: %s\n", msg)
					continue
				}

				tw := out.NewTable("TOPIC", "PARTITION", "START-OFFSET", "LEADER-EPOCH", "LAG", "ERROR")
				for _, topic := range group.Topics {
					for _, partition := range topic.Partitions {
						errMsg := ""
						if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
							errMsg = err.Error()
							if partition.ErrorMessage != nil {
								errMsg += ": " + *partition.ErrorMessage
							}
						}
						lag := "-"
						if partition.Lag >= 0 {
							lag = strconv.FormatInt(partition.Lag, 10)
						}
						tw.Print(
							topic.Topic,
							partition.Partition,
							partition.StartOffset,
							partition.LeaderEpoch,
							lag,
							errMsg,
						)
					}
				}
				tw.Flush()
				fmt.Println()
			}
		},
	}
	cmd.Flags().StringArrayVarP(&topicParts, "topic", "t", nil, "topic and optional partitions to describe offsets for (e.g. foo:1,2,3 or foo); repeatable")
	return cmd
}

func alterOffsetsCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "alter-offsets GROUP TOPIC-PARTITION-OFFSETS...",
		Short: "Alter share group start offsets (Kafka 4.0+).",
		Long: `Alter share group start offsets (KIP-932, Kafka 4.0+).

The group must be empty (no active consumers). The format for specifying
topic partition offsets is "foo:p#,o#" where foo is a topic, # after p is
the partition number, and # after o is the new start offset.

Multiple partitions for the same topic can be specified by repeating the
topic:p#,o# argument.
`,
		Example: "alter-offsets mygroup foo:p0,o100 foo:p1,o200 bar:p0,o50",
		Args:    cobra.MinimumNArgs(2),
		Run: func(_ *cobra.Command, args []string) {
			group := args[0]
			tpos, err := parseTopicPartitionOffsets(args[1:])
			out.MaybeDie(err, "unable to parse topic partition offsets: %v", err)

			req := kmsg.NewPtrAlterShareGroupOffsetsRequest()
			req.GroupID = group
			for topic, pos := range tpos {
				rt := kmsg.NewAlterShareGroupOffsetsRequestTopic()
				rt.Topic = topic
				for _, po := range pos {
					rp := kmsg.NewAlterShareGroupOffsetsRequestTopicPartition()
					rp.Partition = po.partition
					rp.StartOffset = po.offset
					rt.Partitions = append(rt.Partitions, rp)
				}
				req.Topics = append(req.Topics, rt)
			}

			kresp, err := req.RequestWith(context.Background(), cl.Client())
			out.MaybeDie(err, "unable to alter share group offsets: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			if err := kerr.ErrorForCode(kresp.ErrorCode); err != nil {
				msg := err.Error()
				if kresp.ErrorMessage != nil {
					msg += ": " + *kresp.ErrorMessage
				}
				out.Die(msg)
			}

			tw := out.NewTable("TOPIC", "PARTITION", "ERROR")
			for _, topic := range kresp.Topics {
				for _, partition := range topic.Partitions {
					errMsg := "OK"
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						errMsg = err.Error()
						if partition.ErrorMessage != nil {
							errMsg += ": " + *partition.ErrorMessage
						}
					}
					tw.Print(topic.Topic, partition.Partition, errMsg)
				}
			}
			tw.Flush()
		},
	}
	return cmd
}

func deleteOffsetsCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete-offsets GROUP TOPICS...",
		Short: "Delete share group offsets for topics (Kafka 4.0+).",
		Long: `Delete share group offsets for topics (KIP-932, Kafka 4.0+).

The group must be empty (no active consumers). This deletes all offset state
for the specified topics within the share group.
`,
		Example: "delete-offsets mygroup foo bar",
		Args:    cobra.MinimumNArgs(2),
		Run: func(_ *cobra.Command, args []string) {
			group := args[0]
			topics := args[1:]

			req := kmsg.NewPtrDeleteShareGroupOffsetsRequest()
			req.GroupID = group
			for _, t := range topics {
				rt := kmsg.NewDeleteShareGroupOffsetsRequestTopic()
				rt.Topic = t
				req.Topics = append(req.Topics, rt)
			}

			kresp, err := req.RequestWith(context.Background(), cl.Client())
			out.MaybeDie(err, "unable to delete share group offsets: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			if err := kerr.ErrorForCode(kresp.ErrorCode); err != nil {
				msg := err.Error()
				if kresp.ErrorMessage != nil {
					msg += ": " + *kresp.ErrorMessage
				}
				out.Die(msg)
			}

			tw := out.NewTable("TOPIC", "ERROR")
			for _, topic := range kresp.Topics {
				errMsg := "OK"
				if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
					errMsg = err.Error()
					if topic.ErrorMessage != nil {
						errMsg += ": " + *topic.ErrorMessage
					}
				}
				tw.Print(topic.Topic, errMsg)
			}
			tw.Flush()
		},
	}
	return cmd
}

type partitionOffset struct {
	partition int32
	offset    int64
}

func parseTopicPartitionOffsets(list []string) (map[string][]partitionOffset, error) {
	rePo := regexp.MustCompile(`^p(\d+),o(-?\d+)$`)

	tpos := make(map[string][]partitionOffset)
	for _, item := range list {
		split := strings.SplitN(item, ":", 2)
		if len(split[0]) == 0 {
			return nil, fmt.Errorf("item %q invalid empty topic", item)
		}
		if len(split) == 1 {
			return nil, fmt.Errorf("item %q missing partition offset (expected topic:p#,o#)", item)
		}

		matches := rePo.FindStringSubmatch(split[1])
		if len(matches) == 0 {
			return nil, fmt.Errorf("item %q partition offset %q does not match p(\\d+),o(-?\\d+)", item, split[1])
		}
		partition, _ := strconv.Atoi(matches[1])
		offset, _ := strconv.Atoi(matches[2])

		tpos[split[0]] = append(tpos[split[0]], partitionOffset{int32(partition), int64(offset)})
	}
	return tpos, nil
}
