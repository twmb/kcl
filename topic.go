package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

func init() {
	root.AddCommand(topicCmd())
}

func topicCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic",
		Short: "Perform topic relation actions",
	}

	cmd.AddCommand(topicListCommand())
	cmd.AddCommand(topicCreateCmd())
	cmd.AddCommand(topicDeleteCmd())
	cmd.AddCommand(topicDescribeConfigCmd())
	cmd.AddCommand(topicAlterConfigCmd())
	cmd.AddCommand(topicAddPartitionsCmd())
	return cmd
}

func topicListCommand() *cobra.Command {
	var detailed bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all topics",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			kresp, err := client().Request(new(kmsg.MetadataRequest))
			maybeDie(err, "unable to get metadata: %v", err)
			resp := kresp.(*kmsg.MetadataResponse)

			if asJSON {
				dumpJSON(resp.TopicMetadata)
				return
			}

			printTopics(resp.TopicMetadata, detailed)
		},
	}

	cmd.Flags().BoolVarP(&detailed, "detailed", "d", false, "include detailed information about all topic partitions")
	return cmd
}

func printTopics(topics []kmsg.MetadataResponseTopicMetadata, detailed bool) {
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Topic < topics[j].Topic
	})

	if !detailed {
		fmt.Fprintf(tw, "NAME\tPARTITIONS\tREPLICAS\n")
		for _, topic := range topics {
			parts := len(topic.PartitionMetadata)
			replicas := 0
			if parts > 0 {
				replicas = len(topic.PartitionMetadata[0].Replicas)
			}
			fmt.Fprintf(tw, "%s\t%d\t%d\n",
				topic.Topic, parts, replicas)
		}
		tw.Flush()
		return
	}

	buf := new(bytes.Buffer)
	buf.Grow(10 << 10)
	defer func() { os.Stdout.Write(buf.Bytes()) }()

	for _, topic := range topics {
		fmt.Fprintf(buf, "%s", topic.Topic)
		if topic.IsInternal {
			fmt.Fprint(buf, " (internal)")
		}

		parts := topic.PartitionMetadata
		fmt.Fprintf(buf, ", %d partition", len(parts))
		if len(parts) > 1 {
			buf.WriteByte('s')
		}
		buf.WriteString("\n")

		sort.Slice(parts, func(i, j int) bool {
			return parts[i].Partition < parts[j].Partition
		})
		for _, part := range topic.PartitionMetadata {
			fmt.Fprintf(buf, "  %4d  leader %d replicas %v isr %v",
				part.Partition,
				part.Leader,
				part.Replicas,
				part.ISR,
			)
			if len(part.OfflineReplicas) > 0 {
				fmt.Fprintf(buf, ", offline replicas %v", part.OfflineReplicas)
			}
			if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
				fmt.Fprintf(buf, " (%s)", err)
			}
			fmt.Fprintln(buf)
		}
	}
}

func topicCreateCmd() *cobra.Command {
	req := kmsg.CreateTopicsRequest{Timeout: cfg.TimeoutMillis}

	var topicReq kmsg.CreateTopicsRequestTopic
	var configKVs []string

	cmd := &cobra.Command{
		Use:   "create TOPIC",
		Short: "Create a topic",
		ValidArgs: []string{
			"--num-partitions",
			"--replication-factor",
			"--kv",
			"--validate",
		},
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			topicReq.Topic = args[0]

			kvs, err := parseKVs(configKVs)
			maybeDie(err, "unable to parse KVs: %v", err)
			for _, kv := range kvs {
				topicReq.ConfigEntries = append(topicReq.ConfigEntries,
					kmsg.CreateTopicsRequestTopicConfigEntry{
						ConfigName:  kv.k,
						ConfigValue: kmsg.StringPtr(kv.v),
					})
			}

			req.Topics = append(req.Topics, topicReq)

			kresp, err := client().Request(&req)
			maybeDie(err, "unable to create topic %q: %v", args[0], err)
			if asJSON {
				dumpJSON(kresp)
				return
			}
			resps := kresp.(*kmsg.CreateTopicsResponse).TopicErrors
			if len(resps) != 1 {
				dumpAndDie(kresp, "quitting; one topic create requested but received %d responses", len(resps))
			}
			errAndMsg(resps[0].ErrorCode, resps[0].ErrorMessage)
		},
	}

	cmd.Flags().BoolVarP(&req.ValidateOnly, "validate", "v", false, "(v)alidate the topic creation request; do not create topics (dry run)")
	cmd.Flags().Int32VarP(&topicReq.NumPartitions, "num-partitions", "p", 1, "number of (p)artitions to create")
	cmd.Flags().Int16VarP(&topicReq.ReplicationFactor, "replication-factor", "r", 1, "number of (r)eplicas to have of each partition")
	cmd.Flags().StringSliceVarP(&configKVs, "kv", "k", nil, "list of (k)ey value config parameters (comma separated or repeated flag; e.g. cleanup.policy=compact,preallocate=true)")

	return cmd
}

func topicDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete TOPICS...",
		Short: "Delete all listed topics",
		Run: func(_ *cobra.Command, args []string) {
			resp, err := client().Request(&kmsg.DeleteTopicsRequest{
				Timeout: cfg.TimeoutMillis,
				Topics:  args,
			})
			maybeDie(err, "unable to delete topics: %v", err)
			if asJSON {
				dumpJSON(resp)
				return
			}
			resps := resp.(*kmsg.DeleteTopicsResponse).TopicErrorCodes
			if len(resps) != len(args) {
				dumpAndDie(resp, "quitting; %d topic deletions requested but received %d responses", len(resps))
			}
			for i := 0; i < len(resps); i++ {
				msg := "OK"
				if err := kerr.ErrorForCode(resps[i].ErrorCode); err != nil {
					msg = err.Error()
				}
				fmt.Fprintf(tw, "%s\t%s\n", resps[i].Topic, msg)
			}
			tw.Flush()
		},
	}
}

func topicDescribeConfigCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "describe TOPIC",
		Short: "Describe a topic's configuration",
		Long: `Print key/value config pairs for a topic.

This command prints all key/value config values for a topic, as well
as the value's source. Read only keys are suffixed with *.

The JSON output option includes all config synonyms.
`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			describeConfig(args, false)
		},
	}
}

func topicAlterConfigCmd() *cobra.Command {
	return alterConfigsCmd(
		"alter-config TOPIC",
		"Alter a topic's configuration",
		`Alter a topic's configuration.

This command supports JSON output.
`,
		false,
	)
}

func topicAddPartitionsCmd() *cobra.Command {
	var assignmentFlag string

	cmd := &cobra.Command{
		Use:   "add-partitions TOPIC",
		Short: "Add partitions to a topic",
		Long: `Add partitions to a topic.

Adding partitions to topics is done by requesting a total amount of
partitions for a topic combined with an assignment of which brokers
should own the replicas of each new partition.

This CLI handles the total count of final partitions; you just need
to specify the where new partitions and their replicas should go.

The input format is r,r,r;r,r,r. Each semicolon delimits a new partition.
This command strips space. Note that because the partition delimiter is
a semicolon, the input will need quoting if using multiple partitions
(or if using spaces).

To add a single new partition with three replicas, you would do 1,2,3.

To add three partitions with two replicas each, you would do '1,2; 3,1; 2,1'.


This command supports JSON output.
`,
		ValidArgs: []string{
			"--assignments",
		},
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			assignments, err := parseAssignments(assignmentFlag)
			maybeDie(err, "parse assignments failure: %v", err)
			if len(assignments) == 0 {
				die("no new partitions requested")
			}

			kmetaResp, err := client().Request(&kmsg.MetadataRequest{
				Topics: args,
			})
			maybeDie(err, "unable to get topic metadata: %v", err)
			metas := kmetaResp.(*kmsg.MetadataResponse).TopicMetadata
			if len(metas) != 1 {
				die("quitting; one metadata topic requested but received %d responses", len(metas))
			}
			if metas[0].Topic != args[0] {
				die("quitting; metadata responded with non-requested topic %s", metas[0].Topic)
			}
			if err := kerr.ErrorForCode(metas[0].ErrorCode); err != nil {
				die("quitting; metadata responded with topic error %s", err)
			}

			currentPartitionCount := len(metas[0].PartitionMetadata)
			if currentPartitionCount > 0 {
				currentReplicaCount := len(metas[0].PartitionMetadata[0].Replicas)
				if currentReplicaCount != len(assignments[0]) {
					die("cannot create partitions with %d when existing partitions have %d replicas",
						len(assignments[0]), currentReplicaCount)
				}
			}

			createResp, err := client().Request(&kmsg.CreatePartitionsRequest{
				TopicPartitions: []kmsg.CreatePartitionsRequestTopicPartition{
					{
						Topic: args[0],
						NewPartitions: kmsg.CreatePartitionsRequestTopicPartitionNewPartitions{
							Count:      int32(currentPartitionCount + len(assignments)),
							Assignment: assignments,
						},
					},
				},
				Timeout: cfg.TimeoutMillis,
			})
			maybeDie(err, "unable to create topic partitions: %v", err)

			if asJSON {
				dumpJSON(createResp)
				return
			}
			resps := createResp.(*kmsg.CreatePartitionsResponse).TopicErrors
			if len(resps) != 1 {
				dumpAndDie(createResp, "quitting; one topic partition creation requested but received %d responses", len(resps))
			}
			errAndMsg(resps[0].ErrorCode, resps[0].ErrorMessage)
		},
	}

	cmd.Flags().StringVarP(&assignmentFlag, "assignments", "a", "", "assignment of new semicolon delimited partitions and their comma delimited replicas to broker IDs")

	return cmd
}

func parseAssignments(in string) ([][]int32, error) {
	var partitions [][]int32
	var replicasSize int

	for _, partition := range strings.Split(in, ";") {
		partition = strings.TrimSpace(partition)
		if len(partition) == 0 {
			continue
		}

		var replicas []int32
		for _, replica := range strings.Split(partition, ",") {
			replica = strings.TrimSpace(replica)
			if len(replica) == 0 {
				continue
			}

			r, err := strconv.Atoi(replica)
			if err != nil {
				return nil, fmt.Errorf("unable to parse replica %s", replica)
			}

			for i := range replicas {
				if replicas[i] == int32(r) {
					return nil, errors.New("duplicate brokers not allowed in replica assignment")
				}
			}
			replicas = append(replicas, int32(r))
		}
		if len(replicas) == 0 {
			continue
		}

		if replicasSize == 0 {
			replicasSize = len(replicas)
		} else if len(replicas) != replicasSize {
			return nil, errors.New("all partitions must have the same number of replicas")
		}

		partitions = append(partitions, replicas)
	}

	return partitions, nil
}
