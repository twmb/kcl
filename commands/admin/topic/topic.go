// Package topic contains topic related utilities and subcommands.
package topic

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/commands/metadata"
	"github.com/twmb/kcl/kv"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "topic",
		Aliases: []string{"t"},
		Short:   "Perform topic relation actions (create, list, delete, add-partitions).",
	}

	cmd.AddCommand(topicCreateCommand(cl))
	cmd.AddCommand(topicListCommand(cl))
	cmd.AddCommand(topicDeleteCommand(cl))
	cmd.AddCommand(topicAddPartitionsCommand(cl))
	return cmd
}

func topicCreateCommand(cl *client.Client) *cobra.Command {
	var (
		numPartitions     int32
		replicationFactor int16
		configKVs         []string
		validateOnly      bool
	)

	cmd := &cobra.Command{
		Use:     "create TOPICS",
		Aliases: []string{"c"},
		Short:   "Create topics",
		Long: `Create topics (Kafka 0.10.1+).

All topics created with this command will have the same number of partitions,
replication factor, and key/value configs.
`,
		Args: cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			kvs, err := kv.Parse(configKVs)
			out.MaybeDie(err, "unable to parse KVs: %v", err)
			req := kmsg.CreateTopicsRequest{TimeoutMillis: cl.TimeoutMillis()}
			req.ValidateOnly = validateOnly
			var configs []kmsg.CreateTopicsRequestTopicConfig
			for _, kv := range kvs {
				configs = append(configs, kmsg.CreateTopicsRequestTopicConfig{
					Name:  kv.K,
					Value: kmsg.StringPtr(kv.V),
				})
			}

			for _, topic := range args {
				req.Topics = append(req.Topics, kmsg.CreateTopicsRequestTopic{
					Topic:             topic,
					ReplicationFactor: replicationFactor,
					NumPartitions:     numPartitions,
					Configs:           configs,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), &req)
			out.MaybeDie(err, "unable to create topic %q: %v", args[0], err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			resp := kresp.(*kmsg.CreateTopicsResponse)
			tw := out.BeginTabWrite()
			defer tw.Flush()
			if resp.Version >= 7 {
				fmt.Fprintf(tw, "NAME\tID\tMESSAGE\n")
			} else {
				fmt.Fprintf(tw, "NAME\tMESSAGE\n")
			}
			for _, topic := range resp.Topics {
				msg := "OK"
				if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
					msg = err.Error()
				}
				if resp.Version >= 7 {
					fmt.Fprintf(tw, "%s\t%x\t%s\n", topic.Topic, topic.TopicID, msg)
				} else {
					fmt.Fprintf(tw, "%s\t%s\n", topic.Topic, msg)
				}
			}
		},
	}

	cmd.Flags().BoolVarP(&validateOnly, "dry", "d", false, "dry run: validate the topic creation request; do not create topics (Kafka 0.10.2+)")
	cmd.Flags().Int32VarP(&numPartitions, "num-partitions", "p", 20, "number of partitions to create")
	cmd.Flags().Int16VarP(&replicationFactor, "replication-factor", "r", 1, "number of replicas to have of each partition")
	cmd.Flags().StringArrayVarP(&configKVs, "kv", "k", nil, "list of key=value config parameters (repeatable, e.g. -k cleanup.policy=compact -k preallocate=true)")

	return cmd
}

func topicListCommand(cl *client.Client) *cobra.Command {
	var detailed bool

	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all topics",
		Long:    "List all topics (alias for metadata -t)",
		Run: func(_ *cobra.Command, _ []string) {
			req := kmsg.NewPtrMetadataRequest()
			resp, err := req.RequestWith(context.Background(), cl.Client())
			out.MaybeDie(err, "unable to list topics: %v", err)
			metadata.PrintTopics(resp.Version, resp.Topics, false, detailed)
		},
	}
	cmd.Flags().BoolVarP(&detailed, "detailed", "d", false, "include detailed information about all topic partitions")
	return cmd
}

func topicDeleteCommand(cl *client.Client) *cobra.Command {
	var ids bool
	cmd := &cobra.Command{
		Use:   "delete TOPICS...",
		Short: "Delete all listed topics (Kafka 0.10.1+).",
		Run: func(_ *cobra.Command, topics []string) {
			req := &kmsg.DeleteTopicsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
				TopicNames:    topics,
			}
			for _, topic := range topics {
				t := kmsg.NewDeleteTopicsRequestTopic()
				if ids {
					if len(topic) != 32 {
						out.Die("topic id %s is not a 32 byte hex string")
					}
					raw, err := hex.DecodeString(topic)
					out.MaybeDie(err, "topic id %s is not a hex string")
					copy(t.TopicID[:], raw)
				} else {
					t.Topic = kmsg.StringPtr(topic)
				}
				req.Topics = append(req.Topics, t)
			}

			resp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to delete topics: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(resp)
			}
			resps := resp.(*kmsg.DeleteTopicsResponse).Topics
			tw := out.BeginTabWrite()
			defer tw.Flush()
			for _, topicResp := range resps {
				msg := "OK"
				if err := kerr.ErrorForCode(topicResp.ErrorCode); err != nil {
					msg = err.Error()
				}
				topic := ""
				if topicResp.Topic != nil {
					topic = *topicResp.Topic
				} else {
					topic = fmt.Sprintf("%x", topicResp.TopicID)
				}
				fmt.Fprintf(tw, "%s\t%s\n", topic, msg)
			}
		},
	}
	cmd.Flags().BoolVar(&ids, "ids", false, "whether the input topics should be parsed as topic IDs")
	return cmd
}

func topicAddPartitionsCommand(cl *client.Client) *cobra.Command {
	var topics []string

	cmd := &cobra.Command{
		Use:   "add-partitions -t TOPIC ASSIGNMENTS",
		Short: "Add partitions to a topic",
		Long: `Add partitions to a topic (Kafka 1.0.0+).

As a client, adding partitions to topics is done by requesting a total amount
of partitions for a topic combined with an assignment of which brokers should
own the replicas of each new partition.

This CLI handles the total count of final partitions; you as a user just need
to specify the where new partitions and their replicas should go.

When adding partitions to multiple topics, all topics use the same assignment.

The assignment format is

  replica,replica : replica,replica
  \             /   \             /
   one partition     one partition
`,

		Example: `To add three partitions with two replicas each,
add-partitions -t foo 1,2 : 3,1 : 2,3

To add three partitions with one replica each,
add-partitions -t foo 1:2:3

To add a single partition with three replicas to two topics,
add-partitions -t bar -t baz 1, 2, 3`,

		Args: cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if len(topics) == 0 {
				out.Die("missing topics to add partitions to")
			}

			assignments, err := parseAssignments(strings.Join(args, ""))
			out.MaybeDie(err, "parse assignments failure: %v", err)
			if len(assignments) == 0 {
				out.Die("no new partitions requested")
			}

			// Get the metadata so we can determine the final partition count.
			metaReq := new(kmsg.MetadataRequest)
			for _, topic := range topics {
				t := topic
				metaReq.Topics = append(metaReq.Topics, kmsg.MetadataRequestTopic{Topic: &t})
			}
			kmetaResp, err := cl.Client().Request(context.Background(), metaReq)
			out.MaybeDie(err, "unable to get topic metadata: %v", err)
			metaResp := kmetaResp.(*kmsg.MetadataResponse)

			createReq := kmsg.CreatePartitionsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for _, topic := range metaResp.Topics {
				if topic.Topic == nil {
					out.Die("metadata returned nil topic, unknown topic ID!")
				}
				currentPartitionCount := len(topic.Partitions)
				if currentPartitionCount > 0 {
					currentReplicaCount := len(topic.Partitions[0].Replicas)
					if currentReplicaCount != len(assignments[0].Replicas) {
						fmt.Fprintf(os.Stderr, "ERR: requested topic %s has partitions with %d replicas; you cannot create a new partition with %d (must match)",
							*topic.Topic, currentReplicaCount, len(assignments[0].Replicas))
					}
				}

				createReq.Topics = append(createReq.Topics, kmsg.CreatePartitionsRequestTopic{
					Topic:      *topic.Topic,
					Count:      int32(currentPartitionCount + len(assignments)),
					Assignment: assignments,
				})
			}

			createResp, err := cl.Client().Request(context.Background(), &createReq)
			out.MaybeDie(err, "unable to create topic partitions: %v", err)

			if cl.AsJSON() {
				out.ExitJSON(createResp)
			}

			resps := createResp.(*kmsg.CreatePartitionsResponse).Topics
			tw := out.BeginTabWrite()
			defer tw.Flush()
			for _, topic := range resps {
				errKind := "OK"
				errMsg := ""
				if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
					errKind = err.Error()
					if topic.ErrorMessage != nil {
						errMsg = *topic.ErrorMessage
					}
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\n", topic.Topic, errKind, errMsg)
			}
		},
	}

	cmd.Flags().StringArrayVarP(&topics, "topic", "t", nil, "topic to add partitions to; repeatable")

	return cmd
}

func parseAssignments(in string) ([]kmsg.CreatePartitionsRequestTopicAssignment, error) {
	var partitions []kmsg.CreatePartitionsRequestTopicAssignment
	var replicasSize int

	for _, partition := range strings.Split(in, ":") {
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

		partitions = append(partitions, kmsg.CreatePartitionsRequestTopicAssignment{Replicas: replicas})
	}

	return partitions, nil
}
