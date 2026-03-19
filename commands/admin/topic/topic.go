// Package topic contains topic related utilities and subcommands.
package topic

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
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
		Short:   "Topic operations (list, describe, create, delete, trim-prefix).",
	}

	cmd.AddCommand(topicCreateCommand(cl))
	cmd.AddCommand(topicListCommand(cl))
	cmd.AddCommand(topicDeleteCommand(cl))
	cmd.AddCommand(topicAddPartitionsCommand(cl))
	cmd.AddCommand(topicDescribeCommand(cl))
	cmd.AddCommand(topicTrimPrefixCommand(cl))
	return cmd
}

func parseReplicaAssignment(s string) ([]kmsg.CreateTopicsRequestTopicReplicaAssignment, error) {
	var assignments []kmsg.CreateTopicsRequestTopicReplicaAssignment
	for i, group := range strings.Split(s, ",") {
		group = strings.TrimSpace(group)
		if len(group) == 0 {
			continue
		}
		var replicas []int32
		for _, idStr := range strings.Split(group, ":") {
			idStr = strings.TrimSpace(idStr)
			if len(idStr) == 0 {
				continue
			}
			id, err := strconv.Atoi(idStr)
			if err != nil {
				return nil, fmt.Errorf("unable to parse broker ID %q: %v", idStr, err)
			}
			replicas = append(replicas, int32(id))
		}
		if len(replicas) == 0 {
			continue
		}
		assignments = append(assignments, kmsg.CreateTopicsRequestTopicReplicaAssignment{
			Partition: int32(i),
			Replicas:  replicas,
		})
	}
	return assignments, nil
}

func topicCreateCommand(cl *client.Client) *cobra.Command {
	var (
		numPartitions     int32
		replicationFactor int16
		configKVs         []string
		validateOnly      bool
		ifNotExists       bool
		replicaAssignment string
	)

	cmd := &cobra.Command{
		Use:     "create TOPICS",
		Aliases: []string{"c"},
		Short:   "Create topics",
		Long: `Create topics (Kafka 0.10.1+).

All topics created with this command will have the same number of partitions,
replication factor, and key/value configs.

To manually assign replicas, use --replica-assignment with a comma-separated
list of colon-separated broker IDs. Each comma-separated group is a partition's
replica list. For example, "0:1:2,1:2:3,2:3:0" creates 3 partitions with 3
replicas each. When using --replica-assignment, do not use --num-partitions or
--replication-factor.
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			kvs, err := kv.Parse(configKVs)
			if err != nil {
				return fmt.Errorf("unable to parse KVs: %v", err)
			}
			req := kmsg.CreateTopicsRequest{TimeoutMillis: cl.TimeoutMillis()}
			req.ValidateOnly = validateOnly
			var configs []kmsg.CreateTopicsRequestTopicConfig
			for _, kv := range kvs {
				configs = append(configs, kmsg.CreateTopicsRequestTopicConfig{
					Name:  kv.K,
					Value: kmsg.StringPtr(kv.V),
				})
			}

			var assignments []kmsg.CreateTopicsRequestTopicReplicaAssignment
			if replicaAssignment != "" {
				assignments, err = parseReplicaAssignment(replicaAssignment)
				if err != nil {
					return fmt.Errorf("unable to parse replica assignment: %v", err)
				}
				if len(assignments) == 0 {
					return out.Errf(out.ExitUsage, "--replica-assignment specified but no partitions parsed")
				}
				numPartitions = -1
				replicationFactor = -1
			}

			for _, topic := range args {
				req.Topics = append(req.Topics, kmsg.CreateTopicsRequestTopic{
					Topic:             topic,
					ReplicationFactor: replicationFactor,
					NumPartitions:     numPartitions,
					ReplicaAssignment: assignments,
					Configs:           configs,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), &req)
			if err != nil {
				return fmt.Errorf("unable to create topic %q: %v", args[0], err)
			}

			resp := kresp.(*kmsg.CreateTopicsResponse)
			var table *out.FormattedTable
			if resp.Version >= 7 {
				table = out.NewFormattedTable(cl.Format(), "topic.create", 1, "topics",
					"NAME", "ID", "MESSAGE")
			} else {
				table = out.NewFormattedTable(cl.Format(), "topic.create", 1, "topics",
					"NAME", "MESSAGE")
			}
			for _, topic := range resp.Topics {
				msg := "OK"
				if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
					if ifNotExists && err == kerr.TopicAlreadyExists {
						msg = "OK (already exists)"
					} else {
						msg = err.Error()
						if topic.ErrorMessage != nil {
							msg += ": " + *topic.ErrorMessage
						}
					}
				}
				if resp.Version >= 7 {
					table.Row(topic.Topic, fmt.Sprintf("%x", topic.TopicID), msg)
				} else {
					table.Row(topic.Topic, msg)
				}
			}
			table.Flush()
			return nil
		},
	}

	cmd.Flags().BoolVarP(&validateOnly, "dry-run", "d", false, "validate the topic creation request; do not create topics (Kafka 0.10.2+)")
	cmd.Flags().Int32VarP(&numPartitions, "num-partitions", "p", 20, "number of partitions to create")
	cmd.Flags().Int16VarP(&replicationFactor, "replication-factor", "r", 1, "number of replicas to have of each partition")
	cmd.Flags().StringArrayVarP(&configKVs, "kv", "k", nil, "list of key=value config parameters (repeatable, e.g. -k cleanup.policy=compact -k preallocate=true)")
	cmd.Flags().BoolVar(&ifNotExists, "if-not-exists", false, "suppress error if topic already exists")
	cmd.Flags().StringVar(&replicaAssignment, "replica-assignment", "", "manual replica assignment as comma-separated partition groups of colon-separated broker IDs (e.g. 0:1:2,1:2:3,2:3:0)")

	return cmd
}

func topicListCommand(cl *client.Client) *cobra.Command {
	var (
		detailed     bool
		showInternal bool
		regexFilter  string
	)

	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all topics",
		Long: `List all topics.

EXAMPLES:
  kcl topic list                    # all non-internal topics
  kcl topic list -i                 # include internal topics
  kcl topic list --regex 'logs\.'   # filter by regex
`,
		RunE: func(_ *cobra.Command, _ []string) error {
			req := kmsg.NewPtrMetadataRequest()
			resp, err := req.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to list topics: %v", err)
			}

			var topics []kmsg.MetadataResponseTopic
			for _, t := range resp.Topics {
				if !showInternal && t.IsInternal {
					continue
				}
				if regexFilter != "" {
					re, err := regexp.Compile(regexFilter)
					if err != nil {
						return out.Errf(out.ExitUsage, "invalid --regex: %v", err)
					}
					name := ""
					if t.Topic != nil {
						name = *t.Topic
					}
					if !re.MatchString(name) {
						continue
					}
				}
				topics = append(topics, t)
			}
			metadata.PrintTopics(cl.Format(), resp.Version, topics, false, detailed)
			return nil
		},
	}
	cmd.Flags().BoolVarP(&detailed, "detailed", "d", false, "include detailed information about all topic partitions")
	cmd.Flags().BoolVarP(&showInternal, "internal", "i", false, "include internal topics")
	cmd.Flags().StringVar(&regexFilter, "regex", "", "filter topics by regex pattern")
	return cmd
}

func topicDeleteCommand(cl *client.Client) *cobra.Command {
	var ids bool
	var ifExists bool
	var dryRun bool
	var useRegex bool
	cmd := &cobra.Command{
		Use:   "delete TOPICS...",
		Short: "Delete all listed topics (Kafka 0.10.1+).",
		Long: `Delete all listed topics (Kafka 0.10.1+).

Use --regex to treat arguments as regex patterns: all topics matching any
pattern will be deleted. Use --dry-run to see which topics would be deleted
without actually deleting them.
`,
		RunE: func(_ *cobra.Command, topics []string) error {
			if useRegex {
				if ids {
					return out.Errf(out.ExitUsage, "--regex and --ids cannot be used together")
				}
				// Compile all patterns first.
				var patterns []*regexp.Regexp
				for _, pat := range topics {
					re, err := regexp.Compile(pat)
					if err != nil {
						return out.Errf(out.ExitUsage, "invalid regex %q: %v", pat, err)
					}
					patterns = append(patterns, re)
				}

				// Fetch all topic names via metadata.
				metaReq := kmsg.NewPtrMetadataRequest()
				metaResp, err := metaReq.RequestWith(context.Background(), cl.Client())
				if err != nil {
					return fmt.Errorf("unable to list topics: %v", err)
				}

				topics = nil
				for _, t := range metaResp.Topics {
					name := ""
					if t.Topic != nil {
						name = *t.Topic
					}
					for _, re := range patterns {
						if re.MatchString(name) {
							topics = append(topics, name)
							break
						}
					}
				}
				if len(topics) == 0 {
					fmt.Fprintln(os.Stderr, "No topics matched the provided regex patterns.")
					return nil
				}
			}

			if dryRun {
				fmt.Fprintln(os.Stderr, "Dry run: the following topics would be deleted:")
				for _, topic := range topics {
					fmt.Fprintf(os.Stderr, "  %s\n", topic)
				}
				return nil
			}

			req := &kmsg.DeleteTopicsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
				TopicNames:    topics,
			}
			for _, topic := range topics {
				t := kmsg.NewDeleteTopicsRequestTopic()
				if ids {
					if len(topic) != 32 {
						return out.Errf(out.ExitUsage, "topic id %s is not a 32 byte hex string", topic)
					}
					raw, err := hex.DecodeString(topic)
					if err != nil {
						return out.Errf(out.ExitUsage, "topic id %s is not a hex string", topic)
					}
					copy(t.TopicID[:], raw)
				} else {
					t.Topic = kmsg.StringPtr(topic)
				}
				req.Topics = append(req.Topics, t)
			}

			resp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to delete topics: %v", err)
			}

			resps := resp.(*kmsg.DeleteTopicsResponse).Topics
			table := out.NewFormattedTable(cl.Format(), "topic.delete", 1, "topics",
				"NAME", "MESSAGE")
			for _, topicResp := range resps {
				msg := "OK"
				if err := kerr.ErrorForCode(topicResp.ErrorCode); err != nil {
					if ifExists && err == kerr.UnknownTopicOrPartition {
						msg = "OK (did not exist)"
					} else {
						msg = err.Error()
						if topicResp.ErrorMessage != nil {
							msg += ": " + *topicResp.ErrorMessage
						}
					}
				}
				topic := ""
				if topicResp.Topic != nil {
					topic = *topicResp.Topic
				} else {
					topic = fmt.Sprintf("%x", topicResp.TopicID)
				}
				table.Row(topic, msg)
			}
			table.Flush()
			return nil
		},
	}
	cmd.Flags().BoolVar(&ids, "ids", false, "whether the input topics should be parsed as topic IDs")
	cmd.Flags().BoolVar(&ifExists, "if-exists", false, "suppress error if topic does not exist")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print topics that would be deleted without actually deleting them")
	cmd.Flags().BoolVar(&useRegex, "regex", false, "treat topic arguments as regex patterns; match against all existing topics")
	return cmd
}

func topicAddPartitionsCommand(cl *client.Client) *cobra.Command {
	var topics []string
	var force bool

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
		RunE: func(_ *cobra.Command, args []string) error {
			if len(topics) == 0 {
				return out.Errf(out.ExitUsage, "missing topics to add partitions to")
			}

			for _, topic := range topics {
				if strings.HasPrefix(topic, "__") && !force {
					return out.Errf(out.ExitUsage, "topic %q is an internal topic (starts with \"__\"); use --force to modify internal topics", topic)
				}
				if strings.HasPrefix(topic, "__") && force {
					fmt.Fprintf(os.Stderr, "WARNING: modifying internal topic %q; this can cause system instability\n", topic)
				}
			}

			assignments, err := parseAssignments(strings.Join(args, ""))
			if err != nil {
				return fmt.Errorf("parse assignments failure: %v", err)
			}
			if len(assignments) == 0 {
				return out.Errf(out.ExitUsage, "no new partitions requested")
			}

			// Get the metadata so we can determine the final partition count.
			metaReq := new(kmsg.MetadataRequest)
			for _, topic := range topics {
				t := topic
				metaReq.Topics = append(metaReq.Topics, kmsg.MetadataRequestTopic{Topic: &t})
			}
			kmetaResp, err := cl.Client().Request(context.Background(), metaReq)
			if err != nil {
				return fmt.Errorf("unable to get topic metadata: %v", err)
			}
			metaResp := kmetaResp.(*kmsg.MetadataResponse)

			createReq := kmsg.CreatePartitionsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for _, topic := range metaResp.Topics {
				if topic.Topic == nil {
					return fmt.Errorf("metadata returned nil topic, unknown topic ID!")
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
			if err != nil {
				return fmt.Errorf("unable to create topic partitions: %v", err)
			}

			resps := createResp.(*kmsg.CreatePartitionsResponse).Topics
			table := out.NewFormattedTable(cl.Format(), "topic.add-partitions", 1, "topics",
				"NAME", "STATUS", "MESSAGE")
			for _, topic := range resps {
				errKind := "OK"
				errMsg := ""
				if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
					errKind = err.Error()
					if topic.ErrorMessage != nil {
						errMsg = *topic.ErrorMessage
					}
				}
				table.Row(topic.Topic, errKind, errMsg)
			}
			table.Flush()
			return nil
		},
	}

	cmd.Flags().StringArrayVarP(&topics, "topic", "t", nil, "topic to add partitions to; repeatable")
	cmd.Flags().BoolVar(&force, "force", false, "allow modifying internal topics (those starting with \"__\")")

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
