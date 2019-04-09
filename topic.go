package main

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/twmb/kgo/kmsg"
)

// kcl topics
// kcl topics delete ...
// kcl topic create ...
// kcl topic delete ...
// kcl topic describe ...

func init() {
	root.AddCommand(topicsCmd())
	root.AddCommand(topicCmd())
}

func topicsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topics",
		Short: "list all topics",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			kresp, err := client.Request(new(kmsg.MetadataRequest))
			maybeDie(err, "unable to get metadata: %v", err)
			resp := kresp.(*kmsg.MetadataResponse)

			type topicData struct {
				name       string
				partitions int
				replicas   int
			}

			topics := make([]topicData, 0, len(resp.TopicMetadata))
			for _, topicMeta := range resp.TopicMetadata {
				td := topicData{
					name: topicMeta.Topic,
				}
				if len(topicMeta.PartitionMetadata) > 0 {
					td.partitions = len(topicMeta.PartitionMetadata)
					td.replicas = len(topicMeta.PartitionMetadata[0].Replicas)
				}
				topics = append(topics, td)
			}

			sort.Slice(topics, func(i, j int) bool {
				return topics[i].name < topics[j].name
			})
			fmt.Fprintf(tw, "NAME\t#PARTITIONS\t#REPLICAS\n")
			for _, topic := range topics {
				fmt.Fprintf(tw, "%s\t%d\t%d\n",
					topic.name, topic.partitions, topic.replicas)
			}
			tw.Flush()
		},
	}

	cmd.AddCommand(topicsDeleteCommand())

	return cmd
}

func topicsDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "delete TOPICS",
		Short: "delete all listed topics",
		Run: func(_ *cobra.Command, args []string) {
			resp, err := client.Request(&kmsg.DeleteTopicsRequest{
				Timeout: reqTimeoutMillis,
				Topics:  args,
			})
			maybeDie(err, "unable to delete topics: %v", err)
			dumpJSON(resp)
		},
	}
}

func topicCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic",
		Short: "create, delete, list, or describe topic(s)",
	}

	cmd.AddCommand(topicCreateCmd())
	cmd.AddCommand(topicDeleteCmd())
	return cmd
}

func topicCreateCmd() *cobra.Command {
	req := kmsg.CreateTopicsRequest{Timeout: reqTimeoutMillis}

	var topicReq kmsg.CreateTopicsRequestTopics
	var configKVs string

	cmd := &cobra.Command{
		Use:   "create TOPIC",
		Short: "create a topic",
		ValidArgs: []string{
			"--num-partitions",
			"--replication-factor",
			"--config-kvs",
			"--validate-only",
		},
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			topicReq.Topic = args[0]

			kvs, err := parseKVs(configKVs)
			maybeDie(err, "unable to parse KVs: %v", err)
			for _, kv := range kvs {
				topicReq.ConfigEntries = append(topicReq.ConfigEntries,
					kmsg.CreateTopicsRequestTopicsConfigEntries{kv.k, kmsg.StringPtr(kv.v)})
			}

			req.Topics = append(req.Topics, topicReq)

			resp, err := client.Request(&req)
			maybeDie(err, "unable to create topic: %v", err)
			dumpJSON(resp)
		},
	}

	cmd.Flags().BoolVar(&req.ValidateOnly, "validate-only", false, "validate the topic partition request; do not create topics (dry run)")
	cmd.Flags().Int32VarP(&topicReq.NumPartitions, "num-partitions", "p", 1, "number of partitions to create")
	cmd.Flags().Int16VarP(&topicReq.ReplicationFactor, "replication-factor", "r", 1, "number of replicas to have of each partition")
	cmd.Flags().StringVar(&configKVs, "config-kvs", "", "comma delimited list of key value config pairs corresponding to Kafka topic configuration kvs (e.g. cleanup.policy=compact,preallocate=true)")

	return cmd
}

func topicDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete TOPIC",
		Short: "delete a topic",
		Args:  cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			resp, err := client.Request(&kmsg.DeleteTopicsRequest{
				Timeout: reqTimeoutMillis,
				Topics:  args,
			})
			maybeDie(err, "unable to delete topic: %v", err)
			dumpJSON(resp)
		},
	}
}
