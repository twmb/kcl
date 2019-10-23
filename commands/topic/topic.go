// Package topic contains topic related utilities and subcommands.
package topic

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/kv"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic",
		Short: "Perform topic relation actions",
	}

	cmd.AddCommand(topicCreateCommand(cl))
	cmd.AddCommand(topicDeleteCommand(cl))
	cmd.AddCommand(topicAddPartitionsCommand(cl))
	return cmd
}

func topicCreateCommand(cl *client.Client) *cobra.Command {
	req := kmsg.CreateTopicsRequest{TimeoutMillis: cl.TimeoutMillis()}

	var topicReq kmsg.CreateTopicsRequestTopic
	var configKVs []string

	cmd := &cobra.Command{
		Use:   "create TOPIC",
		Short: "Create a topic",
		ValidArgs: []string{
			"--num-partitions",
			"--replication-factor",
			"--kv",
			"--dry",
		},
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			topicReq.Topic = args[0]

			kvs, err := kv.Parse(configKVs)
			out.MaybeDie(err, "unable to parse KVs: %v", err)
			for _, kv := range kvs {
				topicReq.Configs = append(topicReq.Configs,
					kmsg.CreateTopicsRequestTopicConfig{
						Name:  kv.K,
						Value: kmsg.StringPtr(kv.V),
					})
			}

			req.Topics = append(req.Topics, topicReq)

			kresp, err := cl.Client().Request(context.Background(), &req)
			out.MaybeDie(err, "unable to create topic %q: %v", args[0], err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}
			resps := kresp.(*kmsg.CreateTopicsResponse).Topics
			if len(resps) != 1 {
				out.ExitErrJSON(kresp, "quitting; one topic create requested but received %d responses", len(resps))
			}
			out.ErrAndMsg(resps[0].ErrorCode, resps[0].ErrorMessage)
		},
	}

	cmd.Flags().BoolVarP(&req.ValidateOnly, "dry", "d", false, "dry run: validate the topic creation request; do not create topics")
	cmd.Flags().Int32VarP(&topicReq.NumPartitions, "num-partitions", "p", 20, "number of (p)artitions to create")
	cmd.Flags().Int16VarP(&topicReq.ReplicationFactor, "replication-factor", "r", 1, "number of (r)eplicas to have of each partition")
	cmd.Flags().StringSliceVarP(&configKVs, "kv", "k", nil, "list of (k)ey value config parameters (comma separated or repeated flag; e.g. cleanup.policy=compact,preallocate=true)")

	return cmd
}

func topicDeleteCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "delete TOPICS...",
		Short: "Delete all listed topics",
		Run: func(_ *cobra.Command, args []string) {
			resp, err := cl.Client().Request(context.Background(), &kmsg.DeleteTopicsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
				Topics:        args,
			})
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
				fmt.Fprintf(tw, "%s\t%s\n", topicResp.Topic, msg)
			}
		},
	}
}

func topicAddPartitionsCommand(cl *client.Client) *cobra.Command {
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
			out.MaybeDie(err, "parse assignments failure: %v", err)
			if len(assignments) == 0 {
				out.Die("no new partitions requested")
			}

			req := new(kmsg.MetadataRequest)
			for _, topic := range args {
				req.Topics = append(req.Topics, kmsg.MetadataRequestTopic{Topic: topic})
			}
			kmetaResp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to get topic metadata: %v", err)
			metas := kmetaResp.(*kmsg.MetadataResponse).Topics
			if len(metas) != 1 {
				out.Die("quitting; one metadata topic requested but received %d responses", len(metas))
			}
			if metas[0].Topic != args[0] {
				out.Die("quitting; metadata responded with non-requested topic %s", metas[0].Topic)
			}
			if err := kerr.ErrorForCode(metas[0].ErrorCode); err != nil {
				out.Die("quitting; metadata responded with topic error %s", err)
			}

			currentPartitionCount := len(metas[0].Partitions)
			if currentPartitionCount > 0 {
				currentReplicaCount := len(metas[0].Partitions[0].Replicas)
				if currentReplicaCount != len(assignments[0].Replicas) {
					out.Die("cannot create partitions with %d when existing partitions have %d replicas",
						len(assignments[0].Replicas), currentReplicaCount)
				}
			}

			createResp, err := cl.Client().Request(context.Background(), &kmsg.CreatePartitionsRequest{
				Topics: []kmsg.CreatePartitionsRequestTopic{
					{
						Topic:      args[0],
						Count:      int32(currentPartitionCount + len(assignments)),
						Assignment: assignments,
					},
				},
				TimeoutMillis: cl.TimeoutMillis(),
			})
			out.MaybeDie(err, "unable to create topic partitions: %v", err)

			if cl.AsJSON() {
				out.ExitJSON(createResp)
			}
			resps := createResp.(*kmsg.CreatePartitionsResponse).Topics
			if len(resps) != 1 {
				out.ExitErrJSON(createResp, "quitting; one topic partition creation requested but received %d responses", len(resps))
			}
			out.ErrAndMsg(resps[0].ErrorCode, resps[0].ErrorMessage)
		},
	}

	cmd.Flags().StringVarP(&assignmentFlag, "assignments", "a", "", "assignment of new semicolon delimited partitions and their comma delimited replicas to broker IDs")

	return cmd
}

func parseAssignments(in string) ([]kmsg.CreatePartitionsRequestTopicAssignment, error) {
	var partitions []kmsg.CreatePartitionsRequestTopicAssignment
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

		partitions = append(partitions, kmsg.CreatePartitionsRequestTopicAssignment{Replicas: replicas})
	}

	return partitions, nil
}
