// Package group contains group related subcommands.
package group

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

var (
	gGroupsLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "drio_kafka_group_lag",
		Help: "Current lag for all kafka consumer groups",
	}, []string{
		"cgname",
	})
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "group",
		Aliases: []string{"g"},
		Short:   "Perform group related actions (list, describe, delete, offset-delete).",
		Args:    cobra.ExactArgs(0),
	}

	cmd.AddCommand(
		listCommand(cl),
		describeCommand(cl),
		deleteCommand(cl),
		offsetDeleteCommand(cl),
		serveLags(cl),
	)

	return cmd
}

func serveLags(cl *client.Client) *cobra.Command {
	var readCommitted bool

	return &cobra.Command{
		Use:   "server-lags",
		Short: "Expose lags via prometheus metrics",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, groups []string) {
			if len(groups) == 0 {
				groups = listGroups(cl)
			}
			if len(groups) == 0 {
				out.Die("no groups to describe")
			}

			described := describeGroups(cl, groups)
			fetchedOffsets := fetchOffsets(cl, groups)
			listedOffsets := listOffsets(cl, described, readCommitted)

			go func() {
				for {
					updateLags(described, fetchedOffsets, listedOffsets)
					fmt.Printf("Sleeping \n")
					time.Sleep(10 * time.Second)
				}
			}()

			http.Handle("/metrics", promhttp.Handler())
			port := "8787" // TODO
			log.Printf("Listening on :%s", port)
			err := http.ListenAndServe(fmt.Sprintf("localhost:%s", port), nil)
			if err != nil {
				log.Panicf("Error starting web server: %s", err)
			}

		},
	}

}

func updateLags(
	groups []describedGroup,
	fetched map[string]map[int32]offset,
	listed map[string]map[int32]offset,
) {
	lookup := func(m map[string]map[int32]offset, topic string, partition int32) offset {
		p := m[topic]
		if p == nil {
			return offset{at: -1}
		}
		o, exists := p[partition]
		if !exists {
			return offset{at: -1}
		}
		return o
	}

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Group < groups[j].Group
	})

	for _, group := range groups {
		for _, member := range group.Members {
			for _, topic := range member.MemberAssignment.Topics {
				t := topic.Topic
				for _, p := range topic.Partitions {
					committed := lookup(fetched, t, p)
					end := lookup(listed, t, p)

					if committed.err != nil {
						fmt.Printf("%s", committed.err)
					} else {
						gGroupsLag.WithLabelValues(group.Group).Set(float64(end.at - committed.at))
					}
				}
			}
		}
	}
}

func listCommand(cl *client.Client) *cobra.Command {
	var statesFilter []string
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all groups (Kafka 0.9.0+).",
		Long: `List all Kafka groups.

This command simply lists groups and their protocol types; it does not describe
the groups listed. This prints all of the information from a ListGroups request.
`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			for i, f := range statesFilter {
				switch client.Strnorm(f) {
				case "preparing":
					statesFilter[i] = "Preparing"
				case "preparingrebalance":
					statesFilter[i] = "PreparingRebalance"
				case "completingrebalance":
					statesFilter[i] = "CompletingRebalance"
				case "stable":
					statesFilter[i] = "Stable"
				case "dead":
					statesFilter[i] = "Dead"
				case "empty":
					statesFilter[i] = "Empty"
				}
			}
			kresps := cl.Client().RequestSharded(context.Background(), &kmsg.ListGroupsRequest{
				StatesFilter: statesFilter,
			})

			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "BROKER\tGROUP ID\tPROTO TYPE\tSTATE\tERROR\n")
			for _, kresp := range kresps {
				err := kresp.Err
				if err == nil {
					err = kerr.ErrorForCode(kresp.Resp.(*kmsg.ListGroupsResponse).ErrorCode)
				}
				if err != nil {
					fmt.Fprintf(tw, "%d\t\t\t\t%v\n", kresp.Meta.NodeID, err)
					continue
				}

				resp := kresp.Resp.(*kmsg.ListGroupsResponse)
				for _, group := range resp.Groups {
					fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t\n", kresp.Meta.NodeID, group.Group, group.ProtocolType, group.GroupState)
				}
			}
		},
	}
	cmd.Flags().StringArrayVarP(&statesFilter, "filter", "f", nil, "filter groups listed by state (Preparing, PreparingRebalance, CompletingRebalance, Stable, Dead, Empty; Kafka 2.6.0+; repeatable)")
	return cmd
}

func deleteCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "delete GROUPS...",
		Short: "Delete all listed Kafka groups (Kafka 1.1.0+).",
		Args:  cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			brokerResps := cl.Client().RequestSharded(context.Background(), &kmsg.DeleteGroupsRequest{
				Groups: args,
			})
			tw := out.BeginTabWrite()
			defer tw.Flush()
			fmt.Fprintf(tw, "BROKER\tGROUP\tERROR\n")
			for _, brokerResp := range brokerResps {
				kresp, err := brokerResp.Resp, brokerResp.Err
				if err != nil {
					fmt.Fprintf(tw, "%d\t\tunable to issue request (addr %s:%d): %v\n", brokerResp.Meta.NodeID, brokerResp.Meta.Host, brokerResp.Meta.Port, err)
					continue
				}
				resp := kresp.(*kmsg.DeleteGroupsResponse)
				for _, resp := range resp.Groups {
					msg := "OK"
					if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
						msg = err.Error()
					}
					fmt.Fprintf(tw, "%d\t%s\t%s\n", brokerResp.Meta.NodeID, resp.Group, msg)
				}
			}
		},
	}
}

func offsetDeleteCommand(cl *client.Client) *cobra.Command {
	var topicParts []string

	cmd := &cobra.Command{
		Use:   "offset-delete GROUP",
		Short: "Delete offsets for a Kafka group.",
		Long: `Forcefully delete offsets for a Kafka group (Kafka 2.4.0+).

Introduced in Kafka 2.4.0, this command forcefully deletes committed offsets
for a group. Why, you ask? Group commit expiration semantics have changed
across Kafka releases. KIP-211 addressed commits expiring in groups that were
infrequently committing but not yet dead, but introduced a problem where
commits can hang around in some edge cases. See the motivation in KIP-496 for
more detals.

The format for deleting offsets per topic partition is "foo:1,2,3", where foo
is a topic and 1,2,3 are partition numbers.
`,
		Example: "offset-delete mygroup -t foo:1,2,3 -t bar:9",
		Args:    cobra.ExactArgs(1),

		Run: func(_ *cobra.Command, args []string) {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			out.MaybeDie(err, "unable to parse topic partitions: %v", err)

			req := &kmsg.OffsetDeleteRequest{
				Group: args[0],
			}
			for topic, partitions := range tps {
				dt := kmsg.OffsetDeleteRequestTopic{
					Topic: topic,
				}
				for _, partition := range partitions {
					dt.Partitions = append(dt.Partitions, kmsg.OffsetDeleteRequestTopicPartition{Partition: partition})
				}
				req.Topics = append(req.Topics, dt)
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to delete offsets: %v", err)
			resp := kresp.(*kmsg.OffsetDeleteResponse)
			if cl.AsJSON() {
				out.ExitJSON(resp)
			}

			if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.Die(err.Error())
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					msg := "OK"
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						msg = err.Error()
					}
					fmt.Fprintf(tw, "%s\t%d\t%s\n", topic.Topic, partition.Partition, msg)
				}
			}
		},
	}

	cmd.Flags().StringArrayVarP(&topicParts, "topic", "t", nil, "topic and partitions to delete offsets for; repeatable")
	return cmd
}
