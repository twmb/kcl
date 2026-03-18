// Package streamsgroup contains streams group (KIP-1071) commands.
package streamsgroup

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "streams-group",
		Aliases: []string{"stg"},
		Short:   "Streams group operations (KIP-1071, Kafka 4.1+).",
		Args:    cobra.ExactArgs(0),
	}

	cmd.AddCommand(
		listCommand(cl),
		describeCommand(cl),
		deleteCommand(cl),
	)

	return cmd
}

func listCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all streams groups.",
		Long: `List streams groups (KIP-1071, Kafka 4.1+).

Equivalent to "group list --type-filter streams". Lists groups by issuing
a ListGroups request with type filter "streams".
`,
		Run: func(_ *cobra.Command, _ []string) {
			kresps := cl.Client().RequestSharded(context.Background(), &kmsg.ListGroupsRequest{
				TypesFilter: []string{"streams"},
			})

			tw := out.BeginTabWrite()
			defer tw.Flush()
			fmt.Fprintf(tw, "BROKER\tGROUP ID\tSTATE\n")
			for _, kresp := range kresps {
				err := kresp.Err
				if err == nil {
					err = kerr.ErrorForCode(kresp.Resp.(*kmsg.ListGroupsResponse).ErrorCode)
				}
				if err != nil {
					fmt.Fprintf(tw, "%d\t\t%v\n", kresp.Meta.NodeID, err)
					continue
				}
				resp := kresp.Resp.(*kmsg.ListGroupsResponse)
				for _, group := range resp.Groups {
					fmt.Fprintf(tw, "%d\t%s\t%s\n", kresp.Meta.NodeID, group.Group, group.GroupState)
				}
			}
		},
	}
}

func describeCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "describe GROUPS...",
		Aliases: []string{"d"},
		Short:   "Describe streams groups (KIP-1071).",
		Long: `Describe streams groups using the StreamsGroupDescribe API (KIP-1071, Kafka 4.1+).

Shows the streams topology (subtopologies, source topics, changelog topics),
member assignments, and process IDs.
`,
		Args: cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, groups []string) {
			req := kmsg.NewPtrStreamsGroupDescribeRequest()
			req.Groups = groups

			shards := cl.Client().RequestSharded(context.Background(), req)

			if cl.Format() == "json" {
				type jsonMember struct {
					MemberID    string `json:"member_id"`
					ClientID    string `json:"client_id"`
					Host        string `json:"host"`
					MemberEpoch int32  `json:"member_epoch"`
					ProcessID   string `json:"process_id"`
				}
				type jsonSubtopology struct {
					SubtopologyID           string   `json:"subtopology_id"`
					SourceTopics            []string `json:"source_topics"`
					RepartitionSinkTopics   []string `json:"repartition_sink_topics"`
					StateChangelogTopics    []string `json:"state_changelog_topics"`
					RepartitionSourceTopics []string `json:"repartition_source_topics"`
				}
				type jsonGroup struct {
					GroupID       string            `json:"group_id"`
					Coordinator   int32             `json:"coordinator"`
					State         string            `json:"state"`
					Epoch         int32             `json:"epoch"`
					TopologyEpoch int32             `json:"topology_epoch,omitempty"`
					Members       []jsonMember      `json:"members"`
					Subtopologies []jsonSubtopology `json:"subtopologies,omitempty"`
					Error         string            `json:"error,omitempty"`
				}
				var groups []jsonGroup
				for _, shard := range shards {
					if shard.Err != nil {
						continue
					}
					resp := shard.Resp.(*kmsg.StreamsGroupDescribeResponse)
					for _, g := range resp.Groups {
						jg := jsonGroup{
							GroupID:     g.Group,
							Coordinator: shard.Meta.NodeID,
							State:       g.State,
							Epoch:       g.Epoch,
						}
						if err := kerr.ErrorForCode(g.ErrorCode); err != nil {
							jg.Error = err.Error()
							if g.ErrorMessage != nil {
								jg.Error += ": " + *g.ErrorMessage
							}
						}
						if g.Topology != nil {
							jg.TopologyEpoch = g.Topology.Epoch
							for _, st := range g.Topology.Subtopologies {
								jg.Subtopologies = append(jg.Subtopologies, jsonSubtopology{
									SubtopologyID:           st.SubtopologyID,
									SourceTopics:            st.SourceTopics,
									RepartitionSinkTopics:   st.RepartitionSinkTopics,
									StateChangelogTopics:    topicInfoNameSlice(st.StateChangelogTopics),
									RepartitionSourceTopics: topicInfoNameSlice(st.RepartitionSourceTopics),
								})
							}
						}
						for _, m := range g.Members {
							jg.Members = append(jg.Members, jsonMember{
								MemberID:    m.MemberID,
								ClientID:    m.ClientID,
								Host:        m.ClientHost,
								MemberEpoch: m.MemberEpoch,
								ProcessID:   m.ProcessID,
							})
						}
						groups = append(groups, jg)
					}
				}
				out.MarshalJSON("streams-group.describe", 1, map[string]any{"groups": groups})
				return
			}

			for _, shard := range shards {
				if shard.Err != nil {
					fmt.Printf("unable to issue StreamsGroupDescribe to broker %d: %v\n", shard.Meta.NodeID, shard.Err)
					continue
				}
				resp := shard.Resp.(*kmsg.StreamsGroupDescribeResponse)
				for _, g := range resp.Groups {
					printStreamsGroup(shard.Meta.NodeID, g)
					fmt.Println()
				}
			}
		},
	}
}

func printStreamsGroup(broker int32, g kmsg.StreamsGroupDescribeResponseGroup) {
	tw := out.NewTabWriter()
	fmt.Fprintf(tw, "GROUP\t%s\n", g.Group)
	fmt.Fprintf(tw, "COORDINATOR\t%d\n", broker)
	fmt.Fprintf(tw, "STATE\t%s\n", g.State)
	fmt.Fprintf(tw, "EPOCH\t%d\n", g.Epoch)
	if g.Topology != nil {
		fmt.Fprintf(tw, "TOPOLOGY EPOCH\t%d\n", g.Topology.Epoch)
	}
	fmt.Fprintf(tw, "MEMBERS\t%d\n", len(g.Members))
	if err := kerr.ErrorForCode(g.ErrorCode); err != nil {
		msg := err.Error()
		if g.ErrorMessage != nil {
			msg += ": " + *g.ErrorMessage
		}
		fmt.Fprintf(tw, "ERROR\t%s\n", msg)
	}
	tw.Flush()

	if g.Topology != nil && len(g.Topology.Subtopologies) > 0 {
		fmt.Println("\nTOPOLOGY:")
		topoTw := out.NewTable("SUBTOPOLOGY", "SOURCE-TOPICS", "REPARTITION-SINK-TOPICS", "STATE-CHANGELOG-TOPICS", "REPARTITION-SOURCE-TOPICS")
		for _, st := range g.Topology.Subtopologies {
			topoTw.Print(
				st.SubtopologyID,
				strings.Join(st.SourceTopics, ","),
				strings.Join(st.RepartitionSinkTopics, ","),
				topicInfoNames(st.StateChangelogTopics),
				topicInfoNames(st.RepartitionSourceTopics),
			)
		}
		topoTw.Flush()
	}

	if len(g.Members) > 0 {
		fmt.Println("\nMEMBERS:")
		memTw := out.NewTable("MEMBER-ID", "CLIENT-ID", "HOST", "MEMBER-EPOCH", "PROCESS-ID", "ACTIVE-TASKS", "STANDBY-TASKS", "WARMUP-TASKS")
		for _, m := range g.Members {
			var active, standby, warmup []string
			for _, a := range m.Assignment.ActiveTasks {
				parts := fmtPartitions(a.Partitions)
				active = append(active, fmt.Sprintf("%s:[%s]", a.SubtopologyID, parts))
			}
			for _, a := range m.Assignment.StandbyTasks {
				parts := fmtPartitions(a.Partitions)
				standby = append(standby, fmt.Sprintf("%s:[%s]", a.SubtopologyID, parts))
			}
			for _, a := range m.Assignment.WarmupTasks {
				parts := fmtPartitions(a.Partitions)
				warmup = append(warmup, fmt.Sprintf("%s:[%s]", a.SubtopologyID, parts))
			}

			var rack string
			if m.RackID != nil {
				rack = " (rack=" + *m.RackID + ")"
			}

			memTw.Print(
				m.MemberID,
				m.ClientID,
				m.ClientHost+rack,
				m.MemberEpoch,
				m.ProcessID,
				strings.Join(active, " "),
				strings.Join(standby, " "),
				strings.Join(warmup, " "),
			)
		}
		memTw.Flush()
	}
}

func topicInfoNameSlice(infos []kmsg.TopicInfo) []string {
	names := make([]string, len(infos))
	for i, ti := range infos {
		names[i] = ti.Topic
	}
	return names
}

func topicInfoNames(infos []kmsg.TopicInfo) string {
	names := make([]string, len(infos))
	for i, ti := range infos {
		names[i] = ti.Topic
	}
	return strings.Join(names, ",")
}

func fmtPartitions(ps []int32) string {
	strs := make([]string, len(ps))
	for i, p := range ps {
		strs[i] = fmt.Sprintf("%d", p)
	}
	return strings.Join(strs, ",")
}

func deleteCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "delete GROUPS...",
		Short: "Delete streams groups.",
		Long:  "Delete streams groups (Kafka 4.1+). The groups must be empty.",
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
					fmt.Fprintf(tw, "%d\t\t%v\n", brokerResp.Meta.NodeID, err)
					continue
				}
				resp := kresp.(*kmsg.DeleteGroupsResponse)
				for _, r := range resp.Groups {
					msg := "OK"
					if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
						msg = err.Error()
					}
					fmt.Fprintf(tw, "%d\t%s\t%s\n", brokerResp.Meta.NodeID, r.Group, msg)
				}
			}
		},
	}
}
