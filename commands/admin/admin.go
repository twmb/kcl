// Package admin contains admin commands.
package admin

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/commands/admin/acl"
	"github.com/twmb/kcl/commands/admin/clientquotas"
	"github.com/twmb/kcl/commands/admin/configs"
	"github.com/twmb/kcl/commands/admin/dtoken"
	"github.com/twmb/kcl/commands/admin/features"
	"github.com/twmb/kcl/commands/admin/group"
	"github.com/twmb/kcl/commands/admin/logdirs"
	"github.com/twmb/kcl/commands/admin/partas"
	"github.com/twmb/kcl/commands/admin/sharegroup"
	"github.com/twmb/kcl/commands/admin/topic"
	"github.com/twmb/kcl/commands/admin/txn"
	"github.com/twmb/kcl/commands/admin/userscram"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:        "admin",
		Aliases:    []string{"adm", "a"},
		Short:      "Admin utility commands.",
		Deprecated: "use top-level commands instead (e.g., 'kcl group list' instead of 'kcl admin group list')",
		Hidden:     true,
	}

	cmd.AddCommand(
		ElectLeadersCommand(cl),
		DescribeClusterCommand(cl),
		DescribeQuorumCommand(cl),

		acl.Command(cl),
		clientquotas.Command(cl),
		configs.Command(cl),
		dtoken.Command(cl),
		features.Command(cl),
		group.Command(cl),
		topic.Command(cl),
		logdirs.Command(cl),
		partas.Command(cl),
		sharegroup.Command(cl),
		userscram.Command(cl),
		txn.Command(cl),
	)

	return cmd
}

func ElectLeadersCommand(cl *client.Client) *cobra.Command {
	var allPartitions bool
	var unclean bool
	var dryRun bool

	cmd := &cobra.Command{
		Use:   "elect-leaders",
		Short: "Trigger leader elections for partitions",
		Long: `Trigger leader elections for topic partitions (Kafka 2.2.0+).

This command allows for triggering leader elections on any topic and any
partition, as well as on all topic partitions. To run on all, you must not
pass any topic flags, and you must use the --all-partitions flag.

The format for triggering topic partitions is "foo:1,2,3", where foo is a
topic and 1,2,3 are partition numbers.

Use --dry-run to preview without applying.
`,
		Example: "elect-leaders foo:1,2,3 bar:9",
		RunE: func(_ *cobra.Command, topicParts []string) error {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			if err != nil {
				return fmt.Errorf("unable to parse topic partitions: %v", err)
			}
			if dryRun {
				fmt.Fprintln(os.Stderr, "Dry run: would elect leaders for the following partitions:")
				for topic, parts := range tps {
					fmt.Fprintf(os.Stderr, "  %s: %v\n", topic, parts)
				}
				return nil
			}

			req := &kmsg.ElectLeadersRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			if unclean {
				req.ElectionType = 1
			}

			topics := []kmsg.ElectLeadersRequestTopic{}
			if allPartitions {
				topics = nil
			} else if len(topics) == 0 {
				return fmt.Errorf("no topics requested for leader election, and not triggering all; nothing to do")
			}

			for topic, partitions := range tps {
				req.Topics = append(req.Topics, kmsg.ElectLeadersRequestTopic{
					Topic:      topic,
					Partitions: partitions,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to elect leaders: %v", err)
			}

			resp := kresp.(*kmsg.ElectLeadersResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				return fmt.Errorf("%v", err)
			}

			table := out.NewFormattedTable(cl.Format(), "cluster.elect-leaders", 1, "results",
				"TOPIC", "PARTITION", "ERROR", "MESSAGE")
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					errKind := ""
					var msg string
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						errKind = err.Error()
					}
					if partition.ErrorMessage != nil {
						msg = *partition.ErrorMessage
					}
					table.Row(topic.Topic, partition.Partition, errKind, msg)
				}
			}
			table.Flush()
			return nil
		},
	}

	cmd.Flags().BoolVar(&allPartitions, "all-partitions", false, "trigger leader election on all topics for all partitions")
	cmd.Flags().BoolVar(&unclean, "unclean", false, "allow unclean leader election (Kafka 2.4.0+)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "preview which partitions would have leaders elected without applying")

	return cmd
}

func DescribeClusterCommand(cl *client.Client) *cobra.Command {
	var includeAuthorizedOps bool

	cmd := &cobra.Command{
		Use:   "describe-cluster",
		Short: "Describe the Kafka cluster (Kafka 3.0+).",
		Long: `Describe the Kafka cluster (Kafka 3.0+).

This command prints the cluster ID, controller ID, and a table of all brokers
in the cluster.
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			req := kmsg.NewPtrDescribeClusterRequest()
			req.IncludeClusterAuthorizedOperations = includeAuthorizedOps

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to describe cluster: %v", err)
			}

			resp := kresp.(*kmsg.DescribeClusterResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				if cl.Format() == "json" {
					msg := err.Error()
					if resp.ErrorMessage != nil {
						msg += ": " + *resp.ErrorMessage
					}
					out.DieJSON("cluster.describe", err.Error(), msg)
				}
				additional := ""
				if resp.ErrorMessage != nil {
					additional = ": " + *resp.ErrorMessage
				}
				return fmt.Errorf("%s%s", err, additional)
			}

			switch cl.Format() {
			case "json":
				type brokerJSON struct {
					ID   int32  `json:"id"`
					Host string `json:"host"`
					Port int32  `json:"port"`
					Rack string `json:"rack,omitempty"`
				}
				brokers := make([]brokerJSON, len(resp.Brokers))
				for i, b := range resp.Brokers {
					brokers[i] = brokerJSON{ID: b.NodeID, Host: b.Host, Port: b.Port}
					if b.Rack != nil {
						brokers[i].Rack = *b.Rack
					}
				}
				fields := map[string]any{
					"cluster_id":    resp.ClusterID,
					"controller_id": resp.ControllerID,
					"brokers":       brokers,
				}
				if includeAuthorizedOps {
					fields["authorized_operations"] = resp.ClusterAuthorizedOperations
				}
				out.MarshalJSON("cluster.describe", 1, fields)

			case "awk":
				for _, broker := range resp.Brokers {
					var rack string
					if broker.Rack != nil {
						rack = *broker.Rack
					}
					fmt.Printf("%d\t%s\t%d\t%s\n", broker.NodeID, broker.Host, broker.Port, rack)
				}

			default:
				fmt.Printf("CLUSTER ID: %s\n", resp.ClusterID)
				fmt.Printf("CONTROLLER: %d\n", resp.ControllerID)
				if includeAuthorizedOps {
					fmt.Printf("AUTHORIZED OPS: %d\n", resp.ClusterAuthorizedOperations)
				}
				fmt.Println()

				tw := out.BeginTabWrite()
				defer tw.Flush()

				fmt.Fprintf(tw, "ID\tHOST\tPORT\tRACK\n")
				for _, broker := range resp.Brokers {
					var rack string
					if broker.Rack != nil {
						rack = *broker.Rack
					}
					fmt.Fprintf(tw, "%d\t%s\t%d\t%s\n", broker.NodeID, broker.Host, broker.Port, rack)
				}
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&includeAuthorizedOps, "include-authorized-ops", false, "include cluster authorized operations in the response")
	return cmd
}

func DescribeQuorumCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "describe-quorum",
		Short: "Describe the KRaft quorum (Kafka 3.0+).",
		Long: `Describe the KRaft quorum (Kafka 3.0+).

This command describes the quorum status for the __cluster_metadata partition,
including the leader, epoch, high watermark, and information about voters
and observers.
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			req := kmsg.NewPtrDescribeQuorumRequest()
			req.Topics = []kmsg.DescribeQuorumRequestTopic{{
				Topic:      "__cluster_metadata",
				Partitions: []kmsg.DescribeQuorumRequestTopicPartition{{Partition: 0}},
			}}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to describe quorum: %v", err)
			}

			resp := kresp.(*kmsg.DescribeQuorumResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				additional := ""
				if resp.ErrorMessage != nil {
					additional = ": " + *resp.ErrorMessage
				}
				return fmt.Errorf("%s%s", err, additional)
			}

			switch cl.Format() {
			case "json":
				type replicaJSON struct {
					ReplicaID            int32 `json:"replica_id"`
					LogEndOffset         int64 `json:"log_end_offset"`
					LastFetchTimestamp    int64 `json:"last_fetch_timestamp"`
					LastCaughtUpTimestamp int64 `json:"last_caught_up_timestamp"`
				}
				type partJSON struct {
					Topic         string        `json:"topic"`
					Partition     int32         `json:"partition"`
					Leader        int32         `json:"leader"`
					LeaderEpoch   int32         `json:"leader_epoch"`
					HighWatermark int64         `json:"high_watermark"`
					Voters        []replicaJSON `json:"voters,omitempty"`
					Observers     []replicaJSON `json:"observers,omitempty"`
				}
				var parts []partJSON
				for _, topic := range resp.Topics {
					for _, p := range topic.Partitions {
						pj := partJSON{
							Topic: topic.Topic, Partition: p.Partition,
							Leader: p.LeaderID, LeaderEpoch: p.LeaderEpoch,
							HighWatermark: p.HighWatermark,
						}
						for _, v := range p.CurrentVoters {
							pj.Voters = append(pj.Voters, replicaJSON{v.ReplicaID, v.LogEndOffset, v.LastFetchTimestamp, v.LastCaughtUpTimestamp})
						}
						for _, o := range p.Observers {
							pj.Observers = append(pj.Observers, replicaJSON{o.ReplicaID, o.LogEndOffset, o.LastFetchTimestamp, o.LastCaughtUpTimestamp})
						}
						parts = append(parts, pj)
					}
				}
				out.MarshalJSON("cluster.describe-quorum", 1, map[string]any{"partitions": parts})

			case "awk":
				for _, topic := range resp.Topics {
					for _, p := range topic.Partitions {
						for _, v := range p.CurrentVoters {
							fmt.Printf("%s\t%d\tvoter\t%d\t%d\t%d\t%d\n", topic.Topic, p.Partition, v.ReplicaID, v.LogEndOffset, v.LastFetchTimestamp, v.LastCaughtUpTimestamp)
						}
						for _, o := range p.Observers {
							fmt.Printf("%s\t%d\tobserver\t%d\t%d\t%d\t%d\n", topic.Topic, p.Partition, o.ReplicaID, o.LogEndOffset, o.LastFetchTimestamp, o.LastCaughtUpTimestamp)
						}
					}
				}

			default:
				for _, topic := range resp.Topics {
					for _, p := range topic.Partitions {
						if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
							fmt.Printf("%s partition %d: %v\n", topic.Topic, p.Partition, err)
							continue
						}
						fmt.Printf("%s partition %d: leader %d, epoch %d, high-watermark %d\n",
							topic.Topic, p.Partition, p.LeaderID, p.LeaderEpoch, p.HighWatermark)

						if len(p.CurrentVoters) > 0 {
							fmt.Println()
							fmt.Println("VOTERS:")
							tw := out.BeginTabWrite()
							fmt.Fprintf(tw, "  REPLICA\tLOG-END-OFFSET\tLAST-FETCH-TIMESTAMP\tLAST-CAUGHT-UP-TIMESTAMP\n")
							for _, v := range p.CurrentVoters {
								fmt.Fprintf(tw, "  %d\t%d\t%d\t%d\n",
									v.ReplicaID, v.LogEndOffset, v.LastFetchTimestamp, v.LastCaughtUpTimestamp)
							}
							tw.Flush()
						}

						if len(p.Observers) > 0 {
							fmt.Println()
							fmt.Println("OBSERVERS:")
							tw := out.BeginTabWrite()
							fmt.Fprintf(tw, "  REPLICA\tLOG-END-OFFSET\tLAST-FETCH-TIMESTAMP\tLAST-CAUGHT-UP-TIMESTAMP\n")
							for _, o := range p.Observers {
								fmt.Fprintf(tw, "  %d\t%d\t%d\t%d\n",
									o.ReplicaID, o.LogEndOffset, o.LastFetchTimestamp, o.LastCaughtUpTimestamp)
							}
							tw.Flush()
						}
					}
				}
			}
			return nil
		},
	}
}
