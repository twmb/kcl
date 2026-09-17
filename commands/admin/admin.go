// Package admin contains admin commands.
package admin

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

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
	// Everything under admin forwards to the top level, except the four
	// that moved under cluster.
	out.AliasOf(cmd, "")
	electLeaders := ElectLeadersCommand(cl)
	out.AliasOf(electLeaders, "cluster.elect-leaders")
	describeCluster := DescribeClusterCommand(cl)
	out.AliasOf(describeCluster, "cluster.describe")
	describeQuorum := DescribeQuorumCommand(cl)
	out.AliasOf(describeQuorum, "cluster.describe-quorum")
	featuresCmd := features.Command(cl)
	out.AliasOf(featuresCmd, "cluster.features")

	cmd.AddCommand(
		electLeaders,
		describeCluster,
		describeQuorum,

		acl.Command(cl),
		clientquotas.Command(cl),
		configs.Command(cl),
		dtoken.Command(cl),
		featuresCmd,
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

var (
	electHeaders    = []string{"TOPIC", "PARTITION", "ERROR", "MESSAGE"}
	brokersHeaders  = []string{"ID", "HOST", "PORT", "RACK"}
	clusterHeaders  = []string{"CLUSTER-ID", "CONTROLLER", "AUTHORIZED-OPERATIONS"}
	quorumHeaders   = []string{"TOPIC", "PARTITION", "LEADER", "LEADER-EPOCH", "HIGH-WATERMARK", "ROLE", "REPLICA", "LOG-END-OFFSET", "LAST-FETCH-TIMESTAMP", "LAST-CAUGHT-UP-TIMESTAMP", "ERROR", "MESSAGE"}
	replicasHeaders = quorumHeaders[5:10]
)

func ElectLeadersCommand(cl *client.Client) *cobra.Command {
	var allPartitions bool
	var unclean bool
	var dryRun bool

	cmd := &cobra.Command{
		Use:   "elect-leaders [TOPIC:P,P...]",
		Short: "Trigger leader elections for partitions.",
		Long: `Trigger leader elections for partitions.

Trigger leader elections for topic partitions (Kafka 2.2.0+).

This command allows for triggering leader elections on any topic and any
partition, as well as on all topic partitions. To run on all, you must not
pass any topics, and you must use the --all-partitions flag.

The format for triggering topic partitions is "foo:1,2,3", where foo is a
topic and 1,2,3 are partition numbers. A bare topic is every partition of the
topic.

The result prints one row per partition with ERROR and MESSAGE. --dry-run
prints the partitions that would be elected, with no result, and asks the
cluster for nothing but the partition list.

EXAMPLES:
  kcl cluster elect-leaders foo:1,2,3 bar:9
  kcl cluster elect-leaders foo --unclean
  kcl cluster elect-leaders --all-partitions --dry-run

SEE ALSO:
  kcl topic describe    see each partition's leader
`,
		RunE: func(_ *cobra.Command, topicParts []string) error {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			if err != nil {
				return out.Errf(out.ExitUsage, "unable to parse topic partitions: %v", err)
			}
			if allPartitions && len(tps) > 0 {
				return out.Errf(out.ExitUsage, "--all-partitions takes no TOPIC:P arguments")
			}
			if !allPartitions && len(tps) == 0 {
				return out.Errf(out.ExitUsage, "no partitions given: name TOPIC:P,P... or use --all-partitions")
			}

			// A bare topic, or every topic for a dry run of
			// --all-partitions, is resolved from metadata. A real
			// --all-partitions run sends no topics at all.
			var metaTopics []kmsg.MetadataRequestTopic
			for topic, partitions := range tps {
				if len(partitions) == 0 {
					metaTopics = append(metaTopics, kmsg.MetadataRequestTopic{Topic: kmsg.StringPtr(topic)})
				}
			}
			if len(metaTopics) > 0 || allPartitions && dryRun {
				resp, err := (&kmsg.MetadataRequest{Topics: metaTopics}).RequestWith(context.Background(), cl.Client())
				if err != nil {
					return fmt.Errorf("unable to get metadata: %v", err)
				}
				for _, topic := range resp.Topics {
					if topic.Topic == nil {
						continue
					}
					if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
						return fmt.Errorf("unable to get metadata for topic %s: %v", *topic.Topic, err)
					}
					for _, partition := range topic.Partitions {
						tps[*topic.Topic] = append(tps[*topic.Topic], partition.Partition)
					}
				}
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", electHeaders...).ResultColumns()
			if dryRun {
				table.SetDryRun(true)
				for _, topic := range slices.Sorted(maps.Keys(tps)) {
					partitions := slices.Clone(tps[topic])
					slices.Sort(partitions)
					for _, p := range partitions {
						table.Row(topic, p, out.Unknown, out.Unknown)
					}
				}
				return table.Flush()
			}

			req := &kmsg.ElectLeadersRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			if unclean {
				req.ElectionType = 1
			}
			if !allPartitions {
				for topic, partitions := range tps {
					req.Topics = append(req.Topics, kmsg.ElectLeadersRequestTopic{
						Topic:      topic,
						Partitions: partitions,
					})
				}
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to elect leaders: %v", err)
			}

			resp := kresp.(*kmsg.ElectLeadersResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				return fmt.Errorf("%v", err)
			}

			type row struct {
				topic     string
				partition int32
				err       string
				msg       string
			}
			var rows []row
			for _, topic := range resp.Topics {
				for _, p := range topic.Partitions {
					rows = append(rows, row{topic.Topic, p.Partition, out.ErrName(p.ErrorCode), out.BrokerMessage(p.ErrorMessage)})
				}
			}
			slices.SortFunc(rows, func(a, b row) int {
				if a.topic != b.topic {
					return strings.Compare(a.topic, b.topic)
				}
				return int(a.partition - b.partition)
			})
			for _, r := range rows {
				table.Row(r.topic, r.partition, r.err, r.msg)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, electHeaders...)

	cmd.Flags().BoolVar(&allPartitions, "all-partitions", false, "trigger leader election on all topics for all partitions")
	cmd.Flags().BoolVar(&unclean, "unclean", false, "allow unclean leader election (Kafka 2.4.0+)")
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "print the partitions that would have leaders elected without electing any")

	return cmd
}

func DescribeClusterCommand(cl *client.Client) *cobra.Command {
	var includeAuthorizedOps bool
	var section string

	cmd := &cobra.Command{
		Use:     "describe",
		Aliases: []string{"describe-cluster"},
		Short:   "Describe the Kafka cluster (Kafka 3.0+).",
		Long: `Describe the Kafka cluster (Kafka 3.0+).

This issues DescribeCluster, which answers cluster-level questions: the
cluster ID, which broker is controller, the broker list, and (with
--include-authorized-ops) what the current principal may do. For topics and
partitions, use "kcl cluster metadata", which issues Metadata instead.

Text prints the cluster summary and then the broker table, sorted by broker
ID. awk prints one section: the broker rows ID HOST PORT RACK by default, or
with --section cluster one row CLUSTER-ID CONTROLLER AUTHORIZED-OPERATIONS.
AUTHORIZED-OPERATIONS is the bitfield the broker answers and is unknown
without --include-authorized-ops.

EXAMPLES:
  kcl cluster describe
  kcl cluster describe --section cluster --format awk    # the ID and controller only

SEE ALSO:
  kcl cluster metadata    topics, partitions, and brokers, from Metadata
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			switch section {
			case "", "cluster", "brokers":
			default:
				return out.Errf(out.ExitUsage, "invalid --section %q: must be cluster or brokers", section)
			}

			showCluster := section == "" || section == "cluster"
			showBrokers := section == "" || section == "brokers"

			req := kmsg.NewPtrDescribeClusterRequest()
			req.IncludeClusterAuthorizedOperations = includeAuthorizedOps

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to describe cluster: %v", err)
			}

			resp := kresp.(*kmsg.DescribeClusterResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				return out.BrokerErr(err, resp.ErrorMessage)
			}

			brokers := slices.Clone(resp.Brokers)
			slices.SortFunc(brokers, func(a, b kmsg.DescribeClusterResponseBroker) int { return int(a.NodeID - b.NodeID) })
			rack := func(b kmsg.DescribeClusterResponseBroker) string {
				if b.Rack != nil {
					return *b.Rack
				}
				return ""
			}
			var authorizedOps any = out.Unknown
			if includeAuthorizedOps {
				authorizedOps = resp.ClusterAuthorizedOperations
			}

			switch cl.Format() {
			case out.FormatJSON:
				type brokerJSON struct {
					ID   int32  `json:"id"`
					Host string `json:"host"`
					Port int32  `json:"port"`
					Rack string `json:"rack"`
				}
				bs := make([]brokerJSON, len(brokers))
				for i, b := range brokers {
					bs[i] = brokerJSON{ID: b.NodeID, Host: b.Host, Port: b.Port, Rack: rack(b)}
				}
				out.MarshalJSON(cl.Command(), 1, map[string]any{
					"cluster_id":            resp.ClusterID,
					"controller_id":         resp.ControllerID,
					"authorized_operations": authorizedOps,
					"brokers":               bs,
				})

			case out.FormatAWK:
				if section == "cluster" {
					table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "cluster", clusterHeaders...)
					table.Row(resp.ClusterID, resp.ControllerID, authorizedOps)
					return table.Flush()
				}
				table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "brokers", brokersHeaders...)
				for _, b := range brokers {
					table.Row(b.NodeID, b.Host, b.Port, rack(b))
				}
				return table.Flush()

			default:
				if showCluster {
					tw := out.BeginTabWrite()
					fmt.Fprintf(tw, "CLUSTER-ID\t%s\n", resp.ClusterID)
					fmt.Fprintf(tw, "CONTROLLER\t%d\n", resp.ControllerID)
					if includeAuthorizedOps {
						fmt.Fprintf(tw, "AUTHORIZED-OPERATIONS\t%d\n", resp.ClusterAuthorizedOperations)
					}
					tw.Flush()
				}
				if showBrokers {
					if showCluster {
						fmt.Println()
					}
					table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "brokers", brokersHeaders...)
					for _, b := range brokers {
						table.Row(b.NodeID, b.Host, b.Port, rack(b))
					}
					return table.Flush()
				}
			}
			return nil
		},
	}
	out.ColumnsFunc(cmd, func() []string {
		if section == "cluster" {
			return clusterHeaders
		}
		return brokersHeaders
	})

	cmd.Flags().StringVar(&section, "section", "", "output section (cluster, brokers; default: all for text, brokers for awk)")
	cmd.Flags().BoolVar(&includeAuthorizedOps, "include-authorized-ops", false, "include cluster authorized operations in the response")
	return cmd
}

func DescribeQuorumCommand(cl *client.Client) *cobra.Command {
	var section string

	cmd := &cobra.Command{
		Use:   "describe-quorum",
		Short: "Describe the KRaft quorum (Kafka 3.0+).",
		Long: `Describe the KRaft quorum (Kafka 3.0+).

This command describes the quorum status for the __cluster_metadata partition,
including the leader, epoch, high watermark, and information about voters
and observers.

Text prints the partition summary and then one table of its replicas, each
row a voter or an observer. awk prints one row per replica, the partition
columns repeated on each: TOPIC PARTITION LEADER LEADER-EPOCH HIGH-WATERMARK
ROLE REPLICA LOG-END-OFFSET LAST-FETCH-TIMESTAMP LAST-CAUGHT-UP-TIMESTAMP
ERROR. --section prints the voters or the observers alone; a partition the
broker could not describe is one row with ERROR set.

EXAMPLES:
  kcl cluster describe-quorum
  kcl cluster describe-quorum --section voters

SEE ALSO:
  kcl cluster describe    the cluster ID, controller, and brokers
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			switch section {
			case "", "voters", "observers":
			default:
				return out.Errf(out.ExitUsage, "invalid --section %q: must be voters or observers", section)
			}

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
				return out.BrokerErr(err, resp.ErrorMessage)
			}

			// A partition the broker could not describe is in the
			// document with its error, and the command exits 1.
			var failed bool
			for _, topic := range resp.Topics {
				for _, p := range topic.Partitions {
					failed = failed || p.ErrorCode != 0
				}
			}

			switch cl.Format() {
			case out.FormatJSON:
				out.MarshalJSON(cl.Command(), 1, map[string]any{"partitions": quorumJSON(resp, section)})

			case out.FormatAWK:
				table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "replicas", quorumHeaders...).ErrorColumn()
				for _, r := range quorumRows(resp, section) {
					table.Row(r...)
				}
				return table.Flush()

			default:
				for _, topic := range resp.Topics {
					for _, p := range topic.Partitions {
						tw := out.BeginTabWrite()
						fmt.Fprintf(tw, "TOPIC\t%s\n", topic.Topic)
						fmt.Fprintf(tw, "PARTITION\t%d\n", p.Partition)
						if p.ErrorCode != 0 {
							fmt.Fprintf(tw, "ERROR\t%s\n", out.ErrName(p.ErrorCode))
							if msg := out.BrokerMessage(p.ErrorMessage); msg != "" {
								fmt.Fprintf(tw, "MESSAGE\t%s\n", msg)
							}
							tw.Flush()
							continue
						}
						fmt.Fprintf(tw, "LEADER\t%d\n", p.LeaderID)
						fmt.Fprintf(tw, "LEADER-EPOCH\t%d\n", p.LeaderEpoch)
						fmt.Fprintf(tw, "HIGH-WATERMARK\t%d\n", p.HighWatermark)
						tw.Flush()
						fmt.Println()

						table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "replicas", replicasHeaders...)
						for _, r := range replicaRows(p, section) {
							table.Row(r...)
						}
						table.Flush()
					}
				}
			}
			if failed {
				return out.ErrSilent
			}
			return nil
		},
	}
	out.Columns(cmd, quorumHeaders...)

	cmd.Flags().StringVar(&section, "section", "", "output section (voters, observers; default: all)")
	return cmd
}

// replicaRows are the ROLE REPLICA LOG-END-OFFSET LAST-FETCH-TIMESTAMP
// LAST-CAUGHT-UP-TIMESTAMP rows of one partition, voters then observers,
// each sorted by replica, filtered by section.
func replicaRows(p kmsg.DescribeQuorumResponseTopicPartition, section string) [][]any {
	var rows [][]any
	add := func(role string, replicas []kmsg.DescribeQuorumResponseTopicPartitionReplicaState) {
		replicas = slices.Clone(replicas)
		slices.SortFunc(replicas, func(a, b kmsg.DescribeQuorumResponseTopicPartitionReplicaState) int {
			return int(a.ReplicaID - b.ReplicaID)
		})
		for _, r := range replicas {
			rows = append(rows, []any{role, r.ReplicaID, r.LogEndOffset, r.LastFetchTimestamp, r.LastCaughtUpTimestamp})
		}
	}
	if section != "observers" {
		add("voter", p.CurrentVoters)
	}
	if section != "voters" {
		add("observer", p.Observers)
	}
	return rows
}

// quorumRows are the awk rows: one per replica with the partition columns
// repeated, or one row with the replica columns unknown for a partition
// that errored.
func quorumRows(resp *kmsg.DescribeQuorumResponse, section string) [][]any {
	var rows [][]any
	for _, topic := range resp.Topics {
		for _, p := range topic.Partitions {
			if p.ErrorCode != 0 {
				rows = append(rows, []any{
					topic.Topic, p.Partition, out.Unknown, out.Unknown, out.Unknown,
					out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown,
					out.ErrName(p.ErrorCode), out.BrokerMessage(p.ErrorMessage),
				})
				continue
			}
			for _, r := range replicaRows(p, section) {
				row := []any{topic.Topic, p.Partition, p.LeaderID, p.LeaderEpoch, p.HighWatermark}
				row = append(row, r...)
				rows = append(rows, append(row, "", ""))
			}
		}
	}
	return rows
}

type quorumReplicaJSON struct {
	ReplicaID             int32 `json:"replica_id"`
	LogEndOffset          int64 `json:"log_end_offset"`
	LastFetchTimestamp    int64 `json:"last_fetch_timestamp"`
	LastCaughtUpTimestamp int64 `json:"last_caught_up_timestamp"`
}

// quorumPartitionJSON is one partition. Voters and observers are [] when
// empty, and the section --section did not ask for is left out.
type quorumPartitionJSON struct {
	Topic         string              `json:"topic"`
	Partition     int32               `json:"partition"`
	Leader        int32               `json:"leader"`
	LeaderEpoch   int32               `json:"leader_epoch"`
	HighWatermark int64               `json:"high_watermark"`
	Error         string              `json:"error"`
	Message       string              `json:"message"`
	Voters        []quorumReplicaJSON `json:"voters,omitzero"`
	Observers     []quorumReplicaJSON `json:"observers,omitzero"`
}

func quorumJSON(resp *kmsg.DescribeQuorumResponse, section string) []quorumPartitionJSON {
	parts := []quorumPartitionJSON{}
	replicas := func(rs []kmsg.DescribeQuorumResponseTopicPartitionReplicaState) []quorumReplicaJSON {
		js := make([]quorumReplicaJSON, 0, len(rs))
		for _, r := range rs {
			js = append(js, quorumReplicaJSON{r.ReplicaID, r.LogEndOffset, r.LastFetchTimestamp, r.LastCaughtUpTimestamp})
		}
		slices.SortFunc(js, func(a, b quorumReplicaJSON) int { return int(a.ReplicaID - b.ReplicaID) })
		return js
	}
	for _, topic := range resp.Topics {
		for _, p := range topic.Partitions {
			pj := quorumPartitionJSON{
				Topic: topic.Topic, Partition: p.Partition,
				Leader: p.LeaderID, LeaderEpoch: p.LeaderEpoch,
				HighWatermark: p.HighWatermark,
			}
			pj.Error = out.ErrName(p.ErrorCode)
			pj.Message = out.BrokerMessage(p.ErrorMessage)
			if section != "observers" {
				pj.Voters = replicas(p.CurrentVoters)
			}
			if section != "voters" {
				pj.Observers = replicas(p.Observers)
			}
			parts = append(parts, pj)
		}
	}
	return parts
}
