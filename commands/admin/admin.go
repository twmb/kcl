// Package admin contains admin commands.
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
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
		Use:     "admin",
		Aliases: []string{"adm", "a"},
		Short:   "Admin utility commands.",
	}

	cmd.AddCommand(
		deleteRecordsCommand(cl),
		electLeaderCommand(cl),
		describeClusterCommand(cl),
		describeQuorumCommand(cl),

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

func electLeaderCommand(cl *client.Client) *cobra.Command {
	var allPartitions bool
	var unclean bool
	var run bool

	cmd := &cobra.Command{
		Use:   "elect-leaders",
		Short: "Trigger leader elections for partitions",
		Long: `Trigger leader elections for topic partitions (Kafka 2.2.0+).

This command allows for triggering leader elections on any topic and any
partition, as well as on all topic partitions. To run on all, you must not
pass any topic flags, and you must use the --all-partitions flag.

The format for triggering topic partitions is "foo:1,2,3", where foo is a
topic and 1,2,3 are partition numbers.

To avoid accidental triggers, this command requires a --run flag to run.
`,
		Example: "elect-leaders --run foo:1,2,3 bar:9",
		Run: func(_ *cobra.Command, topicParts []string) {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			out.MaybeDie(err, "unable to parse topic partitions: %v", err)
			if !run {
				out.Die("use --run to actually run this command")
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
				out.Die("no topics requested for leader election, and not triggering all; nothing to do")
			}

			for topic, partitions := range tps {
				req.Topics = append(req.Topics, kmsg.ElectLeadersRequestTopic{
					Topic:      topic,
					Partitions: partitions,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to elect leaders: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			resp := kresp.(*kmsg.ElectLeadersResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.Die("%v", err)
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

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
					fmt.Fprintf(tw, "%s\t%d\t%v\t%s\n",
						topic.Topic,
						partition.Partition,
						errKind,
						msg,
					)
				}
			}
		},
	}

	cmd.Flags().BoolVar(&allPartitions, "all-partitions", false, "trigger leader election on all topics for all partitions")
	cmd.Flags().BoolVar(&unclean, "unclean", false, "allow unclean leader election (Kafka 2.4.0+)")
	cmd.Flags().BoolVar(&run, "run", false, "actually run the command (avoids accidental elections without this flag)")

	return cmd
}

func deleteRecordsCommand(cl *client.Client) *cobra.Command {
	var jsonFile string

	cmd := &cobra.Command{
		Use:   "delete-records",
		Short: "Delete records for topics.",
		Long: `Delete records for topics (Kafka 0.11.0+).

Normally, Kafka records are deleted based off time or log file size. Sometimes,
that does not play well with processing. To address this limitation, Kafka
added record deleting in 0.11.0.

Deleting records works by deleting all segments whose end offsets are earlier
than the requested delete offset, and then disallowing reads earlier than the
requested start offset in a delete request.

This function works either by reading file of record offsets to delete or by
parsing the passed args. The arg format is topic:p#,o# (hopefully obvious).
The file format is as follows:

  [
    {
      "topic": "string",
      "partition": int32,
      "offset": int64
    },
    ...
  ]

It is possible to use both args and the file.

Record deletion works on a fan out basis: each broker containing partitions for
record deletion needs to be issued a request. This does that appropriately.
`,

		Example: `delete-records foo:p0,o120 foo:p1,o3888
		
delete-records --json-file records.json`,

		Run: func(_ *cobra.Command, args []string) {
			tpos, err := parseTopicPartitionOffsets(args)
			out.MaybeDie(err, "unable to parse topic partition offsets: %v", err)

			if jsonFile != "" {
				type fileReq struct {
					Topic     string `json:"topic"`
					Partition int32  `json:"partition"`
					Offset    int64  `json:"offset"`
				}
				var fileReqs []fileReq
				raw, err := os.ReadFile(jsonFile)
				out.MaybeDie(err, "unable to read json file: %v", err)
				err = json.Unmarshal(raw, &fileReqs)
				out.MaybeDie(err, "unable to unmarshal json file: %v", err)
				for _, fileReq := range fileReqs {
					tpos[fileReq.Topic] = append(tpos[fileReq.Topic], partitionOffset{fileReq.Partition, fileReq.Offset})
				}
			}

			if len(tpos) == 0 {
				out.Die("no records requested for deletion")
			}

			// Create and fire off a bunch of concurrent requests...
			type respErr struct {
				broker int32
				resp   kmsg.Response
				err    error
			}
			req := &kmsg.DeleteRecordsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for topic, partitionOffsets := range tpos {
				reqTopic := kmsg.DeleteRecordsRequestTopic{
					Topic: topic,
				}
				for _, partitionOffset := range partitionOffsets {
					reqTopic.Partitions = append(reqTopic.Partitions, kmsg.DeleteRecordsRequestTopicPartition{
						Partition: partitionOffset.partition,
						Offset:    partitionOffset.offset,
					})
				}
				req.Topics = append(req.Topics, reqTopic)
			}

			brokerResps := cl.Client().RequestSharded(context.Background(), req)

			tw := out.BeginTabWrite()
			defer tw.Flush()

			for _, brokerResp := range brokerResps {
				fmt.Fprintf(tw, "BROKER\tTOPIC\tPARTITION\tNEW LOW WATERMARK\tERROR\n")

				kresp, err := brokerResp.Resp, brokerResp.Err
				if err != nil {
					fmt.Fprintf(tw, "%d\t\t\t\t%s\n",
						brokerResp.Meta.NodeID, fmt.Sprintf("unable to issue request: %s", err.Error()))
					continue
				}

				resp := kresp.(*kmsg.DeleteRecordsResponse)

				for _, topic := range resp.Topics {
					for _, partition := range topic.Partitions {
						msg := "OK"
						if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
							msg = err.Error()
						}
						fmt.Fprintf(tw, "%d\t%s\t%d\t%d\t%s\n",
							brokerResp.Meta.NodeID, topic.Topic, partition.Partition, partition.LowWatermark, msg)
					}
				}
			}
		},
	}

	cmd.Flags().StringVar(&jsonFile, "json-file", "", "if non-empty, a json file to read deletions from")

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
			return nil, fmt.Errorf("item %q invalid empty partition offsets", item)
		}

		matches := rePo.FindStringSubmatch(split[1])
		if len(matches) == 0 {
			return nil, fmt.Errorf("item %q partition offset %q does not match p(\\d+)o(-?\\d+)", item, split[1])
		}
		partition, _ := strconv.Atoi(matches[1])
		offset, _ := strconv.Atoi(matches[2])

		tpos[split[0]] = append(tpos[split[0]], partitionOffset{int32(partition), int64(offset)})
	}
	return tpos, nil
}

func describeClusterCommand(cl *client.Client) *cobra.Command {
	var includeAuthorizedOps bool

	cmd := &cobra.Command{
		Use:   "describe-cluster",
		Short: "Describe the Kafka cluster (Kafka 3.0+).",
		Long: `Describe the Kafka cluster (Kafka 3.0+).

This command prints the cluster ID, controller ID, and a table of all brokers
in the cluster.
`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			req := kmsg.NewPtrDescribeClusterRequest()
			req.IncludeClusterAuthorizedOperations = includeAuthorizedOps

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to describe cluster: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			resp := kresp.(*kmsg.DescribeClusterResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.ErrAndMsg(resp.ErrorCode, resp.ErrorMessage)
				out.Exit()
			}

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
		},
	}

	cmd.Flags().BoolVar(&includeAuthorizedOps, "include-authorized-ops", false, "include cluster authorized operations in the response")
	return cmd
}

func describeQuorumCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "describe-quorum",
		Short: "Describe the KRaft quorum (Kafka 3.0+).",
		Long: `Describe the KRaft quorum (Kafka 3.0+).

This command describes the quorum status for the __cluster_metadata partition,
including the leader, epoch, high watermark, and information about voters
and observers.
`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			req := kmsg.NewPtrDescribeQuorumRequest()
			req.Topics = []kmsg.DescribeQuorumRequestTopic{{
				Topic:      "__cluster_metadata",
				Partitions: []kmsg.DescribeQuorumRequestTopicPartition{{Partition: 0}},
			}}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to describe quorum: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			resp := kresp.(*kmsg.DescribeQuorumResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.ErrAndMsg(resp.ErrorCode, resp.ErrorMessage)
				out.Exit()
			}

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
		},
	}
}
