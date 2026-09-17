package txn

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "txn",
		Short: "Commands related to transaction information.",
	}
	cmd.AddCommand(describeProducers(cl))
	cmd.AddCommand(listCommand(cl))
	cmd.AddCommand(describeCommand(cl))
	return cmd
}

var (
	producersHeaders = []string{"TOPIC", "PARTITION", "PRODUCER-ID", "PRODUCER-EPOCH", "LAST-SEQUENCE", "LAST-TIMESTAMP", "COORDINATOR-EPOCH", "TXN-START-OFFSET", "ERROR", "MESSAGE"}
	listHeaders      = []string{"BROKER", "TRANSACTIONAL-ID", "PRODUCER-ID", "STATE", "ERROR"}
	describeHeaders  = []string{"TRANSACTIONAL-ID", "STATE", "PRODUCER-ID", "PRODUCER-EPOCH", "TIMEOUT-MS", "START-TIMESTAMP", "TOPICS", "ERROR"}
)

func describeProducers(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "describe-producers TOPIC:P...",
		Aliases: []string{"dp"},
		Short:   "Describe active producers.",
		Long: `Describe active producers.

Describe idempotent and transactional producers (Kafka 2.8.0+).

From KIP-664, this command is sent to partition leaders to describe the state
of active idempotent and transactional producers.

For each requested partition, this prints information about the idempotent or
transactional producers producing to the partition. A topic given without
partitions is every partition of the topic. Rows are sorted by topic,
partition, and producer ID.

The information printed:

  PRODUCER-ID          The producer ID of the producer
  PRODUCER-EPOCH       The producer epoch of the producer
  LAST-SEQUENCE        The last sequence number the producer produced
  LAST-TIMESTAMP       The last timestamp the producer produced, UTC (milliseconds in json)
  COORDINATOR-EPOCH    The epoch of the transactional coordinator for this last produce
  TXN-START-OFFSET     The first offset of the transaction
  ERROR                Why a partition could not be described, else empty
  MESSAGE              The text the broker attached to the error, else empty

EXAMPLES:
  kcl txn describe-producers foo:1,2,3 bar:0
  kcl txn describe-producers foo             # every partition of foo

SEE ALSO:
  kcl txn list        list transactions
  kcl txn describe    describe transactions by ID
`,
		Args: cobra.MinimumNArgs(1),

		RunE: func(_ *cobra.Command, topicParts []string) error {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			if err != nil {
				return out.Errf(out.ExitUsage, "unable to parse topic partitions: %v", err)
			}

			var metaTopics []kmsg.MetadataRequestTopic
			for topic, partitions := range tps {
				if len(partitions) == 0 {
					metaTopics = append(metaTopics, kmsg.MetadataRequestTopic{Topic: kmsg.StringPtr(topic)})
				}
			}
			if len(metaTopics) > 0 {
				resp, err := (&kmsg.MetadataRequest{Topics: metaTopics}).RequestWith(context.Background(), cl.Client())
				if err != nil {
					return fmt.Errorf("unable to get metadata: %v", err)
				}
				for _, topic := range resp.Topics {
					if topic.Topic == nil {
						return fmt.Errorf("metadata returned nil topic when we did not fetch with topic IDs")
					}
					for _, partition := range topic.Partitions {
						tps[*topic.Topic] = append(tps[*topic.Topic], partition.Partition)
					}
				}
			}

			req := kmsg.NewDescribeProducersRequest()
			for topic, partitions := range tps {
				req.Topics = append(req.Topics, kmsg.DescribeProducersRequestTopic{
					Topic:      topic,
					Partitions: partitions,
				})
			}

			resp, err := req.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to describe producers: %v", err)
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "producers", producersHeaders...).ErrorColumn()
			for _, r := range producerRows(resp) {
				table.Row(r...)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, producersHeaders...)
	return cmd
}

// producerRows flattens a DescribeProducers response into rows sorted by
// topic, partition, and producer ID. A partition that errored is one row
// with the producer columns unknown and ERROR set.
func producerRows(resp *kmsg.DescribeProducersResponse) [][]any {
	type row struct {
		topic     string
		partition int32
		producer  int64
		cells     []any
	}
	var rows []row
	for _, topic := range resp.Topics {
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != 0 {
				rows = append(rows, row{topic.Topic, partition.Partition, -1, []any{
					topic.Topic, partition.Partition,
					out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown,
					out.ErrName(partition.ErrorCode), out.BrokerMessage(partition.ErrorMessage),
				}})
				continue
			}
			for _, p := range partition.ActiveProducers {
				rows = append(rows, row{topic.Topic, partition.Partition, p.ProducerID, []any{
					topic.Topic,
					partition.Partition,
					p.ProducerID,
					p.ProducerEpoch,
					p.LastSequence,
					out.Millis(p.LastTimestamp),
					p.CoordinatorEpoch,
					p.CurrentTxnStartOffset,
					"", "",
				}})
			}
		}
	}
	slices.SortFunc(rows, func(a, b row) int {
		if a.topic != b.topic {
			return strings.Compare(a.topic, b.topic)
		}
		if a.partition != b.partition {
			return int(a.partition - b.partition)
		}
		return int(a.producer - b.producer)
	})
	cells := make([][]any, len(rows))
	for i, r := range rows {
		cells[i] = r.cells
	}
	return cells
}

func listCommand(cl *client.Client) *cobra.Command {
	var stateFilter []string
	var producerIDFilter []int64

	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List active transactions (Kafka 3.0+).",
		Long: `List active transactions (Kafka 3.0+).

List active transactions across all brokers (Kafka 3.0+).

This command lists all ongoing transactions. You can optionally filter by
transaction state or producer ID. Rows are sorted by transactional ID. A
broker that could not answer is one row with ERROR set.

EXAMPLES:
  kcl txn list
  kcl txn list --state Ongoing --state PrepareCommit

SEE ALSO:
  kcl txn describe              describe transactions by ID
  kcl txn describe-producers    describe producers per partition
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			kresps := cl.Client().RequestSharded(context.Background(), &kmsg.ListTransactionsRequest{
				StateFilters:      stateFilter,
				ProducerIDFilters: producerIDFilter,
			})

			type row struct {
				broker int32
				txnID  string
				cells  []any
			}
			var rows []row
			for _, kresp := range kresps {
				b := kresp.Meta.NodeID
				if kresp.Err != nil {
					rows = append(rows, row{b, "", []any{b, out.Unknown, out.Unknown, out.Unknown, out.ErrCell(kresp.Err)}})
					continue
				}
				resp := kresp.Resp.(*kmsg.ListTransactionsResponse)
				if resp.ErrorCode != 0 {
					rows = append(rows, row{b, "", []any{b, out.Unknown, out.Unknown, out.Unknown, out.ErrName(resp.ErrorCode)}})
					continue
				}
				for _, txn := range resp.TransactionStates {
					rows = append(rows, row{b, txn.TransactionalID, []any{b, txn.TransactionalID, txn.ProducerID, txn.TransactionState, ""}})
				}
			}
			slices.SortFunc(rows, func(a, b row) int {
				if a.txnID != b.txnID {
					return strings.Compare(a.txnID, b.txnID)
				}
				return int(a.broker - b.broker)
			})

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "transactions", listHeaders...).ErrorColumn()
			for _, r := range rows {
				table.Row(r.cells...)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, listHeaders...)

	cmd.Flags().StringArrayVar(&stateFilter, "state", nil, "filter by transaction state (repeatable)")
	cmd.Flags().Int64SliceVar(&producerIDFilter, "producer-id", nil, "filter by producer ID (repeatable)")
	return cmd
}

func describeCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe TRANSACTIONAL_IDS...",
		Short: "Describe transactions (Kafka 3.0+).",
		Long: `Describe transactions (Kafka 3.0+).

Describe active transactions by transactional ID (Kafka 3.0+).

This command describes the state of one or more transactions, including
the producer ID, epoch, timeout, and the topics/partitions involved. Rows
are sorted by transactional ID.

START-TIMESTAMP is when the transaction began, in UTC (milliseconds in
json). TOPICS is the partitions in the transaction: foo:0,1;bar:2 in text
and awk, and an array of {topic, partitions} in JSON.

EXAMPLES:
  kcl txn describe my-app-txn other-txn

SEE ALSO:
  kcl txn list                  list transactions
  kcl txn describe-producers    describe producers per partition
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			req := kmsg.NewDescribeTransactionsRequest()
			req.TransactionalIDs = args

			resp, err := req.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to describe transactions: %v", err)
			}

			states := slices.Clone(resp.TransactionStates)
			slices.SortFunc(states, func(a, b kmsg.DescribeTransactionsResponseTransactionState) int {
				return strings.Compare(a.TransactionalID, b.TransactionalID)
			})
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "transactions", describeHeaders...).ErrorColumn()
			for _, txn := range states {
				if txn.ErrorCode != 0 {
					table.Row(txn.TransactionalID, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.ErrName(txn.ErrorCode))
					continue
				}
				table.Row(
					txn.TransactionalID,
					txn.State,
					txn.ProducerID,
					txn.ProducerEpoch,
					txn.TimeoutMillis,
					out.Millis(txn.StartTimestamp),
					txnTopics(txn.Topics),
					"",
				)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, describeHeaders...)
	return cmd
}

// txnTopic is one topic in a transaction.
type txnTopic struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

// txnTopics is the TOPICS cell: foo:0,1;bar:2 in text and awk, an array
// of {topic, partitions} in JSON, sorted by topic and then partition.
type txnTopics []kmsg.DescribeTransactionsResponseTransactionStateTopic

func (ts txnTopics) sorted() []txnTopic {
	sorted := make([]txnTopic, 0, len(ts))
	for _, t := range ts {
		partitions := slices.Clone(t.Partitions)
		slices.Sort(partitions)
		if partitions == nil {
			partitions = []int32{}
		}
		sorted = append(sorted, txnTopic{t.Topic, partitions})
	}
	slices.SortFunc(sorted, func(a, b txnTopic) int { return strings.Compare(a.Topic, b.Topic) })
	return sorted
}

func (ts txnTopics) String() string {
	var strs []string
	for _, t := range ts.sorted() {
		parts := make([]string, len(t.Partitions))
		for i, p := range t.Partitions {
			parts[i] = strconv.FormatInt(int64(p), 10)
		}
		strs = append(strs, t.Topic+":"+strings.Join(parts, ","))
	}
	return strings.Join(strs, ";")
}

func (ts txnTopics) MarshalJSON() ([]byte, error) {
	return json.Marshal(ts.sorted())
}
