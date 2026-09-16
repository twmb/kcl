package txn

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
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

func describeProducers(cl *client.Client) *cobra.Command {
	var topicParts []string

	cmd := &cobra.Command{
		Use:     "describe-producers",
		Aliases: []string{"dp"},
		Short:   "Describe active producers.",
		Long: `Describe active producers.

Describe idempotent and transactional producers (Kafka 2.8.0+)

From KIP-664, this command is sent to partition leaders to describe the state
of active idempotent and transactional producers.

For each requested partition, this prints information about the idempotent or
transactional producers producing to the partition.

The information printed:

  ID                   The producer ID of the producer
  EPOCH                The producer epoch of the producer
  LAST SEQUENCE        The last sequence number the producer produced
  LAST TIMESTAMP       The last timestamp the producer produced
  COORDINATOR EPOCH    The epoch of the transactional coordinator for this last produce
  TXN START OFFSET     The first offset of the transaction.

`,
		Example: "kcl txn describe-producers foo:1,2,3 bar:0",
		Args:    cobra.MinimumNArgs(1),

		RunE: func(_ *cobra.Command, _ []string) error {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			if err != nil {
				return fmt.Errorf("unable to parse topic partitions: %v", err)
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

			table := out.NewFormattedTable(cl.Format(), "txn.describe-producers", 1, "producers",
				"TOPIC", "PARTITION", "ERROR", "ID", "EPOCH", "LAST-SEQUENCE", "LAST-TIMESTAMP", "COORDINATOR-EPOCH", "TXN-START-OFFSET")
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						msg := err.Error()
						if partition.ErrorMessage != nil {
							msg += ": " + *partition.ErrorMessage
						}
						table.Row(topic.Topic, partition.Partition, msg, "", "", "", "", "", "")
						continue
					}
					for _, producer := range partition.ActiveProducers {
						table.Row(
							topic.Topic,
							partition.Partition,
							"",
							producer.ProducerID,
							producer.ProducerEpoch,
							producer.LastSequence,
							time.Unix(0, producer.LastTimestamp*1e6).UTC().Format("2006-01-02 15:04:05.999"),
							producer.CoordinatorEpoch,
							producer.CurrentTxnStartOffset,
						)
					}
				}
			}
			table.Flush()
			return nil
		},
	}

	return cmd
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
transaction state or producer ID.
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			kresps := cl.Client().RequestSharded(context.Background(), &kmsg.ListTransactionsRequest{
				StateFilters:      stateFilter,
				ProducerIDFilters: producerIDFilter,
			})

			table := out.NewFormattedTable(cl.Format(), "txn.list", 1, "transactions",
				"BROKER", "TRANSACTIONAL-ID", "PRODUCER-ID", "STATE", "ERROR")
			for _, kresp := range kresps {
				if kresp.Err != nil {
					table.Row(kresp.Meta.NodeID, "", "", "", kresp.Err)
					continue
				}
				resp := kresp.Resp.(*kmsg.ListTransactionsResponse)
				if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
					table.Row(kresp.Meta.NodeID, "", "", "", err)
					continue
				}
				for _, txn := range resp.TransactionStates {
					table.Row(kresp.Meta.NodeID, txn.TransactionalID, txn.ProducerID, txn.TransactionState, "")
				}
			}
			table.Flush()
			return nil
		},
	}

	cmd.Flags().StringArrayVar(&stateFilter, "state", nil, "filter by transaction state (repeatable)")
	cmd.Flags().Int64SliceVar(&producerIDFilter, "producer-id", nil, "filter by producer ID (repeatable)")
	return cmd
}

func describeCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "describe TRANSACTIONAL_IDS...",
		Short: "Describe transactions (Kafka 3.0+).",
		Long: `Describe transactions (Kafka 3.0+).

Describe active transactions by transactional ID (Kafka 3.0+).

This command describes the state of one or more transactions, including
the producer ID, epoch, timeout, and the topics/partitions involved.
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			req := kmsg.NewDescribeTransactionsRequest()
			req.TransactionalIDs = args

			resp, err := req.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to describe transactions: %v", err)
			}

			table := out.NewFormattedTable(cl.Format(), "txn.describe", 1, "transactions",
				"TRANSACTIONAL-ID", "STATE", "PRODUCER-ID", "PRODUCER-EPOCH", "TIMEOUT-MS", "START-TIMESTAMP", "TOPICS", "ERROR")
			anyErr := false
			for _, txn := range resp.TransactionStates {
				if err := kerr.ErrorForCode(txn.ErrorCode); err != nil {
					anyErr = true
					table.Row(txn.TransactionalID, "", noNum, noNum, noNum, noTime, "", err.Error())
					continue
				}
				var topics []string
				for _, topic := range txn.Topics {
					topics = append(topics, fmt.Sprintf("%s%v", topic.Topic, topic.Partitions))
				}
				sort.Strings(topics)
				table.Row(
					txn.TransactionalID,
					txn.State,
					num(int64(txn.ProducerID)),
					num(int64(txn.ProducerEpoch)),
					num(int64(txn.TimeoutMillis)),
					millis(txn.StartTimestamp),
					strings.Join(topics, ", "),
					"",
				)
			}
			table.Flush()
			if anyErr {
				return out.ErrSilent
			}
			return nil
		},
	}
}

// A row that failed carries no numbers, but the table renders one value into
// all three formats, so the column types have to travel with the value. num
// and millis print what a person reads and encode what a script parses: an
// empty cell in text and awk, a JSON number or null.
var (
	noNum  = optNum{}
	noTime = optMillis{}
)

func num(v int64) optNum       { return optNum{v: v, ok: true} }
func millis(v int64) optMillis { return optMillis{v: v, ok: true} }

type optNum struct {
	v  int64
	ok bool
}

func (o optNum) String() string {
	if !o.ok {
		return ""
	}
	return strconv.FormatInt(o.v, 10)
}

func (o optNum) MarshalJSON() ([]byte, error) {
	if !o.ok {
		return []byte("null"), nil
	}
	return strconv.AppendInt(nil, o.v, 10), nil
}

// optMillis is a unix millisecond timestamp. Text and awk get the UTC time,
// JSON gets the milliseconds themselves.
type optMillis struct {
	v  int64
	ok bool
}

func (o optMillis) String() string {
	if !o.ok {
		return ""
	}
	return time.Unix(0, o.v*1e6).UTC().Format("2006-01-02 15:04:05.999")
}

func (o optMillis) MarshalJSON() ([]byte, error) {
	if !o.ok {
		return []byte("null"), nil
	}
	return strconv.AppendInt(nil, o.v, 10), nil
}
