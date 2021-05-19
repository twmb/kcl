package txn

import (
	"context"
	"fmt"
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
	cmd.AddCommand(unstickLSO(cl))
	return cmd
}

func describeProducers(cl *client.Client) *cobra.Command {
	var topicParts []string

	cmd := &cobra.Command{
		Use:     "describe-producers",
		Aliases: []string{"dp"},
		Short:   "Describe producers quotas.",
		Long: `Describe idempotent and transactional producers (Kafka 2.8.0+)

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
		Example: "describe-producers foo:1,2,3 bar:0",
		Args:    cobra.MinimumNArgs(1),

		Run: func(_ *cobra.Command, _ []string) {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			out.MaybeDie(err, "unable to parse topic partitions: %v", err)

			var metaTopics []kmsg.MetadataRequestTopic
			for topic, partitions := range tps {
				if len(partitions) == 0 {
					metaTopics = append(metaTopics, kmsg.MetadataRequestTopic{Topic: kmsg.StringPtr(topic)})
				}
			}
			if len(metaTopics) > 0 {
				resp, err := (&kmsg.MetadataRequest{Topics: metaTopics}).RequestWith(context.Background(), cl.Client())
				out.MaybeDie(err, "unable to get metadata: %v", err)
				for _, topic := range resp.Topics {
					for _, partition := range topic.Partitions {
						tps[topic.Topic] = append(tps[topic.Topic], partition.Partition)
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
			out.MaybeDie(err, "unable to describe producers: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(resp)
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "TOPIC\tPARTITION\tERROR\tID\tEPOCH\tLAST SEQUENCE\tLAST TIMESTAMP\tCOORDINATOR EPOCH\tTXN START OFFSET\n")
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						fmt.Fprintf(tw, "%s\t%d\t%s\t\t\t\t\t\t\n", topic.Topic, partition.Partition, err)
						continue
					}
					for _, producer := range partition.ActiveProducers {
						fmt.Fprintf(tw, "%s\t%d\t\t%d\t%d\t%d\t%s\t%d\t%d\n",
							topic.Topic,
							partition.Partition,
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
		},
	}

	return cmd
}
