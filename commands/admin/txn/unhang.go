package txn

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func unstickLSO(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "unstick-lso",
		Aliases: []string{"unhang-lso"},
		Short:   "Fix a topic that has a stuck LastStableOffset.",

		Long: `Fix a topic that has a stuck LastStableOffset.

Depending on a certain sequence of pathologic / buggy events while
transactional producing, a topic's LastStableOffset can get permanently stuck.
This command aims to un-stick that LSO.

This command looks for the producer ID, epoch, and transactional ID that is
causing the stuck LSO. Once finding it, this prompts to unstick the LSO by
faking a transaction. If the transactional ID was reused in a new producer, the
epoch will have already been fenced, and this command will prompt to retry with
a higher epoch. This is safe to do if you know your transactional ID is no
longer in use (if it is still in use, it is likely you do not have a stuck LSO
anyway).

`,
		Example: "unstick-lso",
		Args:    cobra.ExactArgs(1),

		Run: func(_ *cobra.Command, topics []string) {
			unstick(cl.Client(), topics[0])
		},
	}
}

func unstick(cl *kgo.Client, topic string) {
	defer cl.Close()

	var nparts int
	{
		fmt.Printf("Issuing Metadata request for the topic to determine its number of partitions...\n")
		req := kmsg.NewMetadataRequest()
		reqTopic := kmsg.NewMetadataRequestTopic()
		reqTopic.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, reqTopic)

		resp, err := req.RequestWith(context.Background(), cl)
		out.MaybeDie(err, "unable to issue metadata request: %v", err)

		if len(resp.Topics) != 1 {
			out.Die("metadata response unexpectedly returned %d topics when we asked for 1", len(resp.Topics))
		}
		t := resp.Topics[0]
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			out.Die("metadata for topic %s has error, unable to determine the number of partitions in the topic. error: %v", err)
		}
		nparts = len(t.Partitions)
	}

	possibleParts := make(map[int32]int64)
	{
		fmt.Printf("Metadata indicates that there are %d partitions in the topic. Issuing ListOffsets request to determine which partitions may be stuck (i.e., have records)...\n", nparts)
		req := kmsg.NewPtrListOffsetsRequest()
		reqTopic := kmsg.NewListOffsetsRequestTopic()
		reqTopic.Topic = topic
		for i := 0; i < nparts; i++ {
			reqPart := kmsg.NewListOffsetsRequestTopicPartition()
			reqPart.Partition = int32(i)
			reqPart.Timestamp = -1
			reqTopic.Partitions = append(reqTopic.Partitions, reqPart)
		}
		req.Topics = append(req.Topics, reqTopic)

		// First get the end offsets,
		for _, shard := range cl.RequestSharded(context.Background(), req) {
			if shard.Err != nil {
				out.Die("unable to issue ListOffsets request to broker %d: %v", shard.Meta.NodeID, shard.Err)
			}
			resp := shard.Resp.(*kmsg.ListOffsetsResponse)
			if len(resp.Topics) != 1 {
				out.Die("ListOffsets response unexpectedly returned %d topics when we asked for 1", len(resp.Topics))
			}
			t := resp.Topics[0]
			for _, p := range t.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
					out.Die("unable to list offsets for partition %d: %v", p.Partition, err)
				}
				possibleParts[p.Partition] = p.Offset
			}
		}

		for i := range reqTopic.Partitions {
			reqTopic.Partitions[i].Timestamp = -2
		}

		// now subtract out the beginning offsets,
		for _, shard := range cl.RequestSharded(context.Background(), req) {
			if shard.Err != nil {
				out.Die("unable to issue ListOffsets request to broker %d: %v", shard.Meta.NodeID, shard.Err)
			}
			resp := shard.Resp.(*kmsg.ListOffsetsResponse)
			if len(resp.Topics) != 1 {
				out.Die("ListOffsets response unexpectedly returned %d topics when we asked for 1", len(resp.Topics))
			}
			t := resp.Topics[0]
			for _, p := range t.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
					out.Die("unable to list offsets for partition %d: %v", p.Partition, err)
				}
				possibleParts[p.Partition] -= p.Offset
			}
		}

		// anything remaining has records.
		for p, o := range possibleParts {
			if o == 0 {
				delete(possibleParts, p)
			}
		}

		if len(possibleParts) == 0 {
			fmt.Printf("No partition in the topic has records; thus nothing can be stuck.\nExiting\n")
			return
		}
	}

	var stuckPartition int32
	var stuckOffset int64
	{
		// First find the stuck LSO.
		fmt.Printf("Looking for the last stable offset on a partition in %s...\n", topic)
		cl.AssignPartitions(kgo.ConsumeTopics(kgo.NewOffset().AtStart(), topic))
		var found bool
		for !found && len(possibleParts) > 0 {
			fetches := cl.PollFetches(context.Background())
			if errs := fetches.Errors(); len(errs) > 0 {
				out.Die("fetch errors while looking for stuck partition: %v", errs)
			}
			fetches.EachPartition(func(tp kgo.FetchTopicPartition) {
				p := tp.Partition
				delete(possibleParts, p.Partition)
				if p.HighWatermark == p.LastStableOffset {
					return
				}
				found = true
				stuckPartition = p.Partition
				stuckOffset = p.LastStableOffset
			})
		}

		if !found && len(possibleParts) == 0 {
			fmt.Printf("There do not appear to be any stuck partitions on topic %s; all partitions have a HighWatermark equal to the LastStableOffset.\nExiting.\n", topic)
			return
		}
	}

	var pid int64
	var epoch int16
	{
		// Now we directly consume at that offset to find which
		// producer ID and epoch caused this.
		fmt.Printf("Found partition %d stuck at offset %d, looking for producer that caused this...\n", stuckPartition, stuckOffset)
		cl.AssignPartitions(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: map[int32]kgo.Offset{
				stuckPartition: kgo.NewOffset().At(stuckOffset),
			},
		}))
		var found bool
		for !found {
			fetches := cl.PollFetches(context.Background())
			if errs := fetches.Errors(); len(errs) > 0 {
				out.Die("fetch errors while looking for stuck producer ID: %v", errs)
			}
			fetches.EachRecord(func(r *kgo.Record) {
				if !found { // the first record is culprit
					if r.Offset != stuckOffset {
						out.Die("unable to consume at stuck offset %d, first seen record is %d", stuckOffset, r.Offset)
					}
					found = true
					pid = r.ProducerID
					epoch = r.ProducerEpoch
				}
			})
		}
	}

	var txnid string
	{
		fmt.Printf("Found causing producer ID and epoch %d/%d, looking in __transaction_state to find its transactional id...\n", pid, epoch)
		cl.AssignPartitions(kgo.ConsumeTopics(kgo.NewOffset().AtStart(), "__transaction_state"))
		var found bool
		for !found {
			fetches := cl.PollFetches(context.Background())
			fetches.EachRecord(func(r *kgo.Record) {
				if len(r.Value) == 0 {
					return
				}

				var v kmsg.TxnMetadataValue
				if err := v.ReadFrom(r.Value); err != nil {
					out.Die("unable to read TxnMetadataValue: %v", err)
				}

				if v.ProducerID != pid || v.ProducerEpoch != epoch {
					return
				}

				var k kmsg.TxnMetadataKey
				if err := k.ReadFrom(r.Key); err != nil {
					out.Die("unable to read TxnMetadataKey: %v", err)
				}

				found = true
				txnid = k.TransactionalID
			})
		}
		cl.AssignPartitions() // stop consuming, drop buffered data
	}

	{
		fmt.Printf("\nFoudn topic %s stuck with transactional id %s.\nAt this point we can attempt to unhang the transaction.\nIssue AddPartitionsToTxn followed by EndTxn? [Y/n] ", topic, txnid)
		var s string
		fmt.Scanln(&s)
		switch strings.ToLower(s) {
		case "y", "yes":
			fmt.Printf("Proceeding to unstick...\n\n")
		default:
			fmt.Printf("Saw %s, not yes, exiting.\n", s)
			return
		}
	}

	{
	start:
		fmt.Println("Issuing AddPartitionsToTxn request to fake a new transaction...")
		req := kmsg.NewAddPartitionsToTxnRequest()
		req.TransactionalID = txnid
		req.ProducerID = pid
		req.ProducerEpoch = epoch

		reqTopic := kmsg.NewAddPartitionsToTxnRequestTopic()
		reqTopic.Topic = topic
		for i := 0; i < nparts; i++ {
			reqTopic.Partitions = append(reqTopic.Partitions, int32(i))
		}

		req.Topics = append(req.Topics, reqTopic)

		resp, err := req.RequestWith(context.Background(), cl)
		out.MaybeDie(err, "unable to issue AddPartitionsToTxn request: %v", err)

		for _, topic := range resp.Topics {
			for _, partition := range topic.Partitions {
				if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
					if err == kerr.ProducerFenced {
						fmt.Printf("\nProducer at epoch %d was fenced, should we retry with a higher epoch? (If you are sure the txnal ID is no longer in use, this is fine) [Y/n] ", epoch)
						var s string
						fmt.Scanln(&s)
						switch strings.ToLower(s) {
						case "y", "yes":
							epoch++
						default:
							fmt.Printf("Saw %s, not yes, exiting.\n", s)
							return
						}

						goto start
					}

					out.Die("partition in AddPartitionsToTxn had error: %v", err)
				}
			}
		}
	}

	{
		fmt.Printf("Fake transaction began, issuing EndTxn request to end it...\n")
		req := kmsg.NewEndTxnRequest()
		req.TransactionalID = txnid
		req.ProducerID = pid
		req.ProducerEpoch = epoch

		resp, err := req.RequestWith(context.Background(), cl)
		out.MaybeDie(err, "unable to issue EndTxn request: %v", err)
		if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
			out.Die("EndTxn request had error: %v", err)
		}
	}

	fmt.Println("Complete.")
}
