package partas

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

var verifyHeaders = []string{"TOPIC", "PARTITION", "CURRENT-REPLICAS", "TARGET-REPLICAS", "STATUS", "ERROR", "MESSAGE"}

// The STATUS a verify row prints; a partition the cluster did not answer
// for is out.Unknown.
const (
	statusInProgress = "in-progress"
	statusComplete   = "complete"
	statusDiffers    = "differs"
)

// verifyRow is one partition of the plan against the cluster.
type verifyRow struct {
	topic     string
	partition int32
	current   any // []int32, or out.Unknown when err is set
	target    []int32
	status    any // a status constant, or out.Unknown when err is set
	err       any // the error the cluster answered, or "" as ErrorColumn wants
	msg       *string
}

// verifyRows is one row per partition of the plan, sorted by topic and
// partition. A partition ListPartitionReassignments still lists is in
// progress; otherwise its replicas in the metadata are compared with the
// plan as sets. A partition the metadata answers with an error, or does not
// list, has an unknown status.
func verifyRows(plan map[string]map[int32][]int32, listResp *kmsg.ListPartitionReassignmentsResponse, metaResp *kmsg.MetadataResponse) []verifyRow {
	inProgress := make(map[string]map[int32]bool)
	for _, t := range listResp.Topics {
		inProgress[t.Topic] = make(map[int32]bool)
		for _, p := range t.Partitions {
			inProgress[t.Topic][p.Partition] = true
		}
	}
	type partition struct {
		replicas []int32
		err      error
	}
	topicErrs := make(map[string]error)
	partitions := make(map[string]map[int32]partition)
	for _, t := range metaResp.Topics {
		if t.Topic == nil {
			continue
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			topicErrs[*t.Topic] = err
			continue
		}
		partitions[*t.Topic] = make(map[int32]partition)
		for _, p := range t.Partitions {
			partitions[*t.Topic][p.Partition] = partition{p.Replicas, kerr.ErrorForCode(p.ErrorCode)}
		}
	}

	var rows []verifyRow
	for _, topic := range slices.Sorted(maps.Keys(plan)) {
		for _, p := range slices.Sorted(maps.Keys(plan[topic])) {
			row := verifyRow{topic: topic, partition: p, target: sortedSet(plan[topic][p]), err: ""}
			listErr := kerr.ErrorForCode(listResp.ErrorCode)
			meta, listed := partitions[topic][p]
			switch {
			case listErr != nil:
				row.err, row.msg = listErr, listResp.ErrorMessage
			case topicErrs[topic] != nil:
				row.err = topicErrs[topic]
			case !listed:
				row.err = kerr.UnknownTopicOrPartition
			case meta.err != nil:
				row.err = meta.err
			}
			if row.err != "" {
				row.current, row.status = out.Unknown, out.Unknown
				rows = append(rows, row)
				continue
			}
			row.current = sortedSet(meta.replicas)
			switch {
			case inProgress[topic][p]:
				row.status = statusInProgress
			case slices.Equal(row.current.([]int32), row.target):
				row.status = statusComplete
			default:
				row.status = statusDiffers
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func verifyPartitionReassignments(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify 'TOPIC:P->R,R...'...",
		Short: "Verify partition reassignments landed, and clear their throttle.",
		Long: `Verify partition reassignments landed, and clear their throttle.

Verify takes the plan "kcl reassign alter" took and prints one row per
partition saying whether the move is done (Kafka 2.4.0+). The syntax for
each topic is

  topic: 1->2,3,4 ; 2->1,2,3

where the first number is the partition, and -> points to the replicas the
partition was moved to. Note that since this contains a >, you likely need
to quote your input.

STATUS is one of

  in-progress  the cluster still lists the partition as being reassigned
  complete     the partition's replicas are the target
  differs      the partition's replicas are not the target

Replicas are compared as a set: the order the plan lists them in is not
compared, since the cluster may reorder a replica list. A partition the
cluster answers with an error has an unknown STATUS, with the error in
ERROR and MESSAGE.

The command exits 0 only when every row is complete, so a script can loop
on it until the move lands:

  until kcl reassign verify 'foo:0->2,3'; do sleep 10; done

When every row is complete, the replication throttle "kcl reassign alter
--throttle" set is cleared, the way kafka-reassign-partitions.sh --verify
clears it: leader.replication.throttled.rate and
follower.replication.throttled.rate are deleted on every broker the plan
names, current and target, and leader.replication.throttled.replicas and
follower.replication.throttled.replicas are deleted on every topic in the
plan. Nothing is cleared while any partition is in progress or differs. A
throttle that was never set is deleted all the same, which is a no-op.
Text says what was cleared on stderr; JSON carries it under
throttles_cleared.

EXAMPLES:
  kcl reassign verify 'foo:1->1,2,3' 'bar:2->3,4,5;5->3,4,5'
  kcl reassign verify 'foo:0->2,3' --format json | jq .throttles_cleared

SEE ALSO:
  kcl reassign alter     start a reassignment
  kcl reassign list      list reassignments in progress
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, topicPartReplicas []string) error {
			plan, err := flagutil.ParseTopicPartitionReplicas(topicPartReplicas)
			if err != nil {
				return out.Errf(out.ExitUsage, "unable to parse topic partitions replicas: %v", err)
			}

			listReq := &kmsg.ListPartitionReassignmentsRequest{TimeoutMillis: cl.TimeoutMillis()}
			metaReq := kmsg.NewPtrMetadataRequest()
			for _, topic := range slices.Sorted(maps.Keys(plan)) {
				listReq.Topics = append(listReq.Topics, kmsg.ListPartitionReassignmentsRequestTopic{
					Topic:      topic,
					Partitions: slices.Sorted(maps.Keys(plan[topic])),
				})
				t := kmsg.NewMetadataRequestTopic()
				t.Topic = kmsg.StringPtr(topic)
				metaReq.Topics = append(metaReq.Topics, t)
			}
			listResp, err := listReq.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to list partition reassignments: %v", err)
			}
			metaResp, err := metaReq.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to request metadata: %v", err)
			}

			rows := verifyRows(plan, listResp, metaResp)
			complete := true
			var brokers []int32
			var topics []string
			for _, r := range rows {
				if r.status != statusComplete {
					complete = false
					continue
				}
				brokers = append(brokers, r.current.([]int32)...)
				brokers = append(brokers, r.target...)
				topics = append(topics, r.topic)
			}

			var cleared cleared
			var clearErr error
			if complete {
				cleared, clearErr = clearThrottle(cl, brokers, topics)
			}

			// A failed clear rides in the document rather than as a second
			// error document after it: one JSON object per command.
			throttles := map[string]any{
				"brokers": nonNil(cleared.brokers),
				"topics":  nonNil(cleared.topics),
				"error":   "",
			}
			if clearErr != nil {
				throttles["error"] = clearErr.Error()
			}
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", verifyHeaders...).ErrorColumn()
			table.SetField("throttles_cleared", throttles)
			for _, r := range rows {
				table.Row(r.topic, r.partition, r.current, r.target, r.status, r.err, r.msg)
			}
			flushErr := table.Flush()

			if cl.Format() == out.FormatText && (len(cleared.brokers) > 0 || len(cleared.topics) > 0) {
				ids := make([]string, len(cleared.brokers))
				for i, b := range cleared.brokers {
					ids[i] = strconv.FormatInt(int64(b), 10)
				}
				fmt.Fprintf(os.Stderr, "cleared replication throttles on brokers %s and topics %s\n", strings.Join(ids, ","), strings.Join(cleared.topics, ","))
			}
			if clearErr != nil {
				if cl.Format() == out.FormatText {
					fmt.Fprintf(os.Stderr, "unable to clear replication throttles: %v\n", clearErr)
				}
				return out.ErrSilent
			}
			if flushErr != nil {
				return flushErr
			}
			if !complete {
				return out.ErrSilent
			}
			return nil
		},
	}
	out.Columns(cmd, verifyHeaders...)
	return cmd
}

// nonNil is s, or an empty slice for nil, so that JSON prints [] rather
// than null for a list that is known and empty.
func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
