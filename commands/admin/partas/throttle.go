package partas

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

// The configs Kafka's kafka-reassign-partitions.sh --throttle sets: a
// bytes per second rate on every broker a move touches, and on every topic
// the partition:broker pairs the rate applies to.
const (
	brokerLeaderThrottle   = "leader.replication.throttled.rate"
	brokerFollowerThrottle = "follower.replication.throttled.rate"
	topicLeaderThrottle    = "leader.replication.throttled.replicas"
	topicFollowerThrottle  = "follower.replication.throttled.replicas"
)

// partitionMove is where one partition's replicas are and where they are
// going: sources hold the partition today, destinations will. A broker in
// both is not moving and is a source only.
type partitionMove struct {
	sources      []int32
	destinations []int32
}

// moves is every partition being moved, by topic and partition.
type moves map[string]map[int32]partitionMove

// reassignment is one in-progress reassignment as ListPartitionReassignments
// reports it: replicas holds the adding replicas too.
type reassignment struct {
	replicas []int32
	adding   []int32
}

// proposedMoves is Kafka's calculateProposedMoveMap: the in-progress
// reassignments seed the map, and each proposed partition then takes its
// sources from the reassignment in progress if there is one, else from the
// partition's current replicas. Its destinations are the proposed replicas
// that are not sources. A proposed partition with no current replicas
// cannot be throttled, and is an error, as it is in Kafka.
func proposedMoves(current map[string]map[int32]reassignment, proposed map[string]map[int32][]int32, currentReplicas map[string]map[int32][]int32) (moves, error) {
	m := make(moves)
	for topic, partitions := range current {
		for partition, r := range partitions {
			var sources []int32
			for _, replica := range r.replicas {
				if !slices.Contains(r.adding, replica) {
					sources = append(sources, replica)
				}
			}
			m.set(topic, partition, sources, r.adding)
		}
	}
	for topic, partitions := range proposed {
		for partition, replicas := range partitions {
			var sources []int32
			if move, ok := m[topic][partition]; ok {
				sources = move.sources
			} else if cur, ok := currentReplicas[topic][partition]; ok {
				sources = cur
			} else {
				return nil, fmt.Errorf("unable to throttle %s:%d: it has no current replicas", topic, partition)
			}
			var destinations []int32
			for _, replica := range replicas {
				if !slices.Contains(sources, replica) {
					destinations = append(destinations, replica)
				}
			}
			m.set(topic, partition, sources, destinations)
		}
	}
	return m, nil
}

func (m moves) set(topic string, partition int32, sources, destinations []int32) {
	if m[topic] == nil {
		m[topic] = make(map[int32]partitionMove)
	}
	m[topic][partition] = partitionMove{sortedSet(sources), sortedSet(destinations)}
}

func sortedSet(vals []int32) []int32 {
	vals = slices.Clone(vals)
	slices.Sort(vals)
	return slices.Compact(vals)
}

// throttledReplicas is the value of a topic's throttled.replicas config: the
// partition:broker pairs the throttle applies to, sorted, comma joined. The
// leader throttle names the sources, the follower throttle the destinations.
func (m moves) throttledReplicas(topic string, followers bool) string {
	type pair struct{ partition, broker int32 }
	var pairs []pair
	for partition, move := range m[topic] {
		brokers := move.sources
		if followers {
			brokers = move.destinations
		}
		for _, b := range brokers {
			pairs = append(pairs, pair{partition, b})
		}
	}
	slices.SortFunc(pairs, func(a, b pair) int {
		if a.partition != b.partition {
			return int(a.partition - b.partition)
		}
		return int(a.broker - b.broker)
	})
	strs := make([]string, len(pairs))
	for i, p := range pairs {
		strs[i] = fmt.Sprintf("%d:%d", p.partition, p.broker)
	}
	return strings.Join(strs, ",")
}

// brokers is every broker a move touches, sorted.
func (m moves) brokers() []int32 {
	var all []int32
	for _, partitions := range m {
		for _, move := range partitions {
			all = append(all, move.sources...)
			all = append(all, move.destinations...)
		}
	}
	return sortedSet(all)
}

// throttleRequest is the IncrementalAlterConfigs that sets the throttle:
// the two rates on every broker involved, and the two replica lists on every
// topic. Resources are in a fixed order so that a result reads the same each
// run.
func (m moves) throttleRequest(bytesPerSec int64) *kmsg.IncrementalAlterConfigsRequest {
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	set := func(name, value string) kmsg.IncrementalAlterConfigsRequestResourceConfig {
		c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		c.Name = name
		c.Op = kmsg.IncrementalAlterConfigOpSet
		c.Value = kmsg.StringPtr(value)
		return c
	}
	rate := strconv.FormatInt(bytesPerSec, 10)
	for _, broker := range m.brokers() {
		r := kmsg.NewIncrementalAlterConfigsRequestResource()
		r.ResourceType = kmsg.ConfigResourceTypeBroker
		r.ResourceName = strconv.FormatInt(int64(broker), 10)
		r.Configs = append(r.Configs, set(brokerLeaderThrottle, rate), set(brokerFollowerThrottle, rate))
		req.Resources = append(req.Resources, r)
	}
	for _, topic := range slices.Sorted(maps.Keys(m)) {
		r := kmsg.NewIncrementalAlterConfigsRequestResource()
		r.ResourceType = kmsg.ConfigResourceTypeTopic
		r.ResourceName = topic
		r.Configs = append(r.Configs,
			set(topicLeaderThrottle, m.throttledReplicas(topic, false)),
			set(topicFollowerThrottle, m.throttledReplicas(topic, true)),
		)
		req.Resources = append(req.Resources, r)
	}
	return req
}

// applyThrottle sets the replication throttle for the proposed reassignment
// before it is requested, the way Kafka's tool does: it reads the
// reassignments in progress and the current replicas, works out which
// brokers each partition moves between, and alters the broker and topic
// configs. An alter that fails for any resource is an error, and the
// reassignment is not requested.
func applyThrottle(cl *client.Client, proposed map[string]map[int32][]int32, bytesPerSec int64) (moves, error) {
	listResp, err := (&kmsg.ListPartitionReassignmentsRequest{TimeoutMillis: cl.TimeoutMillis()}).RequestWith(context.Background(), cl.Client())
	if err != nil {
		return nil, fmt.Errorf("unable to list partition reassignments: %v", err)
	}
	if err := kerr.ErrorForCode(listResp.ErrorCode); err != nil {
		return nil, fmt.Errorf("unable to list partition reassignments: %v", out.BrokerErr(err, listResp.ErrorMessage))
	}
	current := make(map[string]map[int32]reassignment)
	for _, t := range listResp.Topics {
		current[t.Topic] = make(map[int32]reassignment)
		for _, p := range t.Partitions {
			current[t.Topic][p.Partition] = reassignment{p.Replicas, p.AddingReplicas}
		}
	}

	metaReq := kmsg.NewPtrMetadataRequest()
	for _, topic := range slices.Sorted(maps.Keys(proposed)) {
		t := kmsg.NewMetadataRequestTopic()
		t.Topic = kmsg.StringPtr(topic)
		metaReq.Topics = append(metaReq.Topics, t)
	}
	metaResp, err := metaReq.RequestWith(context.Background(), cl.Client())
	if err != nil {
		return nil, fmt.Errorf("unable to request metadata: %v", err)
	}
	currentReplicas := make(map[string]map[int32][]int32)
	for _, t := range metaResp.Topics {
		if t.Topic == nil || t.ErrorCode != 0 {
			continue
		}
		currentReplicas[*t.Topic] = make(map[int32][]int32)
		for _, p := range t.Partitions {
			currentReplicas[*t.Topic][p.Partition] = p.Replicas
		}
	}

	m, err := proposedMoves(current, proposed, currentReplicas)
	if err != nil {
		return nil, err
	}
	resp, err := m.throttleRequest(bytesPerSec).RequestWith(context.Background(), cl.Client())
	if err != nil {
		return nil, fmt.Errorf("unable to set the replication throttle: %v", err)
	}
	for _, r := range resp.Resources {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			return nil, fmt.Errorf("unable to set the replication throttle on %s %s: %v", strings.ToLower(r.ResourceType.String()), r.ResourceName, out.BrokerErr(err, r.ErrorMessage))
		}
	}
	return m, nil
}
