package topic

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/out"
)

// listedOffset is one partition's answer to ListOffsets: the broker that
// answered, the offset and its leader epoch, or the error. A broker we could
// not ask answers with its error for every partition it was asked about, and
// a broker of -1 when the partition has no leader to ask.
type listedOffset struct {
	broker int32
	offset int64
	epoch  int32
	err    error
}

// cell is the offset as a table cell: the number, or Unknown when the
// listing failed.
func (l listedOffset) cell() any {
	if l.err != nil || l.offset < 0 {
		return out.Unknown
	}
	return l.offset
}

// epochCell is the leader epoch as a table cell; a broker before 2.1 reports
// none, which is -1 on the wire.
func (l listedOffset) epochCell() any {
	if l.err != nil || l.epoch < 0 {
		return out.Unknown
	}
	return l.epoch
}

// brokerCell is the broker as a table cell, Unknown when no broker could be
// asked.
func (l listedOffset) brokerCell() any {
	if l.broker < 0 {
		return out.Unknown
	}
	return l.broker
}

type listedOffsets map[string]map[int32]listedOffset

func (l listedOffsets) get(topic string, partition int32) listedOffset {
	lo, ok := l[topic][partition]
	if !ok {
		return listedOffset{broker: -1, offset: -1, epoch: -1, err: kerr.UnknownServerError}
	}
	return lo
}

func (l listedOffsets) set(topic string, partition int32, lo listedOffset) {
	m := l[topic]
	if m == nil {
		m = make(map[int32]listedOffset)
		l[topic] = m
	}
	m[partition] = lo
}

// Special ListOffsets timestamps.
const (
	tsStart = -2
	tsEnd   = -1
)

// Isolation levels for ListOffsets; read committed answers -1 with the last
// stable offset.
const (
	readUncommitted = 0
	readCommitted   = 1
)

// listOffsets issues ListOffsets for every partition in tps, one request per
// leader, and returns an answer for every partition asked about: a partition
// whose leader could not be asked, or that has none, carries the error
// franz-go reports for it. Below ListOffsets v1 the offset is the first of
// the old style offsets.
func listOffsets(ctx context.Context, cl *kgo.Client, isolation int8, timestamp int64, tps map[string][]int32) listedOffsets {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	req.IsolationLevel = isolation
	for topic, partitions := range tps {
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = topic
		for _, p := range partitions {
			rp := kmsg.NewListOffsetsRequestTopicPartition()
			rp.Partition = p
			rp.CurrentLeaderEpoch = -1
			rp.Timestamp = timestamp
			rp.MaxNumOffsets = 1
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
	}

	listed := make(listedOffsets)
	for _, shard := range cl.RequestSharded(ctx, req) {
		if shard.Err != nil {
			for _, t := range shard.Req.(*kmsg.ListOffsetsRequest).Topics {
				for _, p := range t.Partitions {
					listed.set(t.Topic, p.Partition, listedOffset{
						broker: shard.Meta.NodeID,
						offset: -1,
						epoch:  -1,
						err:    shard.Err,
					})
				}
			}
			continue
		}
		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				offset := p.Offset
				if resp.Version == 0 && len(p.OldStyleOffsets) > 0 {
					offset = p.OldStyleOffsets[0]
				}
				listed.set(t.Topic, p.Partition, listedOffset{
					broker: shard.Meta.NodeID,
					offset: offset,
					epoch:  p.LeaderEpoch,
					err:    kerr.ErrorForCode(p.ErrorCode),
				})
			}
		}
	}
	return listed
}

// listOffsetsAt is one listing to run concurrently with others.
type listOffsetsAt struct {
	isolation int8
	timestamp int64
}

// listOffsetsAll runs each listing concurrently and returns them in order.
func listOffsetsAll(ctx context.Context, cl *kgo.Client, tps map[string][]int32, at ...listOffsetsAt) []listedOffsets {
	results := make([]listedOffsets, len(at))
	var wg sync.WaitGroup
	for i, a := range at {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = listOffsets(ctx, cl, a.isolation, a.timestamp, tps)
		}()
	}
	wg.Wait()
	return results
}

// partitionsOf returns every partition of each topic from metadata, and a
// per-topic error for a topic the broker did not answer for. A topic with an
// error has no partitions.
func partitionsOf(ctx context.Context, cl *kgo.Client, topics []string) (map[string][]int32, map[string]error, error) {
	req := kmsg.NewPtrMetadataRequest()
	for _, t := range topics {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(t)
		req.Topics = append(req.Topics, rt)
	}
	if len(topics) == 0 {
		req.Topics = nil // every topic
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, nil, err
	}
	tps := make(map[string][]int32)
	errs := make(map[string]error)
	for _, t := range resp.Topics {
		if t.Topic == nil {
			continue
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			errs[*t.Topic] = err
			continue
		}
		if len(topics) == 0 && t.IsInternal {
			continue
		}
		for _, p := range t.Partitions {
			tps[*t.Topic] = append(tps[*t.Topic], p.Partition)
		}
	}
	return tps, errs, nil
}
