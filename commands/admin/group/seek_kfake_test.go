package group

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/kcl/offsetparse"
)

func TestSeekToEnd(t *testing.T) {
	_, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	topic := "seek-end-topic"
	group := "seek-end-group"

	_, err := adm.CreateTopic(ctx, 1, 1, nil, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Produce 10 records.
	for i := 0; i < 10; i++ {
		results := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("msg")})
		if results.FirstErr() != nil {
			t.Fatalf("produce error: %v", results.FirstErr())
		}
	}

	// Commit initial offset at 0 so the group has committed offsets.
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: 0, LeaderEpoch: -1})
	_, err = adm.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatal(err)
	}

	// Seek to end: list end offsets, commit them.
	endOffsets, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	newOffsets := make(kadm.Offsets)
	endOffsets.Each(func(lo kadm.ListedOffset) {
		if lo.Err == nil {
			newOffsets.Add(kadm.Offset{
				Topic:       lo.Topic,
				Partition:   lo.Partition,
				At:          lo.Offset,
				LeaderEpoch: lo.LeaderEpoch,
			})
		}
	})

	_, err = adm.CommitOffsets(ctx, group, newOffsets)
	if err != nil {
		t.Fatal(err)
	}

	// Verify committed offset is 10.
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	o, ok := fetched.Lookup(topic, 0)
	if !ok {
		t.Fatal("committed offset not found after seek to end")
	}
	if o.Offset.At != 10 {
		t.Errorf("committed offset after seek to end = %d, want 10", o.Offset.At)
	}
}

func TestSeekToExactOffset(t *testing.T) {
	_, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	topic := "seek-exact-topic"
	group := "seek-exact-group"

	_, err := adm.CreateTopic(ctx, 1, 1, nil, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Produce 20 records so offset 5 is valid.
	for i := 0; i < 20; i++ {
		results := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("msg")})
		if results.FirstErr() != nil {
			t.Fatalf("produce error: %v", results.FirstErr())
		}
	}

	// Commit initial offset at 0.
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: 0, LeaderEpoch: -1})
	_, err = adm.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatal(err)
	}

	// Parse "5" as an exact offset and commit it.
	spec, err := offsetparse.Parse("5", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if spec.Start.Kind != offsetparse.KindExact || spec.Start.Value != 5 {
		t.Fatalf("unexpected spec: kind=%v value=%d, want exact/5", spec.Start.Kind, spec.Start.Value)
	}

	// Commit exact offset 5.
	newOffsets := make(kadm.Offsets)
	newOffsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: spec.Start.Value, LeaderEpoch: -1})
	_, err = adm.CommitOffsets(ctx, group, newOffsets)
	if err != nil {
		t.Fatal(err)
	}

	// Verify committed offset is 5.
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	o, ok := fetched.Lookup(topic, 0)
	if !ok {
		t.Fatal("committed offset not found after seek to exact offset")
	}
	if o.Offset.At != 5 {
		t.Errorf("committed offset after seek to 5 = %d, want 5", o.Offset.At)
	}
}

func TestSeekWithTopicFilter(t *testing.T) {
	_, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	topicA := "seek-filter-a"
	topicB := "seek-filter-b"
	group := "seek-filter-group"

	_, err := adm.CreateTopic(ctx, 1, 1, nil, topicA)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adm.CreateTopic(ctx, 1, 1, nil, topicB)
	if err != nil {
		t.Fatal(err)
	}

	// Produce 10 records to each topic.
	for i := 0; i < 10; i++ {
		cl.ProduceSync(ctx, &kgo.Record{Topic: topicA, Value: []byte("a")})
		cl.ProduceSync(ctx, &kgo.Record{Topic: topicB, Value: []byte("b")})
	}

	// Commit offset 5 on both topics.
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: topicA, Partition: 0, At: 5, LeaderEpoch: -1})
	offsets.Add(kadm.Offset{Topic: topicB, Partition: 0, At: 5, LeaderEpoch: -1})
	_, err = adm.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatal(err)
	}

	// Seek only topicA to end (offset 10). topicB should remain at 5.
	endOffsets, err := adm.ListEndOffsets(ctx, topicA)
	if err != nil {
		t.Fatal(err)
	}

	newOffsets := make(kadm.Offsets)
	endOffsets.Each(func(lo kadm.ListedOffset) {
		if lo.Err == nil {
			newOffsets.Add(kadm.Offset{
				Topic:       lo.Topic,
				Partition:   lo.Partition,
				At:          lo.Offset,
				LeaderEpoch: lo.LeaderEpoch,
			})
		}
	})
	_, err = adm.CommitOffsets(ctx, group, newOffsets)
	if err != nil {
		t.Fatal(err)
	}

	// Verify topicA is at 10.
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	oA, ok := fetched.Lookup(topicA, 0)
	if !ok {
		t.Fatal("topicA offset not found")
	}
	if oA.Offset.At != 10 {
		t.Errorf("topicA committed offset = %d, want 10", oA.Offset.At)
	}

	// Verify topicB is still at 5 (unchanged).
	oB, ok := fetched.Lookup(topicB, 0)
	if !ok {
		t.Fatal("topicB offset not found")
	}
	if oB.Offset.At != 5 {
		t.Errorf("topicB committed offset = %d, want 5 (should be unchanged)", oB.Offset.At)
	}
}

func TestSeekTimestamp(t *testing.T) {
	_, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	topic := "seek-ts-topic"

	_, err := adm.CreateTopic(ctx, 1, 1, nil, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Produce records.
	for i := 0; i < 10; i++ {
		results := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("msg")})
		if results.FirstErr() != nil {
			t.Fatalf("produce error: %v", results.FirstErr())
		}
	}

	// Use a timestamp far in the past: should resolve to offset 0.
	pastMillis := time.Now().Add(-24 * time.Hour).UnixMilli()
	listed, err := adm.ListOffsetsAfterMilli(ctx, pastMillis, topic)
	if err != nil {
		t.Fatal(err)
	}

	lo, ok := listed.Lookup(topic, 0)
	if !ok {
		t.Fatal("partition 0 not found in timestamp-resolved offsets")
	}
	// With a timestamp in the far past, offset should be at or near 0.
	if lo.Offset < 0 {
		t.Errorf("timestamp-resolved offset = %d, want >= 0", lo.Offset)
	}

	// Use a timestamp far in the future: should resolve to the end offset.
	futureMillis := time.Now().Add(24 * time.Hour).UnixMilli()
	listed, err = adm.ListOffsetsAfterMilli(ctx, futureMillis, topic)
	if err != nil {
		t.Fatal(err)
	}

	lo, ok = listed.Lookup(topic, 0)
	if !ok {
		t.Fatal("partition 0 not found in future-timestamp-resolved offsets")
	}
	// With a timestamp far in the future, offset should be at the end (10).
	if lo.Offset != 10 {
		t.Errorf("future-timestamp-resolved offset = %d, want 10", lo.Offset)
	}

	// Verify that offsetparse handles the @TIMESTAMP syntax correctly.
	tsStr := time.Now().Add(-1 * time.Hour).Format(time.RFC3339)
	spec, err := offsetparse.Parse("@"+tsStr, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if spec.Start.Kind != offsetparse.KindTimestamp {
		t.Errorf("@timestamp spec kind = %v, want KindTimestamp", spec.Start.Kind)
	}
	// The resolved value should be approximately 1 hour ago in millis.
	expectedApprox := time.Now().Add(-1 * time.Hour).UnixMilli()
	diff := spec.Start.Value - expectedApprox
	if diff < -5000 || diff > 5000 {
		t.Errorf("@timestamp resolved to %d, expected approximately %d (diff=%d)", spec.Start.Value, expectedApprox, diff)
	}
}
