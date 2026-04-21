package group

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/kcl/offsetparse"
)

func newTestCluster(t *testing.T) (*kfake.Cluster, *kgo.Client) {
	t.Helper()
	c, err := kfake.NewCluster()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return c, cl
}

func TestSeekResolveStartEnd(t *testing.T) {
	_, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	// Create topic and produce records.
	_, err := adm.CreateTopic(ctx, 1, 1, nil, "test-topic")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		cl.ProduceSync(ctx, &kgo.Record{Topic: "test-topic", Value: []byte("msg")})
	}

	// Test resolving "start".
	spec, _ := offsetparse.Parse("start", time.Now())
	listed, err := adm.ListStartOffsets(ctx, "test-topic")
	if err != nil {
		t.Fatal(err)
	}
	_ = spec
	lo, ok := listed.Lookup("test-topic", 0)
	if !ok {
		t.Fatal("partition 0 not found in start offsets")
	}
	if lo.Offset != 0 {
		t.Errorf("start offset = %d, want 0", lo.Offset)
	}

	// Test resolving "end".
	listed, err = adm.ListEndOffsets(ctx, "test-topic")
	if err != nil {
		t.Fatal(err)
	}
	lo, ok = listed.Lookup("test-topic", 0)
	if !ok {
		t.Fatal("partition 0 not found in end offsets")
	}
	if lo.Offset != 10 {
		t.Errorf("end offset = %d, want 10", lo.Offset)
	}
}

func TestSeekCommitAndVerify(t *testing.T) {
	_, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	// Create topic and produce records.
	_, err := adm.CreateTopic(ctx, 1, 1, nil, "seek-test")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		cl.ProduceSync(ctx, &kgo.Record{Topic: "seek-test", Value: []byte("msg")})
	}

	group := "test-group"

	// Commit initial offsets at 10.
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: "seek-test", Partition: 0, At: 10, LeaderEpoch: -1})
	_, err = adm.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatal(err)
	}

	// Verify committed offset.
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if o, ok := fetched.Lookup("seek-test", 0); !ok {
		t.Fatal("offset not found after commit")
	} else if o.Offset.At != 10 {
		t.Errorf("committed offset = %d, want 10", o.Offset.At)
	}

	// Seek to start (offset 0).
	newOffsets := make(kadm.Offsets)
	startListed, err := adm.ListStartOffsets(ctx, "seek-test")
	if err != nil {
		t.Fatal(err)
	}
	startListed.Each(func(lo kadm.ListedOffset) {
		if lo.Err == nil {
			newOffsets.Add(kadm.Offset{
				Topic:     lo.Topic,
				Partition: lo.Partition,
				At:        lo.Offset,
			})
		}
	})
	_, err = adm.CommitOffsets(ctx, group, newOffsets)
	if err != nil {
		t.Fatal(err)
	}

	// Verify seeked offset.
	fetched, err = adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if o, ok := fetched.Lookup("seek-test", 0); !ok {
		t.Fatal("offset not found after seek")
	} else if o.Offset.At != 0 {
		t.Errorf("seeked offset = %d, want 0", o.Offset.At)
	}
}

func TestSeekRelativeOffset(t *testing.T) {
	_, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	_, err := adm.CreateTopic(ctx, 1, 1, nil, "rel-test")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		cl.ProduceSync(ctx, &kgo.Record{Topic: "rel-test", Value: []byte("msg")})
	}

	group := "rel-group"

	// Commit at 15.
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: "rel-test", Partition: 0, At: 15, LeaderEpoch: -1})
	adm.CommitOffsets(ctx, group, offsets)

	// Parse "-5" (relative backward).
	spec, err := offsetparse.Parse("-5", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if spec.Start.Kind != offsetparse.KindRelative || spec.Start.Value != -5 {
		t.Fatalf("unexpected spec: %+v", spec.Start)
	}

	// Compute new offset: 15 + (-5) = 10.
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	o, ok := fetched.Lookup("rel-test", 0)
	if !ok {
		t.Fatal("offset not found")
	}
	newAt := o.Offset.At + spec.Start.Value
	if newAt != 10 {
		t.Errorf("relative offset = %d, want 10", newAt)
	}
}
