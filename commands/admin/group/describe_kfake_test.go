package group

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDescribeLagComputation(t *testing.T) {
	c, err := kfake.NewCluster()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx := context.Background()
	adm := kadm.NewClient(cl)

	_, err = adm.CreateTopic(ctx, 1, 1, nil, "lag-topic")
	if err != nil {
		t.Fatal(err)
	}

	// Produce 20 records.
	for range 20 {
		cl.ProduceSync(ctx, &kgo.Record{Topic: "lag-topic", Value: []byte("v")})
	}

	// Commit offset at 15 for group.
	group := "lag-group"
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: "lag-topic", Partition: 0, At: 15, LeaderEpoch: -1})
	_, err = adm.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatal(err)
	}

	// Fetch committed and end offsets.
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	co, ok := fetched.Lookup("lag-topic", 0)
	if !ok {
		t.Fatal("committed offset not found")
	}
	if co.Offset.At != 15 {
		t.Errorf("committed = %d, want 15", co.Offset.At)
	}

	endOffsets, err := adm.ListEndOffsets(ctx, "lag-topic")
	if err != nil {
		t.Fatal(err)
	}
	eo, ok := endOffsets.Lookup("lag-topic", 0)
	if !ok {
		t.Fatal("end offset not found")
	}
	if eo.Offset != 20 {
		t.Errorf("end = %d, want 20", eo.Offset)
	}

	// Verify lag = end - committed = 5.
	lag := eo.Offset - co.Offset.At
	if lag != 5 {
		t.Errorf("lag = %d, want 5", lag)
	}
}
