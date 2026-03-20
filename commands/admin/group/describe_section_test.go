package group

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestValidateSectionAllOperators(t *testing.T) {
	valid := []string{"", "summary", "lag", "members"}
	for _, s := range valid {
		if err := validateSection(s); err != nil {
			t.Errorf("validateSection(%q) unexpected error: %v", s, err)
		}
	}

	invalid := []string{"invalid", "Summary", "LAG", "Members", "foo", "all"}
	for _, s := range invalid {
		if err := validateSection(s); err == nil {
			t.Errorf("validateSection(%q) expected error", s)
		}
	}
}

func TestLagPerTopicComputation(t *testing.T) {
	_, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	topicA := "lag-topic-a"
	topicB := "lag-topic-b"
	group := "lag-computation-group"

	_, err := adm.CreateTopic(ctx, 1, 1, nil, topicA)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adm.CreateTopic(ctx, 1, 1, nil, topicB)
	if err != nil {
		t.Fatal(err)
	}

	// Produce 20 records to topicA, 30 to topicB.
	for i := 0; i < 20; i++ {
		results := cl.ProduceSync(ctx, &kgo.Record{Topic: topicA, Value: []byte("a")})
		if results.FirstErr() != nil {
			t.Fatalf("produce to topicA error: %v", results.FirstErr())
		}
	}
	for i := 0; i < 30; i++ {
		results := cl.ProduceSync(ctx, &kgo.Record{Topic: topicB, Value: []byte("b")})
		if results.FirstErr() != nil {
			t.Fatalf("produce to topicB error: %v", results.FirstErr())
		}
	}

	// Commit offset 15 on topicA (lag = 20 - 15 = 5).
	// Commit offset 10 on topicB (lag = 30 - 10 = 20).
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: topicA, Partition: 0, At: 15, LeaderEpoch: -1})
	offsets.Add(kadm.Offset{Topic: topicB, Partition: 0, At: 10, LeaderEpoch: -1})
	_, err = adm.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatal(err)
	}

	// Fetch committed offsets.
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}

	// Fetch end offsets for both topics.
	endOffsetsA, err := adm.ListEndOffsets(ctx, topicA)
	if err != nil {
		t.Fatal(err)
	}
	endOffsetsB, err := adm.ListEndOffsets(ctx, topicB)
	if err != nil {
		t.Fatal(err)
	}

	// Compute lag per topic manually, mimicking the describe command logic.
	lagPerTopic := make(map[string]int64)

	// topicA lag.
	committedA, ok := fetched.Lookup(topicA, 0)
	if !ok {
		t.Fatal("topicA committed offset not found")
	}
	endA, ok := endOffsetsA.Lookup(topicA, 0)
	if !ok {
		t.Fatal("topicA end offset not found")
	}
	lagA := endA.Offset - committedA.Offset.At
	lagPerTopic[topicA] = lagA

	// topicB lag.
	committedB, ok := fetched.Lookup(topicB, 0)
	if !ok {
		t.Fatal("topicB committed offset not found")
	}
	endB, ok := endOffsetsB.Lookup(topicB, 0)
	if !ok {
		t.Fatal("topicB end offset not found")
	}
	lagB := endB.Offset - committedB.Offset.At
	lagPerTopic[topicB] = lagB

	// Verify lag per topic.
	if lagPerTopic[topicA] != 5 {
		t.Errorf("lag for topicA = %d, want 5 (end=20, committed=15)", lagPerTopic[topicA])
	}
	if lagPerTopic[topicB] != 20 {
		t.Errorf("lag for topicB = %d, want 20 (end=30, committed=10)", lagPerTopic[topicB])
	}

	// Verify total lag.
	totalLag := lagPerTopic[topicA] + lagPerTopic[topicB]
	if totalLag != 25 {
		t.Errorf("total lag = %d, want 25", totalLag)
	}

	// Verify the raw offset values are correct.
	if committedA.Offset.At != 15 {
		t.Errorf("topicA committed = %d, want 15", committedA.Offset.At)
	}
	if committedB.Offset.At != 10 {
		t.Errorf("topicB committed = %d, want 10", committedB.Offset.At)
	}
	if endA.Offset != 20 {
		t.Errorf("topicA end = %d, want 20", endA.Offset)
	}
	if endB.Offset != 30 {
		t.Errorf("topicB end = %d, want 30", endB.Offset)
	}
}
