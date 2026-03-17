package group

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestParseLagFilterAllOperators(t *testing.T) {
	type testCase struct {
		expr    string
		lag     int64
		want    bool
		wantErr bool
	}

	tests := []testCase{
		// Greater than.
		{">0", 0, false, false},
		{">0", 1, true, false},
		{">10", 10, false, false},
		{">10", 11, true, false},
		{">10", 9, false, false},

		// Greater than or equal.
		{">=0", 0, true, false},
		{">=10", 9, false, false},
		{">=10", 10, true, false},
		{">=10", 11, true, false},

		// Less than.
		{"<10", 9, true, false},
		{"<10", 10, false, false},
		{"<10", 11, false, false},
		{"<0", 0, false, false},

		// Less than or equal.
		{"<=10", 9, true, false},
		{"<=10", 10, true, false},
		{"<=10", 11, false, false},
		{"<=0", 0, true, false},

		// Equal.
		{"=0", 0, true, false},
		{"=0", 1, false, false},
		{"=100", 100, true, false},
		{"=100", 99, false, false},
		{"=100", 101, false, false},

		// Bare number (treated as >=N).
		{"0", 0, true, false},
		{"0", 1, true, false},
		{"50", 49, false, false},
		{"50", 50, true, false},
		{"50", 51, true, false},

		// Empty returns nil (tested separately below).
		// Errors.
		{"abc", 0, false, true},
		{">abc", 0, false, true},
		{">=xyz", 0, false, true},
		{"<not", 0, false, true},
		{"<=bad", 0, false, true},
		{"=nope", 0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			fn, err := parseLagFilter(tt.expr)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseLagFilter(%q) expected error", tt.expr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLagFilter(%q) unexpected error: %v", tt.expr, err)
			}
			if fn == nil {
				t.Fatalf("parseLagFilter(%q) returned nil", tt.expr)
			}
			got := fn(tt.lag)
			if got != tt.want {
				t.Errorf("parseLagFilter(%q)(%d) = %v, want %v", tt.expr, tt.lag, got, tt.want)
			}
		})
	}

	// Test empty expression returns nil function.
	t.Run("empty", func(t *testing.T) {
		fn, err := parseLagFilter("")
		if err != nil {
			t.Fatalf("parseLagFilter(\"\") error: %v", err)
		}
		if fn != nil {
			t.Error("parseLagFilter(\"\") should return nil function")
		}
	})

	// Test whitespace trimming.
	t.Run("whitespace", func(t *testing.T) {
		fn, err := parseLagFilter(" > 5 ")
		if err != nil {
			t.Fatalf("parseLagFilter(\" > 5 \") error: %v", err)
		}
		if fn == nil {
			t.Fatal("parseLagFilter(\" > 5 \") returned nil")
		}
		if !fn(6) {
			t.Error("expected lag 6 to match > 5")
		}
		if fn(5) {
			t.Error("expected lag 5 to not match > 5")
		}
	})
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

	// Test that lag filter works correctly with computed lag values.
	// Filter: only topics with lag > 10.
	fn, err := parseLagFilter(">10")
	if err != nil {
		t.Fatal(err)
	}
	if fn(lagPerTopic[topicA]) {
		t.Error("topicA lag (5) should not match >10")
	}
	if !fn(lagPerTopic[topicB]) {
		t.Error("topicB lag (20) should match >10")
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
