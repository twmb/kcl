package flagutil

import (
	"fmt"
	"testing"
)

func TestParseTopicPartitions(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		wantErr bool
		check   func(map[string][]int32) error
	}{
		{
			"single topic no partitions",
			[]string{"foo"},
			false,
			func(m map[string][]int32) error {
				if m["foo"] != nil {
					return fmt.Errorf("foo should have nil partitions, got %v", m["foo"])
				}
				return nil
			},
		},
		{
			"single topic with partitions",
			[]string{"foo:1,2,3"},
			false,
			func(m map[string][]int32) error {
				ps := m["foo"]
				if len(ps) != 3 || ps[0] != 1 || ps[1] != 2 || ps[2] != 3 {
					return fmt.Errorf("foo partitions = %v, want [1 2 3]", ps)
				}
				return nil
			},
		},
		{
			"multiple topics",
			[]string{"foo:0,1", "bar:5"},
			false,
			func(m map[string][]int32) error {
				if len(m) != 2 {
					return fmt.Errorf("expected 2 topics, got %d", len(m))
				}
				if len(m["foo"]) != 2 {
					return fmt.Errorf("foo partitions = %v", m["foo"])
				}
				if len(m["bar"]) != 1 || m["bar"][0] != 5 {
					return fmt.Errorf("bar partitions = %v", m["bar"])
				}
				return nil
			},
		},
		{
			"repeated topic merges partitions",
			[]string{"foo:0", "foo:3,5"},
			false,
			func(m map[string][]int32) error {
				ps := m["foo"]
				if len(ps) != 3 || ps[0] != 0 || ps[1] != 3 || ps[2] != 5 {
					return fmt.Errorf("foo partitions = %v, want [0 3 5]", ps)
				}
				return nil
			},
		},
		{
			"bare topic beats per-partition scope",
			[]string{"foo:0", "foo"},
			false,
			func(m map[string][]int32) error {
				if m["foo"] != nil {
					return fmt.Errorf("bare 'foo' should yield nil partitions, got %v", m["foo"])
				}
				return nil
			},
		},
		{
			"empty topic name",
			[]string{":1,2"},
			true,
			nil,
		},
		{
			"invalid partition",
			[]string{"foo:abc"},
			true,
			nil,
		},
	}

	// Separate test for SplitTopicPartitionEntries round-trip.
	got := SplitTopicPartitionEntries([]string{"a,b,c", "d:0,1", "e"})
	want := []string{"a", "b", "c", "d:0,1", "e"}
	if len(got) != len(want) {
		t.Fatalf("SplitTopicPartitionEntries: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("SplitTopicPartitionEntries[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTopicPartitions(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				if err := tt.check(got); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestParseTopicPartitionReplicas(t *testing.T) {
	result, err := ParseTopicPartitionReplicas([]string{"foo: 0->1,2,3 ; 1->4,5,6"})
	if err != nil {
		t.Fatal(err)
	}
	topic, ok := result["foo"]
	if !ok {
		t.Fatal("foo not found")
	}
	if len(topic) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(topic))
	}
	p0 := topic[0]
	if len(p0) != 3 || p0[0] != 1 || p0[1] != 2 || p0[2] != 3 {
		t.Errorf("partition 0 replicas = %v, want [1 2 3]", p0)
	}
	p1 := topic[1]
	if len(p1) != 3 || p1[0] != 4 || p1[1] != 5 || p1[2] != 6 {
		t.Errorf("partition 1 replicas = %v, want [4 5 6]", p1)
	}
}

func TestParseTopicPartitionReplicasErrors(t *testing.T) {
	tests := []struct {
		name  string
		input []string
	}{
		{"missing colon", []string{"foo"}},
		{"missing arrow", []string{"foo: 0,1,2"}},
		{"no replicas", []string{"foo: 0->"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseTopicPartitionReplicas(tt.input)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}
