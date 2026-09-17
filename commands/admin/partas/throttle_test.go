package partas

import (
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestProposedMoves pins Kafka's move map: sources are the current replicas,
// or the replicas of a reassignment in progress less the ones being added;
// destinations are the proposed replicas that are not sources.
func TestProposedMoves(t *testing.T) {
	for _, test := range []struct {
		name     string
		current  map[string]map[int32]reassignment
		proposed map[string]map[int32][]int32
		replicas map[string]map[int32][]int32
		leader   map[string]string
		follower map[string]string
		brokers  []int32
		wantErr  bool
	}{
		{
			name:     "move one partition",
			proposed: map[string]map[int32][]int32{"foo": {0: {2, 3}}},
			replicas: map[string]map[int32][]int32{"foo": {0: {1, 2}}},
			leader:   map[string]string{"foo": "0:1,0:2"},
			follower: map[string]string{"foo": "0:3"},
			brokers:  []int32{1, 2, 3},
		},
		{
			name:     "no broker changes",
			proposed: map[string]map[int32][]int32{"foo": {0: {2, 1}}},
			replicas: map[string]map[int32][]int32{"foo": {0: {1, 2}}},
			leader:   map[string]string{"foo": "0:1,0:2"},
			follower: map[string]string{"foo": ""},
			brokers:  []int32{1, 2},
		},
		{
			name:     "reassignment in progress supplies the sources",
			current:  map[string]map[int32]reassignment{"foo": {0: {replicas: []int32{1, 2, 3}, adding: []int32{3}}}},
			proposed: map[string]map[int32][]int32{"foo": {0: {4}}},
			replicas: map[string]map[int32][]int32{"foo": {0: {1, 2, 3}}},
			leader:   map[string]string{"foo": "0:1,0:2"},
			follower: map[string]string{"foo": "0:4"},
			brokers:  []int32{1, 2, 4},
		},
		{
			name:     "an unrelated reassignment in progress is throttled too",
			current:  map[string]map[int32]reassignment{"bar": {1: {replicas: []int32{5, 6}, adding: []int32{6}}}},
			proposed: map[string]map[int32][]int32{"foo": {0: {2}}},
			replicas: map[string]map[int32][]int32{"foo": {0: {1}}},
			leader:   map[string]string{"foo": "0:1", "bar": "1:5"},
			follower: map[string]string{"foo": "0:2", "bar": "1:6"},
			brokers:  []int32{1, 2, 5, 6},
		},
		{
			name:     "pairs sort by partition then broker",
			proposed: map[string]map[int32][]int32{"foo": {10: {3}, 2: {3}}},
			replicas: map[string]map[int32][]int32{"foo": {10: {2, 1}, 2: {1}}},
			leader:   map[string]string{"foo": "2:1,10:1,10:2"},
			follower: map[string]string{"foo": "2:3,10:3"},
			brokers:  []int32{1, 2, 3},
		},
		{
			name:     "a partition with no replicas cannot be throttled",
			proposed: map[string]map[int32][]int32{"foo": {0: {2}}},
			wantErr:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, err := proposedMoves(test.current, test.proposed, test.replicas)
			if test.wantErr {
				if err == nil {
					t.Fatal("got nil err, want one")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for topic, want := range test.leader {
				if got := m.throttledReplicas(topic, false); got != want {
					t.Errorf("%s leader throttle = %q, want %q", topic, got, want)
				}
			}
			for topic, want := range test.follower {
				if got := m.throttledReplicas(topic, true); got != want {
					t.Errorf("%s follower throttle = %q, want %q", topic, got, want)
				}
			}
			if got := m.brokers(); !slices.Equal(got, test.brokers) {
				t.Errorf("brokers = %v, want %v", got, test.brokers)
			}
		})
	}
}

// TestThrottleRequest pins the configs the request sets: both rates on each
// broker, both replica lists on each topic, brokers before topics and each
// sorted.
func TestThrottleRequest(t *testing.T) {
	m, err := proposedMoves(nil,
		map[string]map[int32][]int32{"foo": {0: {2, 3}}, "bar": {1: {1}}},
		map[string]map[int32][]int32{"foo": {0: {1, 2}}, "bar": {1: {3}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	req := m.throttleRequest(1000)

	type want struct {
		typ     kmsg.ConfigResourceType
		name    string
		configs map[string]string
	}
	wants := []want{
		{kmsg.ConfigResourceTypeBroker, "1", map[string]string{brokerLeaderThrottle: "1000", brokerFollowerThrottle: "1000"}},
		{kmsg.ConfigResourceTypeBroker, "2", map[string]string{brokerLeaderThrottle: "1000", brokerFollowerThrottle: "1000"}},
		{kmsg.ConfigResourceTypeBroker, "3", map[string]string{brokerLeaderThrottle: "1000", brokerFollowerThrottle: "1000"}},
		{kmsg.ConfigResourceTypeTopic, "bar", map[string]string{topicLeaderThrottle: "1:3", topicFollowerThrottle: "1:1"}},
		{kmsg.ConfigResourceTypeTopic, "foo", map[string]string{topicLeaderThrottle: "0:1,0:2", topicFollowerThrottle: "0:3"}},
	}
	if len(req.Resources) != len(wants) {
		t.Fatalf("%d resources, want %d", len(req.Resources), len(wants))
	}
	for i, w := range wants {
		r := req.Resources[i]
		if r.ResourceType != w.typ || r.ResourceName != w.name {
			t.Errorf("resource %d = %v %q, want %v %q", i, r.ResourceType, r.ResourceName, w.typ, w.name)
			continue
		}
		got := make(map[string]string)
		for _, c := range r.Configs {
			if c.Op != kmsg.IncrementalAlterConfigOpSet || c.Value == nil {
				t.Errorf("%s %s: config %s is not a set", w.typ, w.name, c.Name)
				continue
			}
			got[c.Name] = *c.Value
		}
		for k, v := range w.configs {
			if got[k] != v {
				t.Errorf("%s %s: %s = %q, want %q", w.typ, w.name, k, got[k], v)
			}
		}
		if len(got) != len(w.configs) {
			t.Errorf("%s %s: configs = %v, want %v", w.typ, w.name, got, w.configs)
		}
	}
}
