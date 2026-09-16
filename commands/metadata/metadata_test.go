package metadata

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func topic(name string, partitions ...int32) kmsg.MetadataResponseTopic {
	t := kmsg.NewMetadataResponseTopic()
	if name != "" {
		t.Topic = kmsg.StringPtr(name)
	}
	for _, p := range partitions {
		rp := kmsg.NewMetadataResponseTopicPartition()
		rp.Partition = p
		t.Partitions = append(t.Partitions, rp)
	}
	return t
}

func TestSortTopics(t *testing.T) {
	for _, test := range []struct {
		name   string
		in     []kmsg.MetadataResponseTopic
		expect []string
		parts  []int32
	}{
		{
			name:   "by name",
			in:     []kmsg.MetadataResponseTopic{topic("demo-proto"), topic("demo-avro"), topic("demo-json")},
			expect: []string{"demo-avro", "demo-json", "demo-proto"},
		},
		{
			name:   "a topic we know only by ID sorts last",
			in:     []kmsg.MetadataResponseTopic{topic(""), topic("zzz"), topic("aaa")},
			expect: []string{"aaa", "zzz", "ERR-UNKNOWN"},
		},
		{
			name:   "partitions by number",
			in:     []kmsg.MetadataResponseTopic{topic("t", 3, 0, 10, 2)},
			expect: []string{"t"},
			parts:  []int32{0, 2, 3, 10},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sortTopics(test.in)
			if len(test.in) != len(test.expect) {
				t.Fatalf("got %d topics != exp %d", len(test.in), len(test.expect))
			}
			for i, exp := range test.expect {
				if got := topicOut(test.in[i].Topic); got != exp {
					t.Errorf("topic %d: got %s != exp %s", i, got, exp)
				}
			}
			if test.parts == nil {
				return
			}
			for i, exp := range test.parts {
				if got := test.in[0].Partitions[i].Partition; got != exp {
					t.Errorf("partition %d: got %d != exp %d", i, got, exp)
				}
			}
		})
	}
}

func TestSortBrokers(t *testing.T) {
	brokers := make([]kmsg.MetadataResponseBroker, 0, 3)
	for _, id := range []int32{3, 1, 2} {
		b := kmsg.NewMetadataResponseBroker()
		b.NodeID = id
		brokers = append(brokers, b)
	}
	sortBrokers(brokers)
	for i, exp := range []int32{1, 2, 3} {
		if got := brokers[i].NodeID; got != exp {
			t.Errorf("broker %d: got %d != exp %d", i, got, exp)
		}
	}
}
