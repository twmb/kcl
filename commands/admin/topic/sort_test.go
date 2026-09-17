package topic

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func metaTopic(name string, partitions ...int32) kmsg.MetadataResponseTopic {
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
			in:     []kmsg.MetadataResponseTopic{metaTopic("demo-proto"), metaTopic("demo-avro"), metaTopic("demo-json")},
			expect: []string{"demo-avro", "demo-json", "demo-proto"},
		},
		{
			name:   "a topic we know only by ID sorts last",
			in:     []kmsg.MetadataResponseTopic{metaTopic(""), metaTopic("zzz"), metaTopic("aaa")},
			expect: []string{"aaa", "zzz", ""},
		},
		{
			name:   "partitions by number",
			in:     []kmsg.MetadataResponseTopic{metaTopic("t", 3, 0, 10, 2)},
			expect: []string{"t"},
			parts:  []int32{0, 2, 3, 10},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			SortTopics(test.in)
			if len(test.in) != len(test.expect) {
				t.Fatalf("got %d topics != exp %d", len(test.in), len(test.expect))
			}
			for i, exp := range test.expect {
				got := ""
				if test.in[i].Topic != nil {
					got = *test.in[i].Topic
				}
				if got != exp {
					t.Errorf("topic %d: got %q != exp %q", i, got, exp)
				}
			}
			for i, exp := range test.parts {
				if got := test.in[0].Partitions[i].Partition; got != exp {
					t.Errorf("partition %d: got %d != exp %d", i, got, exp)
				}
			}
		})
	}
}
