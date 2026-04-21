package topic

import (
	"context"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestParseTopicID(t *testing.T) {
	tests := []struct {
		in  string
		err bool
	}{
		{"00000000000000000000000000000000", false},
		{"465a97c59919152e1827030ce374ec71", false},
		{"465a97c5-9919-152e-1827-030ce374ec71", false},
		{"465a97c5-9919152e-1827-030ce374ec71", false}, // dashes are stripped regardless of position
		{"", true},
		{"short", true},
		{"465a97c59919152e1827030ce374ec7g", true}, // non-hex
		{"465a97c59919152e1827030ce374ec", true},   // 30 chars
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			_, err := parseTopicID(tt.in)
			if tt.err {
				if err == nil {
					t.Errorf("parseTopicID(%q) expected error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseTopicID(%q): %v", tt.in, err)
			}
			stripped := strings.ReplaceAll(tt.in, "-", "")
			if len(stripped) != 32 {
				t.Fatalf("test input not 32 hex chars after strip: %q", tt.in)
			}
		})
	}
}

func TestTopicDescribeMetadata(t *testing.T) {
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

	// Create a topic with 3 partitions.
	_, err = adm.CreateTopic(ctx, 3, 1, nil, "describe-test")
	if err != nil {
		t.Fatal(err)
	}

	// Fetch metadata to verify.
	metaReq := kmsg.NewPtrMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr("describe-test")
	metaReq.Topics = append(metaReq.Topics, rt)
	metaResp, err := metaReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}

	if len(metaResp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(metaResp.Topics))
	}
	topic := metaResp.Topics[0]
	if *topic.Topic != "describe-test" {
		t.Errorf("topic name = %q, want describe-test", *topic.Topic)
	}
	if len(topic.Partitions) != 3 {
		t.Errorf("partitions = %d, want 3", len(topic.Partitions))
	}
}

func TestTopicDescribeConfigs(t *testing.T) {
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

	_, err = adm.CreateTopic(ctx, 1, 1, nil, "config-test")
	if err != nil {
		t.Fatal(err)
	}

	// Fetch configs.
	configs, err := fetchTopicConfigs(ctx, cl, []string{"config-test"})
	if err != nil {
		t.Fatal(err)
	}
	if configs == nil {
		t.Fatal("expected non-nil configs")
	}

	// kfake returns configs for the topic.
	cfgs, ok := configs["config-test"]
	if !ok {
		t.Fatal("config-test not found in configs response")
	}
	// kfake should return at least some default configs.
	_ = cfgs // just verify no panic
}

func TestInt32sToString(t *testing.T) {
	tests := []struct {
		input []int32
		want  string
	}{
		{[]int32{1, 2, 3}, "[1,2,3]"},
		{[]int32{0}, "[0]"},
		{nil, "[]"},
		{[]int32{}, "[]"},
	}
	for _, tt := range tests {
		got := int32sToString(tt.input)
		if got != tt.want {
			t.Errorf("int32sToString(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
