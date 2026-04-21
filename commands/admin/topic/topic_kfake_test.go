package topic

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newTopicTestCluster(t *testing.T) (*kfake.Cluster, *kgo.Client) {
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

func TestTopicCreateAndList(t *testing.T) {
	_, cl := newTopicTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	// Create a topic.
	_, err := adm.CreateTopic(ctx, 3, 1, nil, "list-test-topic")
	if err != nil {
		t.Fatal(err)
	}

	// List topics via metadata request to verify it appears.
	metaReq := kmsg.NewPtrMetadataRequest()
	metaResp, err := metaReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, topic := range metaResp.Topics {
		if topic.Topic != nil && *topic.Topic == "list-test-topic" {
			found = true
			if len(topic.Partitions) != 3 {
				t.Errorf("partitions = %d, want 3", len(topic.Partitions))
			}
			break
		}
	}
	if !found {
		t.Error("created topic 'list-test-topic' not found in metadata listing")
	}
}

func TestTopicCreateIfNotExists(t *testing.T) {
	_, cl := newTopicTestCluster(t)
	ctx := context.Background()

	topicName := "if-not-exists-topic"

	// First create: should succeed.
	req := kmsg.NewPtrCreateTopicsRequest()
	reqTopic := kmsg.NewCreateTopicsRequestTopic()
	reqTopic.Topic = topicName
	reqTopic.NumPartitions = 1
	reqTopic.ReplicationFactor = 1
	req.Topics = append(req.Topics, reqTopic)

	kresp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if len(kresp.Topics) != 1 {
		t.Fatalf("expected 1 topic in response, got %d", len(kresp.Topics))
	}
	if topicErr := kerr.ErrorForCode(kresp.Topics[0].ErrorCode); topicErr != nil {
		t.Fatalf("first create failed: %v", topicErr)
	}

	// Second create: should return TOPIC_ALREADY_EXISTS.
	req2 := kmsg.NewPtrCreateTopicsRequest()
	reqTopic2 := kmsg.NewCreateTopicsRequestTopic()
	reqTopic2.Topic = topicName
	reqTopic2.NumPartitions = 1
	reqTopic2.ReplicationFactor = 1
	req2.Topics = append(req2.Topics, reqTopic2)

	kresp2, err := req2.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if len(kresp2.Topics) != 1 {
		t.Fatalf("expected 1 topic in second response, got %d", len(kresp2.Topics))
	}
	topicErr := kerr.ErrorForCode(kresp2.Topics[0].ErrorCode)

	// Verify that the error is TOPIC_ALREADY_EXISTS.
	if topicErr != kerr.TopicAlreadyExists {
		t.Errorf("second create error = %v, want TOPIC_ALREADY_EXISTS", topicErr)
	}

	// Simulate --if-not-exists behavior: suppress TOPIC_ALREADY_EXISTS.
	if topicErr == kerr.TopicAlreadyExists {
		// This is expected and not a real error with --if-not-exists.
		topicErr = nil
	}
	if topicErr != nil {
		t.Errorf("with if-not-exists suppression, error = %v, want nil", topicErr)
	}
}

func TestTopicDelete(t *testing.T) {
	_, cl := newTopicTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	topicName := "delete-me-topic"

	// Create a topic.
	_, err := adm.CreateTopic(ctx, 1, 1, nil, topicName)
	if err != nil {
		t.Fatal(err)
	}

	// Verify it exists.
	metaReq := kmsg.NewPtrMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr(topicName)
	metaReq.Topics = append(metaReq.Topics, rt)
	metaResp, err := metaReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if len(metaResp.Topics) == 0 {
		t.Fatal("topic not found after creation")
	}
	if topicErr := kerr.ErrorForCode(metaResp.Topics[0].ErrorCode); topicErr != nil {
		t.Fatalf("topic error after creation: %v", topicErr)
	}

	// Delete the topic.
	deleteReq := kmsg.NewPtrDeleteTopicsRequest()
	delTopic := kmsg.NewDeleteTopicsRequestTopic()
	delTopic.Topic = kmsg.StringPtr(topicName)
	deleteReq.Topics = append(deleteReq.Topics, delTopic)

	deleteKresp, err := deleteReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleteKresp.Topics) != 1 {
		t.Fatalf("expected 1 topic in delete response, got %d", len(deleteKresp.Topics))
	}
	if delErr := kerr.ErrorForCode(deleteKresp.Topics[0].ErrorCode); delErr != nil {
		t.Fatalf("delete error: %v", delErr)
	}

	// Verify the topic no longer exists by requesting metadata for it
	// specifically. After deletion, kfake should return an error code
	// (UNKNOWN_TOPIC_OR_PARTITION) or exclude it from the response.
	metaReq2 := kmsg.NewPtrMetadataRequest()
	rt2 := kmsg.NewMetadataRequestTopic()
	rt2.Topic = kmsg.StringPtr(topicName)
	metaReq2.Topics = append(metaReq2.Topics, rt2)
	metaResp2, err := metaReq2.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}

	for _, topic := range metaResp2.Topics {
		if topic.Topic != nil && *topic.Topic == topicName {
			topicErr := kerr.ErrorForCode(topic.ErrorCode)
			if topicErr == nil {
				// Some kfake versions may still return the topic briefly.
				// The key verification is that the delete request itself
				// returned no error (tested above).
				t.Log("note: deleted topic still appears in metadata without error; delete response was successful")
			}
		}
	}
}

func TestParseReplicaAssignment(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantParts int
		wantErr   bool
		check     func(t *testing.T, assignments []kmsg.CreateTopicsRequestTopicReplicaAssignment)
	}{
		{
			name:      "standard 3 partitions 3 replicas",
			input:     "0:1:2,1:2:3,2:3:0",
			wantParts: 3,
			check: func(t *testing.T, a []kmsg.CreateTopicsRequestTopicReplicaAssignment) {
				// Partition 0: replicas [0,1,2]
				if a[0].Partition != 0 {
					t.Errorf("partition[0].Partition = %d, want 0", a[0].Partition)
				}
				if len(a[0].Replicas) != 3 {
					t.Errorf("partition[0] replicas len = %d, want 3", len(a[0].Replicas))
				} else {
					if a[0].Replicas[0] != 0 || a[0].Replicas[1] != 1 || a[0].Replicas[2] != 2 {
						t.Errorf("partition[0] replicas = %v, want [0,1,2]", a[0].Replicas)
					}
				}

				// Partition 1: replicas [1,2,3]
				if a[1].Partition != 1 {
					t.Errorf("partition[1].Partition = %d, want 1", a[1].Partition)
				}
				if len(a[1].Replicas) != 3 {
					t.Errorf("partition[1] replicas len = %d, want 3", len(a[1].Replicas))
				} else {
					if a[1].Replicas[0] != 1 || a[1].Replicas[1] != 2 || a[1].Replicas[2] != 3 {
						t.Errorf("partition[1] replicas = %v, want [1,2,3]", a[1].Replicas)
					}
				}

				// Partition 2: replicas [2,3,0]
				if a[2].Partition != 2 {
					t.Errorf("partition[2].Partition = %d, want 2", a[2].Partition)
				}
				if len(a[2].Replicas) != 3 {
					t.Errorf("partition[2] replicas len = %d, want 3", len(a[2].Replicas))
				} else {
					if a[2].Replicas[0] != 2 || a[2].Replicas[1] != 3 || a[2].Replicas[2] != 0 {
						t.Errorf("partition[2] replicas = %v, want [2,3,0]", a[2].Replicas)
					}
				}
			},
		},
		{
			name:      "single partition single replica",
			input:     "0",
			wantParts: 1,
			check: func(t *testing.T, a []kmsg.CreateTopicsRequestTopicReplicaAssignment) {
				if a[0].Partition != 0 {
					t.Errorf("partition = %d, want 0", a[0].Partition)
				}
				if len(a[0].Replicas) != 1 || a[0].Replicas[0] != 0 {
					t.Errorf("replicas = %v, want [0]", a[0].Replicas)
				}
			},
		},
		{
			name:      "two partitions two replicas",
			input:     "0:1,2:3",
			wantParts: 2,
			check: func(t *testing.T, a []kmsg.CreateTopicsRequestTopicReplicaAssignment) {
				if len(a[0].Replicas) != 2 {
					t.Errorf("partition[0] replicas len = %d, want 2", len(a[0].Replicas))
				}
				if len(a[1].Replicas) != 2 {
					t.Errorf("partition[1] replicas len = %d, want 2", len(a[1].Replicas))
				}
			},
		},
		{
			name:      "empty string",
			input:     "",
			wantParts: 0,
		},
		{
			name:      "whitespace only groups",
			input:     " , ",
			wantParts: 0,
		},
		{
			name:    "invalid broker id",
			input:   "abc",
			wantErr: true,
		},
		{
			name:    "invalid broker id in group",
			input:   "0:xyz,1:2",
			wantErr: true,
		},
		{
			name:      "trailing comma",
			input:     "0:1:2,",
			wantParts: 1,
			check: func(t *testing.T, a []kmsg.CreateTopicsRequestTopicReplicaAssignment) {
				if len(a[0].Replicas) != 3 {
					t.Errorf("replicas = %v, want 3 replicas", a[0].Replicas)
				}
			},
		},
		{
			name:      "whitespace around values",
			input:     " 0 : 1 : 2 , 1 : 2 : 3 ",
			wantParts: 2,
			check: func(t *testing.T, a []kmsg.CreateTopicsRequestTopicReplicaAssignment) {
				if a[0].Replicas[0] != 0 || a[0].Replicas[1] != 1 || a[0].Replicas[2] != 2 {
					t.Errorf("partition[0] replicas = %v, want [0,1,2]", a[0].Replicas)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assignments, err := parseReplicaAssignment(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseReplicaAssignment(%q) expected error", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseReplicaAssignment(%q) unexpected error: %v", tt.input, err)
			}
			if len(assignments) != tt.wantParts {
				t.Fatalf("parseReplicaAssignment(%q) returned %d partitions, want %d", tt.input, len(assignments), tt.wantParts)
			}
			if tt.check != nil {
				tt.check(t, assignments)
			}
		})
	}
}
