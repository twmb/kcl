package admin

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// kfake does not implement DescribeQuorum, so the rows are checked against a
// response built by hand.
func quorumResponse() *kmsg.DescribeQuorumResponse {
	msg := "no leader"
	return &kmsg.DescribeQuorumResponse{Topics: []kmsg.DescribeQuorumResponseTopic{{
		Topic: "__cluster_metadata",
		Partitions: []kmsg.DescribeQuorumResponseTopicPartition{
			{
				Partition: 0, LeaderID: 2, LeaderEpoch: 7, HighWatermark: 100,
				CurrentVoters: []kmsg.DescribeQuorumResponseTopicPartitionReplicaState{
					{ReplicaID: 3, LogEndOffset: 99},
					{ReplicaID: 2, LogEndOffset: 100},
				},
				Observers: []kmsg.DescribeQuorumResponseTopicPartitionReplicaState{
					{ReplicaID: 10, LogEndOffset: 98},
				},
			},
			{Partition: 1, ErrorCode: 6, ErrorMessage: &msg}, // NOT_LEADER_FOR_PARTITION
		},
	}}}
}

func TestQuorumRows(t *testing.T) {
	for _, test := range []struct {
		section string
		want    []string
	}{
		{"", []string{
			"__cluster_metadata 0 2 7 100 voter 2 100 0 0 ",
			"__cluster_metadata 0 2 7 100 voter 3 99 0 0 ",
			"__cluster_metadata 0 2 7 100 observer 10 98 0 0 ",
			"__cluster_metadata 1 - - - - - - - - NOT_LEADER_FOR_PARTITION: no leader",
		}},
		{"voters", []string{
			"__cluster_metadata 0 2 7 100 voter 2 100 0 0 ",
			"__cluster_metadata 0 2 7 100 voter 3 99 0 0 ",
			"__cluster_metadata 1 - - - - - - - - NOT_LEADER_FOR_PARTITION: no leader",
		}},
		{"observers", []string{
			"__cluster_metadata 0 2 7 100 observer 10 98 0 0 ",
			"__cluster_metadata 1 - - - - - - - - NOT_LEADER_FOR_PARTITION: no leader",
		}},
	} {
		t.Run("section "+test.section, func(t *testing.T) {
			rows := quorumRows(quorumResponse(), test.section)
			var got []string
			for _, row := range rows {
				if len(row) != len(quorumHeaders) {
					t.Errorf("row has %d cells, headers %d: %v", len(row), len(quorumHeaders), row)
				}
				var cells []string
				for _, c := range row {
					cells = append(cells, strings.TrimSpace(sprint(c)))
				}
				got = append(got, strings.Join(cells, " "))
			}
			if strings.Join(got, "\n") != strings.Join(test.want, "\n") {
				t.Errorf("rows:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(test.want, "\n"))
			}
		})
	}
}

func sprint(v any) string {
	if s, ok := v.(interface{ String() string }); ok {
		return s.String()
	}
	b, _ := json.Marshal(v)
	return strings.Trim(string(b), `"`)
}

// TestQuorumJSON pins that voters and observers are arrays, sorted, and that
// the section not asked for is absent rather than null or empty.
func TestQuorumJSON(t *testing.T) {
	raw, err := json.Marshal(quorumJSON(quorumResponse(), ""))
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"topic":"__cluster_metadata","partition":0,"leader":2,"leader_epoch":7,"high_watermark":100,"error":"",` +
		`"voters":[{"replica_id":2,"log_end_offset":100,"last_fetch_timestamp":0,"last_caught_up_timestamp":0},{"replica_id":3,"log_end_offset":99,"last_fetch_timestamp":0,"last_caught_up_timestamp":0}],` +
		`"observers":[{"replica_id":10,"log_end_offset":98,"last_fetch_timestamp":0,"last_caught_up_timestamp":0}]},` +
		`{"topic":"__cluster_metadata","partition":1,"leader":0,"leader_epoch":0,"high_watermark":0,"error":"NOT_LEADER_FOR_PARTITION: no leader","voters":[],"observers":[]}]`
	if string(raw) != want {
		t.Errorf("JSON:\n%s\nwant:\n%s", raw, want)
	}

	raw, _ = json.Marshal(quorumJSON(quorumResponse(), "voters"))
	if strings.Contains(string(raw), "observers") || !strings.Contains(string(raw), `"voters":[{`) {
		t.Errorf("--section voters JSON = %s, want voters only", raw)
	}
	raw, _ = json.Marshal(quorumJSON(quorumResponse(), "observers"))
	if strings.Contains(string(raw), "voters") || !strings.Contains(string(raw), `"observers":[{`) {
		t.Errorf("--section observers JSON = %s, want observers only", raw)
	}
}
