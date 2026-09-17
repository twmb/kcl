package topic

import (
	"slices"
	"strings"
	"testing"
)

var (
	describeTopicKeys     = []string{"configs", "error", "internal", "partition_count", "partitions", "replication_factor", "topic", "topic_id"}
	describePartitionKeys = []string{"end_offset", "error", "isr", "leader", "leader_epoch", "offline_replicas", "partition", "replicas", "stable_offset", "start_offset"}
)

// TestDescribeAWK pins the one shape per section: a fixed column count, the
// offsets filled from ListOffsets, STABLE-OFFSET a dash unless --stable, and
// an errored topic kept as one row.
func TestDescribeAWK(t *testing.T) {
	_, addr := newCluster(t, 2, 3, "awk-topic")

	for _, test := range []struct {
		name    string
		args    []string
		columns int
		rows    int
		code    int
		check   func(t *testing.T, rows [][]string)
	}{
		{
			name: "partitions", args: []string{"awk-topic"}, columns: len(describePartitionsHeaders), rows: 2,
			check: func(t *testing.T, rows [][]string) {
				for _, row := range rows {
					// TOPIC PARTITION LEADER LEADER-EPOCH REPLICAS ISR OFFLINE START END STABLE ERROR
					if row[2] != "0" || row[3] == "-" || row[7] != "0" || row[8] != "3" || row[9] != "-" || row[10] != "-" {
						t.Errorf("partition row = %v", row)
					}
				}
			},
		},
		{
			name: "partitions with --stable", args: []string{"awk-topic", "--stable"}, columns: len(describePartitionsHeaders), rows: 2,
			check: func(t *testing.T, rows [][]string) {
				for _, row := range rows {
					if row[9] != "3" {
						t.Errorf("STABLE-OFFSET = %q, want 3", row[9])
					}
				}
			},
		},
		{
			name: "summary", args: []string{"awk-topic", "--section", "summary"}, columns: len(describeSummaryHeaders), rows: 1,
			check: func(t *testing.T, rows [][]string) {
				if row := rows[0]; row[0] != "awk-topic" || len(row[1]) != 32 || row[2] != "2" || row[3] != "1" || row[4] != "false" || row[5] != "-" {
					t.Errorf("summary row = %v", row)
				}
			},
		},
		{
			name: "configs", args: []string{"awk-topic", "--section", "configs"}, columns: len(describeConfigsHeaders), rows: -1,
			check: func(t *testing.T, rows [][]string) {
				if len(rows) == 0 {
					t.Fatal("no config rows")
				}
				for _, row := range rows {
					if !strings.HasSuffix(row[3], "_CONFIG") || row[5] != "-" {
						t.Errorf("config row = %v", row)
					}
				}
			},
		},
		{
			name: "errored topic keeps a row", args: []string{"nosuch", "awk-topic"}, columns: len(describePartitionsHeaders), rows: 3, code: 1,
			check: func(t *testing.T, rows [][]string) {
				if row := rows[2]; row[0] != "nosuch" || row[1] != "-" || !strings.Contains(row[10], "UNKNOWN_TOPIC_OR_PARTITION") {
					t.Errorf("errored row = %v", row)
				}
			},
		},
		{
			name: "errored topic in the summary", args: []string{"nosuch", "--section", "summary"}, columns: len(describeSummaryHeaders), rows: 1, code: 1,
		},
		{name: "at min isr reads the config", args: []string{"awk-topic", "--at-min-isr"}, columns: len(describePartitionsHeaders), rows: 2},
		{name: "under min isr", args: []string{"awk-topic", "--under-min-isr"}, columns: len(describePartitionsHeaders), rows: 0},
		{name: "under replicated", args: []string{"awk-topic", "--under-replicated"}, columns: len(describePartitionsHeaders), rows: 0},
		{name: "bad section", args: []string{"awk-topic", "--section", "nope"}, code: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, code := runKcl(t, addr, append([]string{"describe", "--format", "awk"}, test.args...)...)
			if code != test.code {
				t.Fatalf("exit %d, want %d\n%s", code, test.code, got)
			}
			if test.code == 2 {
				return
			}
			rows := awkRows(t, got, test.columns)
			if test.rows >= 0 && len(rows) != test.rows {
				t.Fatalf("got %d rows, want %d:\n%s", len(rows), test.rows, got)
			}
			if test.check != nil {
				test.check(t, rows)
			}
		})
	}
}

// TestDescribeJSON pins the keys, that stable_offset is null without
// --stable, that --section leaves the other sections out, and that an
// errored topic stays in the array.
func TestDescribeJSON(t *testing.T) {
	_, addr := newCluster(t, 1, 2, "js")

	got, code := runKcl(t, addr, "describe", "js", "nosuch", "--format", "json")
	if code != 1 {
		t.Fatalf("exit %d, want 1\n%s", code, got)
	}
	topics := rowsOf(t, jsonDoc(t, got, "topic.describe"), "topics")
	if len(topics) != 2 {
		t.Fatalf("topics = %v", topics)
	}
	js, nosuch := topics[0], topics[1]
	if !slices.Equal(keysOf(js), describeTopicKeys) {
		t.Errorf("topic keys = %v, want %v", keysOf(js), describeTopicKeys)
	}
	if js["error"] != "" || js["partition_count"] != float64(1) || js["replication_factor"] != float64(1) || js["internal"] != false {
		t.Errorf("js = %v", js)
	}
	parts := js["partitions"].([]any)
	if len(parts) != 1 {
		t.Fatalf("partitions = %v", parts)
	}
	p := parts[0].(map[string]any)
	if !slices.Equal(keysOf(p), describePartitionKeys) {
		t.Errorf("partition keys = %v, want %v", keysOf(p), describePartitionKeys)
	}
	if p["leader"] != float64(0) || p["start_offset"] != float64(0) || p["end_offset"] != float64(2) || p["stable_offset"] != nil || p["error"] != "" {
		t.Errorf("partition = %v", p)
	}
	if _, ok := p["leader_epoch"].(float64); !ok {
		t.Errorf("leader_epoch = %v, want a number", p["leader_epoch"])
	}
	if configs, ok := js["configs"].([]any); !ok || len(configs) == 0 {
		t.Errorf("configs = %v", js["configs"])
	}
	if nosuch["topic"] != "nosuch" || nosuch["partition_count"] != nil || nosuch["internal"] != nil || !strings.Contains(nosuch["error"].(string), "UNKNOWN_TOPIC_OR_PARTITION") {
		t.Errorf("nosuch = %v", nosuch)
	}
	if parts, ok := nosuch["partitions"].([]any); !ok || len(parts) != 0 {
		t.Errorf("nosuch partitions = %v, want []", nosuch["partitions"])
	}

	for _, test := range []struct {
		section string
		absent  []string
	}{
		{"summary", []string{"partitions", "configs"}},
		{"partitions", []string{"configs"}},
		{"configs", []string{"partitions"}},
	} {
		got, code := runKcl(t, addr, "describe", "js", "--section", test.section, "--format", "json")
		if code != 0 {
			t.Fatalf("--section %s: exit %d\n%s", test.section, code, got)
		}
		js := rowsOf(t, jsonDoc(t, got, "topic.describe"), "topics")[0]
		for _, key := range test.absent {
			if _, ok := js[key]; ok {
				t.Errorf("--section %s carries %s", test.section, key)
			}
		}
	}

	got, code = runKcl(t, addr, "describe", "js", "--stable", "--format", "json")
	if code != 0 || !strings.Contains(got, `"stable_offset":2`) {
		t.Errorf("--stable: exit %d %s", code, got)
	}
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
