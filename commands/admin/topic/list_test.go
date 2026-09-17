package topic

import (
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestList(t *testing.T) {
	c, addr := newCluster(t, 2, 0, "logs.a", "logs.b", "other")

	for _, test := range []struct {
		name   string
		args   []string
		fault  *kfake.Fault
		topics []string // TOPIC column, in order
		errors int      // rows with ERROR set
		code   int
	}{
		{name: "all", args: []string{"list"}, topics: []string{"logs.a", "logs.b", "other"}},
		{name: "by name", args: []string{"list", "other", "logs.a"}, topics: []string{"logs.a", "other"}},
		{name: "regex", args: []string{"list", "-r", `^logs\.`}, topics: []string{"logs.a", "logs.b"}},
		{name: "old -r PATTERN form", args: []string{"list", "-r", "other"}, topics: []string{"other"}},
		{name: "missing topic stays in the table", args: []string{"list", "nosuch", "other"}, topics: []string{"nosuch", "other"}, errors: 1, code: 1},
		{
			// A full listing leaves out a topic the broker will not
			// answer for, as Kafka does an unauthorized one, so the
			// error shows when the topic is named.
			name: "broker error on a named topic",
			args: []string{"list", "logs.a", "logs.b", "other"},
			fault: &kfake.Fault{
				Keys:  []kmsg.Key{kmsg.Metadata},
				Topic: "logs.b",
				Err:   kerr.LeaderNotAvailable,
				Count: -1,
			},
			topics: []string{"logs.a", "logs.b", "other"},
			errors: 1,
			code:   1,
		},
		{name: "--regex=PATTERN is an error", args: []string{"list", "--regex=logs"}, code: 2},
		{name: "bad regex", args: []string{"list", "-r", "("}, code: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.fault != nil {
				h := c.Fault(*test.fault)
				defer h.Remove()
			}
			for _, format := range []string{"awk", "json"} {
				got, code := runKcl(t, addr, append(slices.Clone(test.args), "--format", format)...)
				if code != test.code {
					t.Fatalf("%s: exit %d, want %d\n%s", format, code, test.code, got)
				}
				if test.code == 2 {
					continue
				}
				var topics []string
				var errors int
				switch format {
				case "awk":
					for _, row := range awkRows(t, got, len(ListHeaders)) {
						topics = append(topics, row[0])
						if row[5] != "-" {
							errors++
						}
					}
				case "json":
					for _, row := range rowsOf(t, jsonDoc(t, got, "topic.list"), "topics") {
						if got, want := keysOf(row), ListKeys; !slices.Equal(got, slices.Sorted(slices.Values(want))) {
							t.Errorf("row keys = %v, want %v", got, want)
						}
						topics = append(topics, row["topic"].(string))
						if row["error"] != "" {
							errors++
							if row["partition_count"] != nil {
								t.Errorf("errored row partition_count = %v, want null", row["partition_count"])
							}
						} else if row["partition_count"] != float64(2) || row["replication_factor"] != float64(1) || row["internal"] != false {
							t.Errorf("row = %v", row)
						}
					}
				}
				if !slices.Equal(topics, test.topics) {
					t.Errorf("%s: topics = %v, want %v", format, topics, test.topics)
				}
				if errors != test.errors {
					t.Errorf("%s: %d rows with an error, want %d", format, errors, test.errors)
				}
			}
		})
	}
}

// TestListDetailed pins that --detailed is topic describe: its rows and its
// _command.
func TestListDetailed(t *testing.T) {
	_, addr := newCluster(t, 2, 0, "a", "b")
	got, code := runKcl(t, addr, "list", "--detailed", "--format", "awk")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, got)
	}
	if rows := awkRows(t, got, len(describePartitionsHeaders)); len(rows) != 4 {
		t.Errorf("got %d partition rows, want 4:\n%s", len(rows), got)
	}
	got, _ = runKcl(t, addr, "list", "--detailed", "--format", "json")
	doc := jsonDoc(t, got, "topic.describe")
	if rows := rowsOf(t, doc, "topics"); len(rows) != 2 || !strings.Contains(got, `"partitions":[`) {
		t.Errorf("detailed json = %s", got)
	}
}
