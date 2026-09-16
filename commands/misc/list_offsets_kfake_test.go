package misc

import (
	"context"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestListOffsetsAWKRowShape pins that an awk row ends in a column rather
// than a tab: ERROR is last and usually empty, and a dash goes there.
func TestListOffsetsAWKRowShape(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "offsets-topic"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	pcl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer pcl.Close()
	if err := pcl.ProduceSync(context.Background(), &kgo.Record{Topic: "offsets-topic", Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name    string
		args    []string
		lastCol string
	}{
		{"awk", []string{"--format", "awk"}, "-"},
		{"text keeps the empty column", []string{"--format", "text"}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := append([]string{
				"-B", addr, "-X", "dial_timeout=2s", "-X", "retry_timeout=10s",
			}, test.args...)
			got, err := runMisc(t, append(args, "misc", "list-offsets", "offsets-topic")...)
			if err != nil {
				t.Fatalf("list-offsets: %v\n%s", err, got)
			}
			lines := strings.Split(strings.TrimSuffix(got, "\n"), "\n")
			last := lines[len(lines)-1]
			if test.lastCol == "-" {
				if strings.HasSuffix(last, "\t") {
					t.Errorf("awk row ends in a tab: %q", last)
				}
				fields := strings.Split(last, "\t")
				if len(fields) != 7 {
					t.Fatalf("awk row = %d fields, want 7: %q", len(fields), last)
				}
				if fields[6] != "-" {
					t.Errorf("awk ERROR = %q, want a dash", fields[6])
				}
			} else if !strings.HasSuffix(last, " ") && !strings.Contains(last, "offsets-topic") {
				t.Errorf("text row = %q", last)
			}
		})
	}
}
