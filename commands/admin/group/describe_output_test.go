package group

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/client"
)

// runDescribe runs kcl group describe against the cluster with the given
// arguments and returns what it wrote to stdout.
func runDescribe(t *testing.T, c *kfake.Cluster, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs(append([]string{"--no-config-file", "-B", c.ListenAddrs()[0], "-X", "dial_timeout=2s", "-X", "retry_timeout=10s", "group", "describe"}, args...))

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	runErr := root.Execute()
	w.Close()
	os.Stdout = old
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b), runErr
}

// awkRows splits awk output into rows of fields.
func awkRows(s string) [][]string {
	var rows [][]string
	for line := range strings.SplitSeq(strings.TrimSuffix(s, "\n"), "\n") {
		if line == "" {
			continue
		}
		rows = append(rows, strings.Split(line, "\t"))
	}
	return rows
}

func produceN(t *testing.T, cl *kgo.Client, topic string, n int) {
	t.Helper()
	ctx := context.Background()
	for range n {
		if res := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("v")}); res.FirstErr() != nil {
			t.Fatal(res.FirstErr())
		}
	}
}

func commitAt(t *testing.T, adm *kadm.Client, group, topic string, partition int32, at int64) {
	t.Helper()
	var offsets kadm.Offsets
	offsets.Add(kadm.Offset{Topic: topic, Partition: partition, At: at, LeaderEpoch: -1})
	resp, err := adm.CommitOffsets(context.Background(), group, offsets)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Error(); err != nil {
		t.Fatal(err)
	}
}

// TestDescribeEachGroupOwnOffsets pins that two groups committed to the same
// partition each describe with their own committed offset. The offsets used
// to be fetched into one map keyed by partition, so every group printed the
// last group's commit.
func TestDescribeEachGroupOwnOffsets(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	if _, err := adm.CreateTopic(ctx, 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	produceN(t, cl, "t", 10)
	commitAt(t, adm, "own-a", "t", 0, 3)
	commitAt(t, adm, "own-b", "t", 0, 7)

	stdout, err := runDescribe(t, c, "own-a", "own-b", "--format", "awk")
	if err != nil {
		t.Fatal(err)
	}
	rows := awkRows(stdout)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2:\n%s", len(rows), stdout)
	}
	// Rows are printed in group order: own-a, then own-b. CURRENT-OFFSET
	// is the third column and LAG the fifth.
	for i, want := range []struct{ current, lag string }{{"3", "7"}, {"7", "3"}} {
		if rows[i][2] != want.current || rows[i][4] != want.lag {
			t.Errorf("row %d = %q, want current %s and lag %s", i, rows[i], want.current, want.lag)
		}
	}
}
