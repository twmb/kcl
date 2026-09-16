package group

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

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

// joinGroup joins group as a member consuming topics from the start and
// polls once, so that the group has a member with an assignment. With
// consumer set, the member speaks the KIP-848 protocol. Nothing is committed
// unless commit is set.
func joinGroup(t *testing.T, c *kfake.Cluster, group string, consumer, commit bool, topics ...string) *kgo.Client {
	t.Helper()
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	}
	if consumer {
		ctx := context.WithValue(context.Background(), "opt_in_kafka_next_gen_balancer_beta", true)
		opts = append(opts, kgo.WithContext(ctx))
	}
	m, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fs := m.PollFetches(ctx)
	if err := fs.Err(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if commit {
		if err := m.CommitUncommittedOffsets(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return m
}

// TestDescribeConsumerProtocolNamesTopics pins that a KIP-848 group whose
// broker sends assignments by topic ID alone, as kfake does, still describes
// by topic name: one row per partition, with the member that owns it, rather
// than a hex ID row with no offsets beside a named row with no member.
func TestDescribeConsumerProtocolNamesTopics(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	if _, err := adm.CreateTopic(ctx, 2, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	produceN(t, cl, "t", 10)
	joinGroup(t, c, "g848", true, true, "t")

	stdout, err := runDescribe(t, c, "g848", "--consumer-protocol", "--format", "awk")
	if err != nil {
		t.Fatal(err)
	}
	rows := awkRows(stdout)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2:\n%s", len(rows), stdout)
	}
	for i, row := range rows {
		if row[0] != "t" || row[1] != strconv.Itoa(i) || row[5] == "" {
			t.Errorf("row %d = %q, want topic t, partition %d, and a member", i, row, i)
		}
	}

	stdout, err = runDescribe(t, c, "g848", "--consumer-protocol", "--format", "awk", "--section", "members")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout, "\tt:") {
		t.Errorf("members section does not name the assigned topic:\n%s", stdout)
	}
}
