package group

import (
	"context"
	"encoding/json"
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
	// Rows are printed in group order, and each begins with its group.
	for i, want := range []struct{ group, current, lag string }{{"own-a", "3", "7"}, {"own-b", "7", "3"}} {
		if rows[i][0] != want.group || rows[i][3] != want.current || rows[i][6] != want.lag {
			t.Errorf("row %d = %q, want group %s, current %s, lag %s", i, rows[i], want.group, want.current, want.lag)
		}
	}
}

// TestDescribeLogStartOffset pins the LOG-START-OFFSET column against a log
// trimmed with DeleteRecords, and that a partition with nothing committed
// counts its lag from the log start rather than from zero.
func TestDescribeLogStartOffset(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	if _, err := adm.CreateTopic(ctx, 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	produceN(t, cl, "t", 10)
	var trim kadm.Offsets
	trim.Add(kadm.Offset{Topic: "t", Partition: 0, At: 4})
	if _, err := adm.DeleteRecords(ctx, trim); err != nil {
		t.Fatal(err)
	}
	commitAt(t, adm, "trim-committed", "t", 0, 6)
	joinGroup(t, c, "trim-classic", false, false, "t")
	joinGroup(t, c, "trim-consumer", true, false, "t")

	for _, test := range []struct {
		group   string
		args    []string
		current string
		lag     string
		member  bool
	}{
		{group: "trim-committed", current: "6", lag: "4"},
		{group: "trim-classic", current: "-", lag: "6", member: true},
		{group: "trim-consumer", args: []string{"--consumer-protocol"}, current: "-", lag: "6", member: true},
	} {
		t.Run(test.group, func(t *testing.T) {
			stdout, err := runDescribe(t, c, append([]string{test.group, "--format", "awk"}, test.args...)...)
			if err != nil {
				t.Fatal(err)
			}
			rows := awkRows(stdout)
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1:\n%s", len(rows), stdout)
			}
			row := rows[0]
			if row[3] != test.current || row[4] != "4" || row[5] != "10" || row[6] != test.lag || (row[7] != "") != test.member {
				t.Errorf("row = %q, want current %s, start 4, end 10, lag %s, member %v", row, test.current, test.lag, test.member)
			}
		})
	}

	stdout, err := runDescribe(t, c, "trim-committed", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Groups []struct {
			Lag []struct {
				LogStartOffset int64 `json:"log_start_offset"`
				LogEndOffset   int64 `json:"log_end_offset"`
			} `json:"lag"`
		} `json:"groups"`
	}
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout)
	}
	if len(doc.Groups) != 1 || len(doc.Groups[0].Lag) != 1 {
		t.Fatalf("unexpected document: %s", stdout)
	}
	if got := doc.Groups[0].Lag[0]; got.LogStartOffset != 4 || got.LogEndOffset != 10 {
		t.Errorf("log_start_offset %d, log_end_offset %d, want 4 and 10", got.LogStartOffset, got.LogEndOffset)
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
		if row[1] != "t" || row[2] != strconv.Itoa(i) || row[7] == "" {
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
