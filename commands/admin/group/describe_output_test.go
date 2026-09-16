package group

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
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

// produceTo produces n records to each named partition of topic.
func produceTo(t *testing.T, c *kfake.Cluster, topic string, n int, partitions ...int32) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := context.Background()
	for _, p := range partitions {
		for range n {
			if res := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: p, Value: []byte("v")}); res.FirstErr() != nil {
				t.Fatal(res.FirstErr())
			}
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
	joinGroup(t, c, "trim-classic", false, "t")
	joinGroup(t, c, "trim-consumer", true, "t")

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

// joinGroup joins group as a member of topics and returns once the member
// holds an assignment, so that describe sees it. The member commits nothing
// and stays in the group until the test ends. With consumer set, the member
// speaks the KIP-848 protocol.
func joinGroup(t *testing.T, c *kfake.Cluster, group string, consumer bool, topics ...string) {
	t.Helper()
	assigned := make(chan struct{})
	var once sync.Once
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) {
			once.Do(func() { close(assigned) })
		}),
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

	// The first poll joins the group.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.PollFetches(ctx)
	}()
	select {
	case <-assigned:
	case <-ctx.Done():
		t.Fatalf("group %s: no assignment within 5s", group)
	}
	cancel()
	<-done
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
	joinGroup(t, c, "g848", true, "t")

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

func TestParseLagFilter(t *testing.T) {
	for _, test := range []struct {
		expr string
		op   string
		n    int64
		bad  bool
	}{
		{expr: ">0", op: ">", n: 0},
		{expr: ">=10", op: ">=", n: 10},
		{expr: "<5", op: "<", n: 5},
		{expr: "<=5", op: "<=", n: 5},
		{expr: "=0", op: "=", n: 0},
		{expr: "7", op: ">=", n: 7},
		{expr: " > 3 ", op: ">", n: 3},
		{expr: ">=\t100", op: ">=", n: 100},
		{expr: "", bad: true},
		{expr: ">", bad: true},
		{expr: "foo", bad: true},
		{expr: "5x", bad: true},
		{expr: "==5", bad: true},
		{expr: "!=5", bad: true},
		{expr: "99999999999999999999", bad: true},
	} {
		t.Run(test.expr, func(t *testing.T) {
			f, err := parseLagFilter(test.expr)
			if test.bad {
				if err == nil {
					t.Fatalf("parsed %+v, want an error", f)
				}
				if out.ExitCode(err) != out.ExitUsage {
					t.Errorf("exit code %d, want %d", out.ExitCode(err), out.ExitUsage)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if f.op != test.op || f.n != test.n {
				t.Errorf("parsed %s %d, want %s %d", f.op, f.n, test.op, test.n)
			}
		})
	}
}

// TestDescribeLagFilter pins --lag: a group with no partition left is dropped
// from every format, a surviving group keeps its full TOTAL-LAG, a partition
// whose lag is unknown never matches, and dropping every group prints
// nothing in text and an empty groups list in JSON.
func TestDescribeLagFilter(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	if _, err := adm.CreateTopics(ctx, 2, 1, nil, "t", "empty"); err != nil {
		t.Fatal(err)
	}
	produceTo(t, c, "t", 10, 0, 1)
	// behind: lag 2 on partition 0 and 8 on partition 1, 10 in total.
	commitAt(t, adm, "behind", "t", 0, 8)
	commitAt(t, adm, "behind", "t", 1, 2)
	// caught-up: lag 0 on both.
	commitAt(t, adm, "caught-up", "t", 0, 10)
	commitAt(t, adm, "caught-up", "t", 1, 10)
	// unknown: a member on an empty topic with nothing committed, so no
	// lag can be computed.
	joinGroup(t, c, "unknown", false, "empty")

	all := []string{"behind", "caught-up", "unknown"}

	t.Run("text keeps the behind group whole", func(t *testing.T) {
		stdout, err := runDescribe(t, c, append(all, "--lag", ">5")...)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(stdout, "GROUP ") != 1 || !strings.Contains(stdout, "GROUP        behind\n") {
			t.Errorf("want only the behind group:\n%s", stdout)
		}
		if !strings.Contains(stdout, "TOTAL-LAG    10\n") {
			t.Errorf("TOTAL-LAG should be the full 10, not the filtered 8:\n%s", stdout)
		}
		if strings.Count(stdout, "\nt  ") != 1 {
			t.Errorf("want one partition row, the one with lag 8:\n%s", stdout)
		}
	})

	t.Run("awk rows of the surviving group only", func(t *testing.T) {
		stdout, err := runDescribe(t, c, append(all, "--lag", ">=2", "--format", "awk")...)
		if err != nil {
			t.Fatal(err)
		}
		rows := awkRows(stdout)
		if len(rows) != 2 || rows[0][0] != "behind" || rows[1][0] != "behind" || rows[0][6] != "2" || rows[1][6] != "8" {
			t.Errorf("want the two behind rows with lag 2 and 8, got:\n%s", stdout)
		}
	})

	t.Run("unknown lag never matches", func(t *testing.T) {
		stdout, err := runDescribe(t, c, "unknown", "--lag", "<=1000000")
		if err != nil {
			t.Fatal(err)
		}
		if stdout != "" {
			t.Errorf("want nothing on stdout, got:\n%s", stdout)
		}
	})

	t.Run("every group dropped", func(t *testing.T) {
		for _, format := range []string{"text", "awk"} {
			stdout, err := runDescribe(t, c, append(all, "--lag", ">100", "--format", format)...)
			if err != nil {
				t.Fatal(err)
			}
			if stdout != "" {
				t.Errorf("%s: want nothing on stdout, got:\n%s", format, stdout)
			}
		}
		stdout, err := runDescribe(t, c, append(all, "--lag", ">100", "--format", "json")...)
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Command string           `json:"_command"`
			Version int              `json:"_version"`
			Groups  []map[string]any `json:"groups"`
		}
		if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
			t.Fatalf("stdout is not JSON: %v\n%s", err, stdout)
		}
		if doc.Command != "group.describe" || doc.Version != 1 || doc.Groups == nil || len(doc.Groups) != 0 {
			t.Errorf("want an empty groups list under _command and _version, got: %s", stdout)
		}
	})

	t.Run("json keeps the caught-up group at =0", func(t *testing.T) {
		stdout, err := runDescribe(t, c, append(all, "--lag", "=0", "--format", "json")...)
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Groups []struct {
				Group string           `json:"group"`
				Lag   []map[string]any `json:"lag"`
			} `json:"groups"`
		}
		if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
			t.Fatalf("stdout is not JSON: %v\n%s", err, stdout)
		}
		if len(doc.Groups) != 1 || doc.Groups[0].Group != "caught-up" || len(doc.Groups[0].Lag) != 2 {
			t.Errorf("want caught-up with two rows, got: %s", stdout)
		}
	})

	t.Run("bad expression", func(t *testing.T) {
		for _, expr := range []string{"foo", ""} {
			_, err := runDescribe(t, c, "behind", "--lag", expr)
			if err == nil || out.ExitCode(err) != out.ExitUsage {
				t.Errorf("--lag %q: err = %v, want a usage error", expr, err)
			}
		}
	})
}
