package consume

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
)

// These tests drive the real cobra command against an in-process kfake
// cluster. They are only possible because consume unwinds through its quit
// path instead of calling os.Exit -- previously any such test took the test
// binary down with it.

// seedCluster returns a cluster with topic "t" holding n records, and the
// broker addresses.
func seedCluster(t *testing.T, n int) (*kfake.Cluster, []string) {
	return seedClusterParts(t, n, 1)
}

// seedClusterParts returns a cluster with topic "t" of parts partitions,
// holding n records. With more than one partition the records are produced
// without a key, so they land wherever the partitioner puts them; the tests
// that use it care about the partitions that stay empty.
func seedClusterParts(t *testing.T, n, parts int) (*kfake.Cluster, []string) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreateTopic(t.Context(), int32(parts), 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		r := &kgo.Record{Topic: "t", Value: []byte(fmt.Sprintf("v%d", i))}
		if res := cl.ProduceSync(t.Context(), r); res.FirstErr() != nil {
			t.Fatal(res.FirstErr())
		}
	}
	return c, c.ListenAddrs()
}

// runConsume executes "kcl consume t ..." and returns the JSON objects printed.
func runConsume(t *testing.T, addrs []string, args ...string) []map[string]any {
	t.Helper()

	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	kcl := client.New(root)
	root.AddCommand(Command(kcl))
	root.SetArgs(append(append([]string{"consume", "t", "-f", "json"}, args...),
		"--no-config-file", "-B", strings.Join(addrs, ",")))

	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w

	// Drain concurrently: a pipe fills at 64KB and would deadlock a
	// producer that writes more than that before we read.
	var (
		wg  sync.WaitGroup
		buf bytes.Buffer
	)
	wg.Add(1)
	go func() { defer wg.Done(); io.Copy(&buf, r) }()

	runErr := root.Execute()
	w.Close()
	os.Stdout = old
	wg.Wait()
	r.Close()

	if runErr != nil {
		t.Fatalf("consume %v: %v", args, runErr)
	}

	var out []map[string]any
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	for dec.More() {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			t.Fatalf("output is not JSON objects (%v): %s", err, buf.Bytes())
		}
		out = append(out, m)
	}
	return out
}

// TestConsumeNumIsExact is the regression test for replacing os.Exit with the
// quit path. os.Exit stopped the process the instant the count was reached; a
// cooperative stop has to drop the rest of the fetched batch itself, or --num
// silently over-delivers.
func TestConsumeNumIsExact(t *testing.T) {
	_, addrs := seedCluster(t, 10)

	for _, n := range []int{1, 2, 5, 10} {
		got := runConsume(t, addrs, "-o", "start", "-n", fmt.Sprint(n))
		if len(got) != n {
			t.Errorf("-n %d printed %d records", n, len(got))
			continue
		}
		// And they are the first n, in order.
		for i, m := range got {
			if want := fmt.Sprintf("v%d", i); m["value"] != want {
				t.Errorf("-n %d record %d value = %v, want %s", n, i, m["value"], want)
			}
		}
	}
}

// isolationOf reports the isolation level of the first Fetch kcl sends.
func isolationOf(t *testing.T, args ...string) int8 {
	t.Helper()
	c, addrs := seedCluster(t, 1)

	var (
		mu    sync.Mutex
		level int8 = -1
	)
	c.KeepControl()
	c.ControlKey(kmsg.Fetch.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		mu.Lock()
		if level == -1 {
			level = req.(*kmsg.FetchRequest).IsolationLevel
		}
		mu.Unlock()
		return nil, nil, false // fall through to kfake's handler
	})

	runConsume(t, addrs, append([]string{"-o", "start", "-n", "1"}, args...)...)

	mu.Lock()
	defer mu.Unlock()
	if level == -1 {
		t.Fatal("no Fetch was observed")
	}
	return level
}

// kcl used to default to read_committed, the opposite of the Java client,
// librdkafka, rpk, and kcat -- and a default that cannot advance past an open
// transaction's LSO, so one long transaction made consume print nothing.
// Asserting on the flag alone would not have caught the old default, so this
// asserts on the wire. 0 is read_uncommitted, 1 is read_committed.
func TestConsumeIsolationDefaultIsUncommitted(t *testing.T) {
	if got := isolationOf(t); got != 0 {
		t.Errorf("default isolation = %d, want 0 (read_uncommitted)", got)
	}
}

func TestConsumeReadCommittedOptsIn(t *testing.T) {
	if got := isolationOf(t, "--read-committed"); got != 1 {
		t.Errorf("--read-committed isolation = %d, want 1 (read_committed)", got)
	}
}

// The old flag is a deprecated no-op: read_uncommitted is the default now, so
// anyone still passing it gets what they asked for.
func TestConsumeReadUncommittedIsANoop(t *testing.T) {
	if got := isolationOf(t, "--read-uncommitted"); got != 0 {
		t.Errorf("--read-uncommitted isolation = %d, want 0 (read_uncommitted)", got)
	}
}

// --print-control-records shared an if/else with the isolation level, so asking
// for control records silently switched isolation as a side effect.
func TestConsumeControlRecordsDoesNotChangeIsolation(t *testing.T) {
	if got := isolationOf(t, "--print-control-records", "--read-committed"); got != 1 {
		t.Errorf("isolation = %d, want 1 alongside --print-control-records", got)
	}
}

// TestConsumeEndOffsetTerminates covers two former hangs. An exact end
// (-o N:M) had no termination path at all, and :end only finished if it saw a
// record AT the end offset -- which never exists when the end is the high
// watermark. Both printed their range and then waited forever.
func TestConsumeEndOffsetTerminates(t *testing.T) {
	_, addrs := seedCluster(t, 5)

	if got := runConsume(t, addrs, "-o", "0:3"); len(got) != 3 {
		t.Errorf("-o 0:3 got %d records, want 3 (end is exclusive)", len(got))
	}
	if got := runConsume(t, addrs, "-o", ":end"); len(got) != 5 {
		t.Errorf("-o :end got %d records, want 5", len(got))
	}
	if got := runConsume(t, addrs, "-o", "1:4"); len(got) != 3 {
		t.Errorf("-o 1:4 got %d records, want 3", len(got))
	}
}

// TestConsumeTimeoutFires pins that --timeout can fire at all. The check sat at
// the top of the poll loop while PollFetches blocks until records arrive, so
// with no traffic the loop never returned to it and the flag did nothing.
func TestConsumeTimeoutFires(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kadm.NewClient(cl).CreateTopic(t.Context(), 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	cl.Close()

	start := time.Now()
	if got := runConsume(t, c.ListenAddrs(), "--timeout", "400ms"); len(got) != 0 {
		t.Errorf("got %d records from an empty topic", len(got))
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("--timeout took %s; it did not fire", elapsed)
	}
}

// TestConsumeEndOffsetMultiPartition is the multi-partition half of the
// end-offset fix. An exact end used to be applied to every partition
// regardless of what it held, so a partition short of that offset -- an empty
// one, most commonly -- was never finished and the consume hung after printing
// everything it had. The end is now the minimum of the request and each
// partition's high watermark.
func TestConsumeEndOffsetMultiPartition(t *testing.T) {
	// Three partitions, two records. They are unkeyed, so at least one
	// partition is guaranteed empty.
	_, addrs := seedClusterParts(t, 2, 3)

	got := runConsume(t, addrs, "-o", "0:2")
	if len(got) != 2 {
		t.Errorf("-o 0:2 got %d records, want 2", len(got))
	}

	// An end past everything is bounded by what exists, rather than
	// waiting for records that may never arrive.
	got = runConsume(t, addrs, "-o", "0:100")
	if len(got) != 2 {
		t.Errorf("-o 0:100 got %d records, want 2", len(got))
	}
}
