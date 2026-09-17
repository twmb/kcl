package sharegroup

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func newTestCluster(t *testing.T) (*kfake.Cluster, *kgo.Client) {
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

func newRoot() *cobra.Command {
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	return root
}

// runShareGroup runs kcl with the given arguments, "share-group" first,
// against the cluster, with stdin as its standard input, and returns what
// it wrote to stdout. A pipe is never a terminal, so a [y/N] prompt answers
// no.
func runShareGroup(t *testing.T, c *kfake.Cluster, stdin string, args ...string) (string, error) {
	t.Helper()
	root := newRoot()
	root.SetArgs(append([]string{"--no-config-file", "-B", c.ListenAddrs()[0], "-X", "dial_timeout=2s", "-X", "retry_timeout=10s"}, args...))

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		io.WriteString(inW, stdin)
		inW.Close()
	}()
	oldOut, oldIn := os.Stdout, os.Stdin
	os.Stdout, os.Stdin = w, inR
	runErr := root.Execute()
	w.Close()
	os.Stdout, os.Stdin = oldOut, oldIn
	inR.Close()
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

// awkHeader is the header row --awk-header prints for the command the
// arguments name, with its flags parsed so that --section selects the table
// it would at run time.
func awkHeader(t *testing.T, args ...string) []string {
	t.Helper()
	root := newRoot()
	cmd, rest, err := root.Find(args)
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.ParseFlags(rest); err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(out.AwkHeader(cmd), "\n"), "\t")
}

// checkAwkFields fails when a row of stdout has a field count other than the
// registered header's.
func checkAwkFields(t *testing.T, stdout string, args ...string) {
	t.Helper()
	header := awkHeader(t, args...)
	for i, row := range awkRows(stdout) {
		if len(row) != len(header) {
			t.Errorf("awk row %d has %d fields, --awk-header has %d: %q vs %q", i, len(row), len(header), row, header)
		}
	}
}

func parseJSON(t *testing.T, stdout string) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout)
	}
	return doc
}

func produceN(t *testing.T, cl *kgo.Client, topic string, n int) {
	t.Helper()
	for range n {
		if res := cl.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Value: []byte("v")}); res.FirstErr() != nil {
			t.Fatal(res.FirstErr())
		}
	}
}

// joinShareGroup consumes topics as a member of the share group until it
// has fetched n records, then leaves without acknowledging them, so that the
// group exists, Empty, and every partition it read has a start offset of 0
// and the records as lag.
func joinShareGroup(t *testing.T, c *kfake.Cluster, group string, n int, topics ...string) {
	t.Helper()
	// Share groups start at the end of the log by default.
	c.SetGroupConfigs(group, map[string]string{"share.auto.offset.reset": "earliest"})

	m, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ShareGroup(group),
		kgo.ConsumeTopics(topics...),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for got := 0; got < n; {
		fs := m.PollFetches(ctx)
		if err := fs.Err(); err != nil {
			t.Fatalf("share group %s: %v", group, err)
		}
		got += fs.NumRecords()
	}
}

func unmarshalDoc(s string, v any) error {
	return json.Unmarshal([]byte(s), v)
}
