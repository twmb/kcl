package topic

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

// newCluster starts a one broker kfake with the topics seeded at partitions
// each, and records produced to each partition of each topic.
func newCluster(t *testing.T, partitions int32, records int, topics ...string) (*kfake.Cluster, string) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions, topics...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	addr := c.ListenAddrs()[0]
	if records > 0 {
		cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		for _, topic := range topics {
			for p := range partitions {
				for i := range records {
					r := &kgo.Record{Topic: topic, Partition: p, Value: []byte(strings.Repeat("v", i+1))}
					if err := cl.ProduceSync(context.Background(), r).FirstErr(); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
	return c, addr
}

// runKcl runs "kcl topic ..." against addr with the flags a test passes,
// returning stdout and the exit code the error carries, 0 for none.
func runKcl(t *testing.T, addr string, args ...string) (string, int) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	// A flag error exits 2, as main.go arranges.
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return out.Errf(out.ExitUsage, "%v", err)
	})
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs(append([]string{
		"--no-config-file", "-B", addr, "-X", "dial_timeout=2s", "-X", "retry_timeout=10s", "topic",
	}, args...))
	var execErr error
	stdout := captureStdout(t, func() { execErr = root.Execute() })
	code := 0
	if execErr != nil {
		t.Logf("kcl %s: %v", strings.Join(args, " "), execErr)
		code = out.ExitCode(execErr)
		if code == out.ExitOK {
			code = out.ExitError
		}
	}
	return stdout, code
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	fn()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	return string(b)
}

// awkRows splits awk output into rows of fields, failing on a row whose
// field count is not want.
func awkRows(t *testing.T, got string, want int) [][]string {
	t.Helper()
	var rows [][]string
	for _, line := range strings.Split(strings.TrimSuffix(got, "\n"), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != want {
			t.Errorf("awk row has %d fields, want %d: %q", len(fields), want, line)
		}
		rows = append(rows, fields)
	}
	return rows
}

// jsonDoc parses one JSON document and checks its envelope.
func jsonDoc(t *testing.T, got, command string) map[string]any {
	t.Helper()
	if strings.Count(strings.TrimSuffix(got, "\n"), "\n") != 0 {
		t.Fatalf("json is not one line:\n%s", got)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(got), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, got)
	}
	if doc["_command"] != command {
		t.Errorf("_command = %v, want %s", doc["_command"], command)
	}
	if doc["_version"] != float64(1) {
		t.Errorf("_version = %v, want 1", doc["_version"])
	}
	return doc
}

// keysOf are the sorted keys of one JSON object.
func keysOf(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// rowsOf are the objects under key in a document.
func rowsOf(t *testing.T, doc map[string]any, key string) []map[string]any {
	t.Helper()
	raw, ok := doc[key].([]any)
	if !ok {
		t.Fatalf("%s = %v (%T), want an array", key, doc[key], doc[key])
	}
	rows := make([]map[string]any, 0, len(raw))
	for _, r := range raw {
		rows = append(rows, r.(map[string]any))
	}
	return rows
}
