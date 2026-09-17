package metadata

import (
	"encoding/json"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func run(t *testing.T, addr string, args ...string) (string, int) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	cluster := &cobra.Command{Use: "cluster"}
	cluster.AddCommand(Command(cl))
	root.AddCommand(cluster)
	root.SetArgs(append([]string{
		"--no-config-file", "-B", addr, "-X", "dial_timeout=2s", "-X", "retry_timeout=10s", "cluster", "metadata",
	}, args...))
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	execErr := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	code := 0
	if execErr != nil {
		t.Logf("kcl cluster metadata %s: %v", strings.Join(args, " "), execErr)
		code = out.ExitCode(execErr)
	}
	return string(b), code
}

func awkRows(t *testing.T, got string, want int) [][]string {
	t.Helper()
	var rows [][]string
	for _, line := range strings.Split(strings.TrimSuffix(got, "\n"), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != want {
			t.Errorf("awk row has %d fields, want %d: %q", len(fields), want, line)
		}
		rows = append(rows, fields)
	}
	return rows
}

func jsonDoc(t *testing.T, got, command string) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(got), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, got)
	}
	if doc["_command"] != command {
		t.Errorf("_command = %v, want %s", doc["_command"], command)
	}
	return doc
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// TestMetadata pins that --section picks the document's keys and the awk
// table, that an errored topic stays in the table and exits 1, and that
// --detailed is topic describe.
func TestMetadata(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(2), kfake.SeedTopics(2, "m-a", "m-b"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	for _, test := range []struct {
		name string
		args []string
		keys []string // top level keys besides _command and _version
		awk  int      // awk field count
		rows int      // awk rows
		code int
	}{
		{name: "all", args: nil, keys: []string{"brokers", "cluster_id", "controller", "topics"}, awk: 5, rows: 2},
		{name: "cluster", args: []string{"--section", "cluster"}, keys: []string{"cluster_id", "controller"}, awk: 2, rows: 1},
		{name: "brokers", args: []string{"--section", "brokers"}, keys: []string{"brokers"}, awk: 4, rows: 2},
		{name: "topics", args: []string{"--section", "topics"}, keys: []string{"topics"}, awk: 5, rows: 2},
		{name: "named", args: []string{"m-b"}, keys: []string{"brokers", "cluster_id", "controller", "topics"}, awk: 5, rows: 1},
		{name: "missing topic", args: []string{"m-a", "nosuch"}, keys: []string{"brokers", "cluster_id", "controller", "topics"}, awk: 5, rows: 2, code: 1},
		{name: "bad section", args: []string{"--section", "nope"}, code: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, code := run(t, addr, append(slices.Clone(test.args), "--format", "json")...)
			if code != test.code {
				t.Fatalf("json: exit %d, want %d\n%s", code, test.code, got)
			}
			if test.code == 2 {
				return
			}
			doc := jsonDoc(t, got, "cluster.metadata")
			delete(doc, "_command")
			delete(doc, "_version")
			if !slices.Equal(sortedKeys(doc), test.keys) {
				t.Errorf("json keys = %v, want %v", sortedKeys(doc), test.keys)
			}
			if brokers, ok := doc["brokers"].([]any); ok {
				b := brokers[0].(map[string]any)
				if !slices.Equal(sortedKeys(b), []string{"host", "id", "port", "rack"}) {
					t.Errorf("broker keys = %v", sortedKeys(b))
				}
			}
			if topics, ok := doc["topics"].([]any); ok {
				for _, tp := range topics {
					row := tp.(map[string]any)
					if !slices.Equal(sortedKeys(row), []string{"error", "internal", "partition_count", "replication_factor", "topic", "topic_id"}) {
						t.Errorf("topic keys = %v", sortedKeys(row))
					}
					if row["topic"] == "nosuch" && (row["error"] == "" || row["partition_count"] != nil) {
						t.Errorf("nosuch row = %v", row)
					}
				}
			}

			got, code = run(t, addr, append(slices.Clone(test.args), "--format", "awk")...)
			if code != test.code {
				t.Fatalf("awk: exit %d, want %d\n%s", code, test.code, got)
			}
			if rows := awkRows(t, got, test.awk); len(rows) != test.rows {
				t.Errorf("awk rows = %d, want %d:\n%s", len(rows), test.rows, got)
			}
		})
	}

	// Text: the summary table, the controller starred, and no banners.
	got, code := run(t, addr)
	if code != 0 || !strings.Contains(got, "CLUSTER-ID") || !strings.Contains(got, "CONTROLLER") || !strings.Contains(got, "*") || strings.Contains(got, "====") {
		t.Errorf("text: exit %d\n%s", code, got)
	}

	got, code = run(t, addr, "--detailed", "--format", "json")
	if code != 0 {
		t.Fatalf("--detailed: exit %d\n%s", code, got)
	}
	jsonDoc(t, got, "topic.describe")
}
