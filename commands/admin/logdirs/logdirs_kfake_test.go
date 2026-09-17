package logdirs

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

// seededCluster is a one broker cluster holding one record in
// logdirs-topic.
func seededCluster(t *testing.T) (*kfake.Cluster, string) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "logdirs-topic"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	addr := c.ListenAddrs()[0]

	pcl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer pcl.Close()
	if err := pcl.ProduceSync(context.Background(), &kgo.Record{Topic: "logdirs-topic", Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return c, addr
}

// runLogdirs runs "kcl logdirs <args>" against addr in format and returns
// stdout and the command's error.
func runLogdirs(t *testing.T, addr, format string, args ...string) ([]byte, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs(append([]string{
		"--no-config-file", "-B", addr,
		"-X", "dial_timeout=2s", "-X", "retry_timeout=10s",
		"--format", format, "logdirs",
	}, args...))

	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	return b, err
}

// describeAgainstKfake produces one record and returns what describe printed
// in format.
func describeAgainstKfake(t *testing.T, format string, args ...string) []byte {
	t.Helper()
	_, addr := seededCluster(t)
	b, err := runLogdirs(t, addr, format, append([]string{"describe"}, args...)...)
	if err != nil {
		t.Fatalf("describe: %v\n%s", err, b)
	}
	return b
}

// TestDescribeVolumeColumns pins the volume columns against kfake, which
// answers DescribeLogDirs v5. It reports the bytes it is holding as the
// volume total, a fixed 32GiB usable, and cordons nothing.
func TestDescribeVolumeColumns(t *testing.T) {
	b := describeAgainstKfake(t, "json")

	var doc struct {
		Command string `json:"_command"`
		Dirs    []struct {
			Topic    string `json:"topic"`
			Size     *int64 `json:"size"`
			Total    *int64 `json:"total"`
			Usable   *int64 `json:"usable"`
			Cordoned bool   `json:"cordoned"`
		} `json:"dirs"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, b)
	}
	if doc.Command != "logdirs.describe" {
		t.Errorf("_command = %q", doc.Command)
	}
	if len(doc.Dirs) == 0 {
		t.Fatalf("no dirs described\n%s", b)
	}
	for _, d := range doc.Dirs {
		if d.Topic != "logdirs-topic" {
			continue
		}
		if d.Total == nil || d.Size == nil || d.Usable == nil {
			t.Fatalf("a volume column is null, want numbers\n%s", b)
		}
		if *d.Total <= 0 {
			t.Errorf("total = %d, want a size", *d.Total)
		}
		if *d.Total != *d.Size {
			t.Errorf("total = %d, size = %d; kfake counts only what it holds", *d.Total, *d.Size)
		}
		if *d.Usable != 34359738368 {
			t.Errorf("usable = %d, want kfake's 32GiB", *d.Usable)
		}
		if d.Cordoned {
			t.Error("kfake cordons nothing")
		}
	}
}

// TestDescribeColumns pins the row: ERROR last and "-" on a row that did not
// error, and the same field count on every awk row as the header has.
func TestDescribeColumns(t *testing.T) {
	b := describeAgainstKfake(t, "text")
	header := strings.Fields(strings.SplitN(string(b), "\n", 2)[0])
	if !slices.Equal(header, describeHeaders) {
		t.Fatalf("header = %q, want %q", header, describeHeaders)
	}

	awk := describeAgainstKfake(t, "awk")
	for _, line := range strings.Split(strings.TrimSuffix(string(awk), "\n"), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != len(describeHeaders) {
			t.Errorf("awk row has %d fields, want %d: %q", len(fields), len(describeHeaders), line)
			continue
		}
		if fields[len(fields)-1] != "-" {
			t.Errorf("awk ERROR = %q on a clean row, want -", fields[len(fields)-1])
		}
	}

	// -H changes text only: JSON keeps the bytes.
	var doc struct {
		Dirs []struct {
			Size  *int64 `json:"size"`
			Error string `json:"error"`
		} `json:"dirs"`
	}
	b = describeAgainstKfake(t, "json", "-H")
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("-H JSON is not numbers: %v\n%s", err, b)
	}
	if len(doc.Dirs) == 0 || doc.Dirs[0].Size == nil || doc.Dirs[0].Error != "" {
		t.Errorf("-H JSON = %s, want sizes as numbers and error \"\"", b)
	}
	if text := describeAgainstKfake(t, "text", "-H"); !strings.Contains(string(text), "B ") {
		t.Errorf("-H text does not print human sizes:\n%s", text)
	}
}

// TestDescribeAggregate pins the aggregate table's shape per dimension, and
// that a bad dimension is a usage error before anything is asked.
func TestDescribeAggregate(t *testing.T) {
	_, addr := seededCluster(t)
	for _, into := range []string{"broker", "dir", "topic"} {
		b, err := runLogdirs(t, addr, "awk", "describe", "--aggregate-into", into)
		if err != nil {
			t.Fatalf("--aggregate-into %s: %v", into, err)
		}
		rows := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
		if len(rows) != 1 || len(strings.Split(rows[0], "\t")) != 2 {
			t.Errorf("--aggregate-into %s awk = %q, want one row of two fields", into, b)
		}
	}
	_, err := runLogdirs(t, "localhost:1", "awk", "describe", "--aggregate-into", "bogus")
	if code := out.ExitCode(err); err == nil || code != out.ExitUsage {
		t.Errorf("--aggregate-into bogus: err = %v (exit %d), want exit 2", err, code)
	}
}

// TestAlterResults pins the alter result shape: one row per partition with
// ERROR and MESSAGE, "" on success, and a parse error exiting 2.
func TestAlterResults(t *testing.T) {
	_, addr := seededCluster(t)
	b, err := runLogdirs(t, addr, "json", "alter", "logdirs-topic:0=/tmp/elsewhere")
	if err != nil {
		t.Fatalf("alter: %v\n%s", err, b)
	}
	var doc struct {
		Results []struct {
			Topic     string `json:"topic"`
			Partition int32  `json:"partition"`
			Error     string `json:"error"`
			Message   string `json:"message"`
		} `json:"results"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, b)
	}
	if len(doc.Results) != 1 || doc.Results[0].Topic != "logdirs-topic" || doc.Results[0].Error != "" {
		t.Errorf("doc = %+v, want one clean row", doc)
	}

	b, err = runLogdirs(t, addr, "awk", "alter", "logdirs-topic:0=/tmp/elsewhere")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSuffix(string(b), "\n"); got != "logdirs-topic\t0\t-\t-" {
		t.Errorf("awk row = %q", got)
	}

	for _, arg := range []string{"logdirs-topic:0", "logdirs-topic:x=/dir", "a=b=c"} {
		_, err := runLogdirs(t, "localhost:1", "text", "alter", arg)
		if code := out.ExitCode(err); err == nil || code != out.ExitUsage {
			t.Errorf("alter %q: err = %v (exit %d), want exit 2", arg, err, code)
		}
	}
}
