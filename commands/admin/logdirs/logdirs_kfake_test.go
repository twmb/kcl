package logdirs

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/client"
)

// describeAgainstKfake produces one record and returns what describe printed
// in format.
func describeAgainstKfake(t *testing.T, format string) []byte {
	t.Helper()

	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "logdirs-topic"))
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
	if err := pcl.ProduceSync(context.Background(), &kgo.Record{Topic: "logdirs-topic", Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs([]string{
		"--no-config-file", "-B", addr,
		"-X", "dial_timeout=2s", "-X", "retry_timeout=10s",
		"--format", format, "logdirs", "describe",
	})

	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err = root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
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

// TestDescribeVolumeColumnsText pins that the three columns come last, so an
// awk script keeps the indexes it already had.
func TestDescribeVolumeColumnsText(t *testing.T) {
	b := describeAgainstKfake(t, "text")
	header := strings.Fields(strings.SplitN(string(b), "\n", 2)[0])
	want := []string{"BROKER", "ERR", "DIR", "TOPIC", "PARTITION", "SIZE", "OFFSET-LAG", "IS-FUTURE", "TOTAL", "USABLE", "CORDONED"}
	if len(header) != len(want) {
		t.Fatalf("header = %q, want %q", header, want)
	}
	for i, h := range want {
		if header[i] != h {
			t.Errorf("column %d = %q, want %q", i, header[i], h)
		}
	}
}
