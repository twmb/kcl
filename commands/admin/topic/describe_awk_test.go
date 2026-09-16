package topic

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/twmb/kcl/client"
)

// TestDescribeAWKRowShape pins that awk is the partition rows: one row per
// partition, a fixed column count, and a dash where we have no value, so that
// a row never ends in a tab.
func TestDescribeAWKRowShape(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "awk-topic"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	describe := func(args ...string) string {
		t.Helper()
		root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
		cl := client.New(root)
		root.AddCommand(Command(cl))
		root.SetArgs(append([]string{
			"--no-config-file", "-B", addr,
			"-X", "dial_timeout=2s", "-X", "retry_timeout=10s",
			"--format", "awk", "topic", "describe", "awk-topic",
		}, args...))

		r, w, _ := os.Pipe()
		old := os.Stdout
		os.Stdout = w
		err := root.Execute()
		w.Close()
		os.Stdout = old
		b, _ := io.ReadAll(r)
		if err != nil {
			t.Fatalf("describe: %v\n%s", err, b)
		}
		return string(b)
	}

	// The source of a config is what kmsg.ConfigSource calls it, the same
	// name "kcl config describe" prints for the same key.
	t.Run("config sources are kmsg names", func(t *testing.T) {
		got := describe("--section", "configs")
		if got == "" {
			t.Skip("this kfake reports no topic configs")
		}
		for _, line := range strings.Split(strings.TrimSuffix(got, "\n"), "\n") {
			fields := strings.Split(line, "\t")
			if len(fields) != 5 {
				t.Fatalf("configs row = %d fields: %q", len(fields), line)
			}
			if !strings.HasSuffix(fields[3], "_CONFIG") {
				t.Errorf("source = %q, want a kmsg.ConfigSource name", fields[3])
			}
		}
	})

	for _, test := range []struct {
		name    string
		args    []string
		columns int
	}{
		{"partitions", nil, 8},
		{"partitions with --stable", []string{"--stable"}, 9},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := describe(test.args...)
			lines := strings.Split(strings.TrimSuffix(got, "\n"), "\n")
			if len(lines) != 2 {
				t.Fatalf("got %d rows, want one per partition:\n%s", len(lines), got)
			}
			for i, line := range lines {
				if strings.HasSuffix(line, "\t") {
					t.Errorf("row %d ends in a tab: %q", i, line)
				}
				if n := len(strings.Split(line, "\t")); n != test.columns {
					t.Errorf("row %d has %d columns, want %d: %q", i, n, test.columns, line)
				}
			}
		})
	}
}
