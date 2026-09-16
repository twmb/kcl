package configs

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/twmb/kcl/client"
)

func describeBrokerConfigs(t *testing.T, addr, format string, extra ...string) string {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs(append([]string{
		"--no-config-file", "-B", addr,
		"-X", "dial_timeout=2s", "-X", "retry_timeout=10s",
		"--format", format, "config", "describe", "0", "-tb",
	}, extra...))

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

// TestDescribeReadOnlyColumn pins that the star marking a read only key stays
// in text, and that JSON and awk answer with the key itself and a read_only
// of their own. broker.id is read only on every broker.
func TestDescribeReadOnlyColumn(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	text := describeBrokerConfigs(t, addr, "text")
	if !strings.Contains(text, "broker.id*") {
		t.Errorf("text lost the read only star:\n%s", text)
	}
	if strings.Contains(strings.SplitN(text, "\n", 2)[0], "READ-ONLY") {
		t.Errorf("text grew a READ-ONLY column:\n%s", text)
	}

	awk := describeBrokerConfigs(t, addr, "awk")
	for _, line := range strings.Split(strings.TrimSuffix(awk, "\n"), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 4 {
			t.Fatalf("awk row = %d fields, want key, value, source, read-only: %q", len(fields), line)
		}
		if strings.HasSuffix(fields[0], "*") {
			t.Errorf("awk key carries the star: %q", fields[0])
		}
		if fields[0] == "broker.id" && fields[3] != "true" {
			t.Errorf("broker.id read-only = %q, want true", fields[3])
		}
		if fields[2] == "" {
			t.Errorf("awk row has no source: %q", line)
		}
	}

	var doc struct {
		Configs []struct {
			Key      string `json:"key"`
			Source   string `json:"source"`
			ReadOnly bool   `json:"read_only"`
		} `json:"configs"`
	}
	raw := describeBrokerConfigs(t, addr, "json")
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	var sawReadOnly bool
	for _, c := range doc.Configs {
		if strings.HasSuffix(c.Key, "*") {
			t.Errorf("json key carries the star: %q", c.Key)
		}
		if !strings.HasSuffix(c.Source, "_CONFIG") {
			t.Errorf("source = %q, want a kmsg.ConfigSource name", c.Source)
		}
		if c.Key == "broker.id" {
			sawReadOnly = c.ReadOnly
		}
	}
	if !sawReadOnly {
		t.Errorf("broker.id read_only is not true:\n%s", raw)
	}

	// --with-types slots TYPE second and keeps READ-ONLY last.
	awk = describeBrokerConfigs(t, addr, "awk", "--with-types")
	for _, line := range strings.Split(strings.TrimSuffix(awk, "\n"), "\n") {
		if n := len(strings.Split(line, "\t")); n != 5 {
			t.Fatalf("awk --with-types row = %d fields, want 5: %q", n, line)
		}
	}
}
