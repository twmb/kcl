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
	"github.com/twmb/kcl/out"
)

// runConfig runs "kcl config <args>" against addr in format and returns
// stdout and the command's error.
func runConfig(t *testing.T, addr, format string, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs(append([]string{
		"--no-config-file", "-B", addr,
		"-X", "dial_timeout=2s", "-X", "retry_timeout=10s",
		"--format", format, "config",
	}, args...))

	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	return string(b), err
}

func describeBrokerConfigs(t *testing.T, addr, format string, extra ...string) string {
	t.Helper()
	got, err := runConfig(t, addr, format, append([]string{"describe", "0", "-tb"}, extra...)...)
	if err != nil {
		t.Fatalf("describe: %v\n%s", err, got)
	}
	return got
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

	// awk rows are RESOURCE KEY TYPE VALUE SOURCE READ-ONLY whether or
	// not --with-types is given: TYPE is "-" without it.
	for _, withTypes := range []bool{false, true} {
		args := []string{}
		if withTypes {
			args = append(args, "--with-types")
		}
		awk := describeBrokerConfigs(t, addr, "awk", args...)
		for _, line := range strings.Split(strings.TrimSuffix(awk, "\n"), "\n") {
			fields := strings.Split(line, "\t")
			if len(fields) != len(describeHeaders) {
				t.Fatalf("awk row = %d fields, want %v: %q", len(fields), describeHeaders, line)
			}
			if fields[0] != "0" {
				t.Errorf("awk row does not lead with the broker described: %q", line)
			}
			if strings.HasSuffix(fields[1], "*") {
				t.Errorf("awk key carries the star: %q", fields[1])
			}
			if withTypes && fields[2] == "-" || !withTypes && fields[2] != "-" {
				t.Errorf("--with-types=%v: TYPE = %q", withTypes, fields[2])
			}
			if fields[1] == "broker.id" && fields[5] != "true" {
				t.Errorf("broker.id read-only = %q, want true", fields[5])
			}
			if fields[4] == "" {
				t.Errorf("awk row has no source: %q", line)
			}
		}
	}

	var doc struct {
		Configs []struct {
			Resource string  `json:"resource"`
			Key      string  `json:"key"`
			Type     *string `json:"type"`
			Source   string  `json:"source"`
			ReadOnly bool    `json:"read_only"`
		} `json:"configs"`
	}
	raw := describeBrokerConfigs(t, addr, "json")
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	var sawReadOnly bool
	for _, c := range doc.Configs {
		if c.Resource != "0" {
			t.Errorf("resource = %q, want the broker described", c.Resource)
		}
		if strings.HasSuffix(c.Key, "*") {
			t.Errorf("json key carries the star: %q", c.Key)
		}
		if c.Type != nil {
			t.Errorf("type = %q without --with-types, want null", *c.Type)
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
}

// TestAlterResults pins the alter result shape: error "" on success and the
// error name on failure, exit 1 after every row prints, and a dry run marked
// as one.
func TestAlterResults(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	raw, err := runConfig(t, addr, "json", "alter", "t", "-s", "retention.ms=1000")
	if err != nil {
		t.Fatalf("alter: %v\n%s", err, raw)
	}
	var doc struct {
		DryRun  bool `json:"dry_run"`
		Results []struct {
			Resource string `json:"resource"`
			Error    string `json:"error"`
			Message  string `json:"message"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if doc.DryRun || len(doc.Results) != 1 || doc.Results[0].Resource != "t" || doc.Results[0].Error != "" {
		t.Errorf("doc = %+v, want one clean row for t", doc)
	}

	raw, err = runConfig(t, addr, "json", "alter", "t", "-s", "retention.ms=1000", "--dry-run")
	if err != nil {
		t.Fatalf("alter --dry-run: %v\n%s", err, raw)
	}
	doc.DryRun = false
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if !doc.DryRun {
		t.Errorf("dry run is not marked: %s", raw)
	}

	raw, err = runConfig(t, addr, "json", "alter", "nosuch", "-s", "retention.ms=1000")
	if code := out.ExitCode(err); err == nil || code != out.ExitError {
		t.Fatalf("alter of a missing topic: err = %v (exit %d), want a silent exit 1", err, code)
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if len(doc.Results) != 1 || doc.Results[0].Error != "UNKNOWN_TOPIC_OR_PARTITION" {
		t.Errorf("doc = %+v, want UNKNOWN_TOPIC_OR_PARTITION for nosuch", doc)
	}
}

// TestAlterDeclined pins the non-incremental alter's guard: with a dynamic
// key that the alter would drop and a stdin that cannot answer, the alter
// is declined, the plan prints as a dry run, and the exit is 0.
func TestAlterDeclined(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	if raw, err := runConfig(t, addr, "json", "alter", "t", "-s", "retention.ms=1000"); err != nil {
		t.Fatalf("seeding a dynamic key: %v\n%s", err, raw)
	}

	r, w, _ := os.Pipe()
	oldStdin := os.Stdin
	os.Stdin = r
	w.Close()
	defer func() { os.Stdin = oldStdin; r.Close() }()

	raw, err := runConfig(t, addr, "json", "alter", "t", "-k", "segment.ms=5")
	if err != nil {
		t.Fatalf("declined alter: %v\n%s", err, raw)
	}
	var doc struct {
		DryRun  bool `json:"dry_run"`
		Results []struct {
			Resource string `json:"resource"`
			Error    any    `json:"error"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if !doc.DryRun || len(doc.Results) != 1 || doc.Results[0].Resource != "t" || doc.Results[0].Error != nil {
		t.Errorf("doc = %+v, want a dry run row for t with no result", doc)
	}

	// The key the alter would have dropped is still there.
	raw, err = runConfig(t, addr, "json", "describe", "t")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	var described struct {
		Configs []struct {
			Key    string `json:"key"`
			Source string `json:"source"`
		} `json:"configs"`
	}
	if err := json.Unmarshal([]byte(raw), &described); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	var kept bool
	for _, c := range described.Configs {
		kept = kept || c.Key == "retention.ms" && c.Source == "DYNAMIC_TOPIC_CONFIG"
	}
	if !kept {
		t.Errorf("the declined alter dropped retention.ms:\n%s", raw)
	}
}
