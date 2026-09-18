package clientmetrics

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

func runClientMetrics(t *testing.T, addr, format string, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs(append([]string{"--no-config-file", "-B", addr, "--format", format, "client-metrics"}, args...))

	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	return string(b), err
}

// TestAlterDeleteResults pins the result shape of alter and delete, the
// exit on a failed row, and that describe and list see what alter wrote.
// A bad interval is the failure: kfake validates it as Kafka does.
func TestAlterDeleteResults(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	type doc struct {
		Results []struct {
			Name    string `json:"name"`
			Error   string `json:"error"`
			Message string `json:"message"`
		} `json:"results"`
	}
	run := func(args ...string) (doc, error) {
		t.Helper()
		raw, err := runClientMetrics(t, addr, "json", args...)
		var d doc
		if jerr := json.Unmarshal([]byte(raw), &d); jerr != nil {
			t.Fatalf("%v: not JSON: %v\n%s", args, jerr, raw)
		}
		return d, err
	}

	d, err := run("alter", "sub1", "-s", "interval.ms=1000")
	if err != nil || len(d.Results) != 1 || d.Results[0].Name != "sub1" || d.Results[0].Error != "" {
		t.Fatalf("alter: err = %v, doc = %+v, want one OK row for sub1", err, d)
	}
	if list, _ := runClientMetrics(t, addr, "awk", "list"); list != "sub1\n" {
		t.Errorf("list after alter = %q, want sub1", list)
	}
	if desc, _ := runClientMetrics(t, addr, "awk", "describe", "sub1"); !strings.Contains(desc, "interval.ms\t1000\t") {
		t.Errorf("describe after alter:\n%s", desc)
	}

	d, err = run("alter", "sub1", "-s", "interval.ms=1")
	if code := out.ExitCode(err); err == nil || code != out.ExitError {
		t.Fatalf("bad interval: err = %v (exit %d), want a silent exit 1", err, code)
	}
	if len(d.Results) != 1 || d.Results[0].Error != "INVALID_REQUEST" {
		t.Errorf("bad interval doc = %+v, want one INVALID_REQUEST row", d)
	}
	if awk, _ := runClientMetrics(t, addr, "awk", "alter", "sub1", "-s", "interval.ms=1"); len(strings.Split(strings.TrimSuffix(awk, "\n"), "\t")) != len(resultHeaders) {
		t.Errorf("awk row = %q, want %d fields", awk, len(resultHeaders))
	}

	d, err = run("delete", "sub1")
	if err != nil || len(d.Results) != 1 || d.Results[0].Error != "" {
		t.Fatalf("delete: err = %v, doc = %+v, want one OK row", err, d)
	}
	if list, _ := runClientMetrics(t, addr, "awk", "list"); list != "" {
		t.Errorf("list after delete = %q, want nothing", list)
	}

	if _, err := runClientMetrics(t, addr, "json", "alter", "sub1", "-s", "novalue"); out.ExitCode(err) != out.ExitUsage {
		t.Errorf("--set without a value: err = %v, want exit 2", err)
	}
}
