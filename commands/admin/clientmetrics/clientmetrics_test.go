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

// TestAlterDeleteResults pins the result shape of alter and delete and the
// exit on a failed row. kfake answers INVALID_REQUEST for a client metrics
// config resource, which is the failure.
func TestAlterDeleteResults(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	for _, args := range [][]string{
		{"alter", "sub1", "-s", "interval.ms=1000"},
		{"delete", "sub1"},
	} {
		raw, err := runClientMetrics(t, addr, "json", args...)
		if code := out.ExitCode(err); err == nil || code != out.ExitError {
			t.Fatalf("%v: err = %v (exit %d), want a silent exit 1\n%s", args, err, code, raw)
		}
		var doc struct {
			Results []struct {
				Name    string `json:"name"`
				Error   string `json:"error"`
				Message string `json:"message"`
			} `json:"results"`
		}
		if err := json.Unmarshal([]byte(raw), &doc); err != nil {
			t.Fatalf("%v: not JSON: %v\n%s", args, err, raw)
		}
		if len(doc.Results) != 1 || doc.Results[0].Name != "sub1" || doc.Results[0].Error != "INVALID_REQUEST" {
			t.Errorf("%v: doc = %+v, want one INVALID_REQUEST row for sub1", args, doc)
		}

		awk, _ := runClientMetrics(t, addr, "awk", args...)
		if fields := strings.Split(strings.TrimSuffix(awk, "\n"), "\t"); len(fields) != len(resultHeaders) {
			t.Errorf("%v: awk row = %q, want %d fields", args, awk, len(resultHeaders))
		}
	}

	if _, err := runClientMetrics(t, addr, "json", "alter", "sub1", "-s", "novalue"); out.ExitCode(err) != out.ExitUsage {
		t.Errorf("--set without a value: err = %v, want exit 2", err)
	}
}
