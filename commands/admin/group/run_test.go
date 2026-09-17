package group

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func newRoot(c *kfake.Cluster) *cobra.Command {
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	return root
}

func rootArgs(c *kfake.Cluster, args ...string) []string {
	return append([]string{"--no-config-file", "-B", c.ListenAddrs()[0], "-X", "dial_timeout=2s", "-X", "retry_timeout=10s"}, args...)
}

// runGroup runs kcl with the given arguments, "group" first, against the
// cluster, with stdin as its standard input, and returns what it wrote to
// stdout. A pipe is never a terminal, so a [y/N] prompt answers no.
func runGroup(t *testing.T, c *kfake.Cluster, stdin string, args ...string) (string, error) {
	t.Helper()
	root := newRoot(c)
	root.SetArgs(rootArgs(c, args...))

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

// awkHeader is the header row --awk-header prints for the command the
// arguments name, with its flags parsed so that --section and --by select
// the table they would at run time.
func awkHeader(t *testing.T, args ...string) []string {
	t.Helper()
	root := newRoot(nil)
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
	if err := unmarshalJSON(stdout, &doc); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout)
	}
	return doc
}
