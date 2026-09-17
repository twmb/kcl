package clientquotas

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func runQuota(t *testing.T, addr, format string, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs(append([]string{"--no-config-file", "-B", addr, "--format", format, "quota"}, args...))

	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	return string(b), err
}

type resultsDoc struct {
	DryRun  bool `json:"dry_run"`
	Results []struct {
		Entity  string `json:"entity"`
		Error   string `json:"error"`
		Message string `json:"message"`
	} `json:"results"`
}

func decode(t *testing.T, raw string, v any) {
	t.Helper()
	if err := json.Unmarshal([]byte(raw), v); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
}

// TestAlterAndDescribe pins the alter result shape, that a dry run is marked
// and changes nothing, that a faulted entity exits 1 after its row prints,
// and that describe rows are sorted by entity.
func TestAlterAndDescribe(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	raw, err := runQuota(t, addr, "json", "alter", "--name", "user=zed", "--add", "producer_byte_rate=1024", "--dry-run")
	if err != nil {
		t.Fatalf("dry run: %v\n%s", err, raw)
	}
	var doc resultsDoc
	decode(t, raw, &doc)
	if !doc.DryRun || len(doc.Results) != 1 || doc.Results[0].Error != "" {
		t.Errorf("dry run doc = %+v, want dry_run and one clean row", doc)
	}
	raw, err = runQuota(t, addr, "json", "describe")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(raw, `"quotas":[]`) {
		t.Errorf("the dry run applied a quota:\n%s", raw)
	}

	for _, name := range []string{"user=zed", "user=alice"} {
		raw, err = runQuota(t, addr, "json", "alter", "--name", name, "--add", "producer_byte_rate=1024")
		if err != nil {
			t.Fatalf("alter %s: %v\n%s", name, err, raw)
		}
		doc = resultsDoc{}
		decode(t, raw, &doc)
		if doc.DryRun || len(doc.Results) != 1 || doc.Results[0].Error != "" || doc.Results[0].Entity != "{"+name+"}" {
			t.Errorf("alter %s doc = %+v", name, doc)
		}
	}

	raw, err = runQuota(t, addr, "json", "describe")
	if err != nil {
		t.Fatal(err)
	}
	var described struct {
		Quotas []struct {
			Entity string  `json:"entity"`
			Key    string  `json:"key"`
			Value  float64 `json:"value"`
		} `json:"quotas"`
	}
	decode(t, raw, &described)
	if len(described.Quotas) != 2 || described.Quotas[0].Entity != "{user=alice}" || described.Quotas[1].Entity != "{user=zed}" {
		t.Errorf("describe = %+v, want alice before zed", described.Quotas)
	}
	if described.Quotas[0].Value != 1024 {
		t.Errorf("value = %v, want the number 1024", described.Quotas[0].Value)
	}

	c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.AlterClientQuotas}, Resource: "zed", Err: kerr.InvalidRequest})
	raw, err = runQuota(t, addr, "json", "alter", "--name", "user=zed", "--delete", "producer_byte_rate")
	if code := out.ExitCode(err); err == nil || code != out.ExitError {
		t.Fatalf("faulted alter: err = %v (exit %d), want a silent exit 1", err, code)
	}
	doc = resultsDoc{}
	decode(t, raw, &doc)
	if len(doc.Results) != 1 || doc.Results[0].Error != "INVALID_REQUEST" {
		t.Errorf("faulted alter doc = %+v, want INVALID_REQUEST", doc)
	}

	awk, err := runQuota(t, addr, "awk", "alter", "--name", "user=zed", "--delete", "producer_byte_rate")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSuffix(awk, "\n"); got != "{user=zed}\t-\t-" {
		t.Errorf("awk row = %q, want the entity and two dashes", got)
	}
}

func TestAlterUsage(t *testing.T) {
	for _, args := range [][]string{
		{"alter"},
		{"alter", "--name", "user=a"},
		{"alter", "--name", "user=a", "--add", "x"},
		{"alter", "--name", "user=a", "--add", "x=notanumber"},
		{"alter", "--name", "bogus=a", "--add", "x=1"},
		{"describe", "--default", "bogus"},
	} {
		_, err := runQuota(t, "localhost:1", "text", args...)
		if code := out.ExitCode(err); err == nil || code != out.ExitUsage {
			t.Errorf("%v: err = %v (exit %d), want exit 2", args, err, code)
		}
	}
}
