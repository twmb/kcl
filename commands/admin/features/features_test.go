package features

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

func runFeatures(t *testing.T, addr, format string, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	cluster := &cobra.Command{Use: "cluster"}
	cluster.AddCommand(Command(cl))
	root.AddCommand(cluster)
	root.SetArgs(append([]string{"--no-config-file", "-B", addr, "--format", format, "cluster", "features"}, args...))

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
	Command string `json:"_command"`
	DryRun  bool   `json:"dry_run"`
	Results []struct {
		Feature string `json:"feature"`
		Error   string `json:"error"`
		Message string `json:"message"`
	} `json:"results"`
}

// TestUpdate pins the update against kfake, which answers UpdateFeatures v2:
// a good update prints one OK row per feature, a dry run is marked and
// changes nothing, and a bad feature is a request-wide error, exit 1.
func TestUpdate(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	raw, err := runFeatures(t, addr, "json", "update", "transaction.version=1", "--upgrade-type", "safe-downgrade", "--dry-run")
	if err != nil {
		t.Fatalf("dry run: %v\n%s", err, raw)
	}
	var doc resultsDoc
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if doc.Command != "cluster.features.update" || !doc.DryRun || len(doc.Results) != 1 || doc.Results[0].Feature != "transaction.version" || doc.Results[0].Error != "" {
		t.Errorf("dry run doc = %+v", doc)
	}
	if desc, _ := runFeatures(t, addr, "awk", "describe"); !strings.Contains(desc, "FINALIZED\ttransaction.version\t0\t2\n") {
		t.Errorf("the dry run changed the finalized level:\n%s", desc)
	}

	raw, err = runFeatures(t, addr, "awk", "update", "transaction.version=1", "--upgrade-type", "safe-downgrade")
	if err != nil {
		t.Fatalf("update: %v\n%s", err, raw)
	}
	if got := strings.TrimSuffix(raw, "\n"); got != "transaction.version\t-\t-" {
		t.Errorf("awk row = %q", got)
	}
	if desc, _ := runFeatures(t, addr, "awk", "describe"); !strings.Contains(desc, "FINALIZED\ttransaction.version\t0\t1\n") {
		t.Errorf("the update did not change the finalized level:\n%s", desc)
	}

	_, err = runFeatures(t, addr, "json", "update", "bogus.version=1")
	if code := out.ExitCode(err); err == nil || code != out.ExitError || !strings.Contains(err.Error(), "FEATURE_UPDATE_FAILED") {
		t.Errorf("bogus feature: err = %v (exit %d), want FEATURE_UPDATE_FAILED exit 1", err, code)
	}

	for _, args := range [][]string{
		{"update", "noequals"},
		{"update", "f=notanumber"},
		{"update", "f=1", "--upgrade-type", "sideways"},
	} {
		_, err := runFeatures(t, addr, "json", args...)
		if code := out.ExitCode(err); err == nil || code != out.ExitUsage {
			t.Errorf("%v: err = %v (exit %d), want exit 2", args, err, code)
		}
	}
}

// TestResultRowsV1 pins the per-feature rows a v0 or v1 response carries,
// which kfake cannot answer since it speaks v2.
func TestResultRowsV1(t *testing.T) {
	req := &kmsg.UpdateFeaturesRequest{FeatureUpdates: []kmsg.UpdateFeaturesRequestFeatureUpdate{{Feature: "a"}, {Feature: "b"}}}
	msg := "downgrade not allowed"
	resp := &kmsg.UpdateFeaturesResponse{Version: 1, Results: []kmsg.UpdateFeaturesResponseResult{
		{Feature: "a"},
		{Feature: "b", ErrorCode: kerr.InvalidUpdateVersion.Code, ErrorMessage: &msg},
	}}
	rows := resultRows(req, resp)
	if len(rows) != 2 || rows[0][1] != "" || rows[1][1] != "INVALID_UPDATE_VERSION" || rows[1][2] != msg {
		t.Errorf("rows = %v", rows)
	}

	resp.Version = 2
	resp.Results = nil
	rows = resultRows(req, resp)
	if len(rows) != 2 || rows[0][0] != "a" || rows[1][0] != "b" || rows[0][1] != "" {
		t.Errorf("v2 rows = %v, want one OK row per requested feature", rows)
	}
}
