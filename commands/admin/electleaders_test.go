package admin

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

// runCluster runs "kcl cluster <args>" and returns stdout and the command's
// error. The commands sit under "cluster", where the tree puts them, because
// _command is the command path.
func runCluster(t *testing.T, addr string, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	cluster := &cobra.Command{Use: "cluster"}
	cluster.AddCommand(ElectLeadersCommand(cl), DescribeClusterCommand(cl), DescribeQuorumCommand(cl))
	root.AddCommand(cluster)
	full := []string{"--no-config-file"}
	if addr != "" {
		full = append(full, "-B", addr)
	}
	root.SetArgs(append(append(full, "cluster"), args...))
	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	return string(b), err
}

type electDoc struct {
	Command string `json:"_command"`
	DryRun  bool   `json:"dry_run"`
	Results []struct {
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Error     any    `json:"error"`
		Message   any    `json:"message"`
	} `json:"results"`
}

// TestElectLeadersDryRunJSON pins that the dry run is the real run's
// document with dry_run and no result, on stdout, sorted, and that naming
// every partition keeps it offline.
func TestElectLeadersDryRunJSON(t *testing.T) {
	raw, err := runCluster(t, "", "--format", "json", "elect-leaders", "--dry-run", "foo:1,2", "bar:0")
	if err != nil {
		t.Fatal(err)
	}
	var doc electDoc
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if doc.Command != "cluster.elect-leaders" || !doc.DryRun || len(doc.Results) != 3 || doc.Results[0].Topic != "bar" {
		t.Errorf("doc = %+v", doc)
	}
	if doc.Results[0].Error != nil || doc.Results[0].Message != nil {
		t.Errorf("a dry run has no result, got %v %v", doc.Results[0].Error, doc.Results[0].Message)
	}
}

// TestElectLeaders pins that the positional TOPIC:P reaches the broker (it
// used to error before sending anything), the result shape, exit 1 on an
// unknown partition, and the usage errors.
func TestElectLeaders(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	raw, err := runCluster(t, addr, "--format", "json", "elect-leaders", "t:1")
	if err != nil {
		t.Fatalf("elect-leaders t:1: %v\n%s", err, raw)
	}
	var doc electDoc
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if doc.DryRun || len(doc.Results) != 1 || doc.Results[0].Partition != 1 || doc.Results[0].Error != "" {
		t.Errorf("doc = %+v, want one clean row for t:1", doc)
	}

	// A bare topic is every partition, resolved from metadata.
	raw, err = runCluster(t, addr, "--format", "awk", "elect-leaders", "t")
	if err != nil {
		t.Fatalf("elect-leaders t: %v\n%s", err, raw)
	}
	if got := strings.TrimSuffix(raw, "\n"); got != "t\t0\t-\t-\nt\t1\t-\t-" {
		t.Errorf("awk = %q, want both partitions of t", got)
	}

	raw, err = runCluster(t, addr, "--format", "json", "elect-leaders", "nosuch:0")
	if code := out.ExitCode(err); err == nil || code != out.ExitError {
		t.Fatalf("nosuch:0: err = %v (exit %d), want a silent exit 1\n%s", err, code, raw)
	}
	doc = electDoc{}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if len(doc.Results) != 1 || doc.Results[0].Error != "UNKNOWN_TOPIC_OR_PARTITION" {
		t.Errorf("doc = %+v, want UNKNOWN_TOPIC_OR_PARTITION", doc)
	}

	for _, args := range [][]string{
		{"elect-leaders"},
		{"elect-leaders", "t:x"},
		{"elect-leaders", "t:0", "--all-partitions"},
	} {
		_, err := runCluster(t, addr, args...)
		if code := out.ExitCode(err); err == nil || code != out.ExitUsage {
			t.Errorf("%v: err = %v (exit %d), want exit 2", args, err, code)
		}
	}
}

// TestDescribeCluster pins the JSON keys, that rack is always present, and
// the awk shape of each section.
func TestDescribeCluster(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(2))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	raw, err := runCluster(t, addr, "--format", "json", "describe")
	if err != nil {
		t.Fatalf("describe: %v\n%s", err, raw)
	}
	var doc struct {
		ClusterID     string `json:"cluster_id"`
		ControllerID  *int32 `json:"controller_id"`
		AuthorizedOps *int32 `json:"authorized_operations"`
		Brokers       []struct {
			ID   int32   `json:"id"`
			Rack *string `json:"rack"`
		} `json:"brokers"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if doc.ClusterID == "" || doc.ControllerID == nil || len(doc.Brokers) != 2 || doc.Brokers[0].ID != 0 || doc.Brokers[1].ID != 1 {
		t.Errorf("doc = %s", raw)
	}
	if doc.AuthorizedOps != nil {
		t.Errorf("authorized_operations = %d without the flag, want null", *doc.AuthorizedOps)
	}
	for _, b := range doc.Brokers {
		if b.Rack == nil {
			t.Errorf("broker %d has no rack key", b.ID)
		}
	}

	raw, err = runCluster(t, addr, "--format", "awk", "describe")
	if err != nil {
		t.Fatal(err)
	}
	rows := strings.Split(strings.TrimSuffix(raw, "\n"), "\n")
	if len(rows) != 2 || len(strings.Split(rows[0], "\t")) != len(brokersHeaders) {
		t.Errorf("awk = %q, want two broker rows of %d fields", raw, len(brokersHeaders))
	}
	raw, err = runCluster(t, addr, "--format", "awk", "describe", "--section", "cluster")
	if err != nil {
		t.Fatal(err)
	}
	if fields := strings.Split(strings.TrimSuffix(raw, "\n"), "\t"); len(fields) != len(clusterHeaders) || fields[2] != "-" {
		t.Errorf("awk --section cluster = %q, want %d fields ending in -", raw, len(clusterHeaders))
	}
	raw, err = runCluster(t, addr, "--format", "awk", "describe", "--section", "cluster", "--include-authorized-ops")
	if err != nil {
		t.Fatal(err)
	}
	if fields := strings.Split(strings.TrimSuffix(raw, "\n"), "\t"); fields[2] == "-" {
		t.Errorf("awk --include-authorized-ops = %q, want the bitfield", raw)
	}

	text, err := runCluster(t, addr, "describe")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(text, "CLUSTER-ID") || strings.Contains(text, "CLUSTER ID:") {
		t.Errorf("text is not the KEY value summary style:\n%s", text)
	}
	if _, err := runCluster(t, addr, "describe", "--section", "bogus"); out.ExitCode(err) != out.ExitUsage {
		t.Errorf("--section bogus: err = %v, want exit 2", err)
	}
}
