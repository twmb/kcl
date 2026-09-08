package admin

import (
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
)

// TestElectLeadersDryRunJSON pins that the dry run is a document on stdout
// under --format json, not prose on stderr.
func TestElectLeadersDryRunJSON(t *testing.T) {
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(ElectLeadersCommand(cl))
	root.SetArgs([]string{"--no-config-file", "--format", "json", "elect-leaders", "--dry-run", "foo:1,2", "bar:0"})
	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Command    string `json:"_command"`
		Partitions []struct {
			Topic     string `json:"topic"`
			Partition int32  `json:"partition"`
		} `json:"partitions"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, b)
	}
	if doc.Command != "cluster.elect-leaders" || len(doc.Partitions) != 3 || doc.Partitions[0].Topic != "bar" {
		t.Errorf("doc = %+v", doc)
	}
}
