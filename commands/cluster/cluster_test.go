package cluster

import (
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
)

// TestDescribeAliasResolves pins that renaming "describe-cluster" to "describe"
// kept the old spelling working. The same constructor is also mounted under the
// hidden "admin" tree, so dropping the alias would have broken two paths.
func TestDescribeAliasResolves(t *testing.T) {
	root := &cobra.Command{Use: "kcl"}
	cl := client.New(root)
	cmd := Command(cl)

	byName, _, err := cmd.Find([]string{"describe"})
	if err != nil {
		t.Fatalf("cluster describe: %v", err)
	}
	byAlias, _, err := cmd.Find([]string{"describe-cluster"})
	if err != nil {
		t.Fatalf("cluster describe-cluster: %v", err)
	}
	if byName != byAlias {
		t.Errorf("describe and describe-cluster resolve to different commands: %q vs %q",
			byName.Name(), byAlias.Name())
	}
	if byName.Name() != "describe" {
		t.Errorf("canonical name = %q, want describe", byName.Name())
	}

	// metadata is a sibling, not the same command -- they issue different
	// RPCs and the help for each says so.
	meta, _, err := cmd.Find([]string{"metadata"})
	if err != nil {
		t.Fatalf("cluster metadata: %v", err)
	}
	if meta == byName {
		t.Error("cluster metadata and cluster describe should be distinct commands")
	}
}
