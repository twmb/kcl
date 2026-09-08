package logdirs

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
)

func TestAlterRequiresArguments(t *testing.T) {
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs([]string{"--no-config-file", "logdirs", "alter"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "requires at least 1 arg") {
		t.Fatalf("err = %v, want an argument count error", err)
	}
}
