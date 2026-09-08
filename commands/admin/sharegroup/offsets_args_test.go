package sharegroup

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
)

func TestOffsetDeleteNeedsTopics(t *testing.T) {
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs([]string{"--no-config-file", "share-group", "offset-delete", "g"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "at least one topic is required (-t)") {
		t.Fatalf("err = %v", err)
	}
}
