package registry

import (
	"errors"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

// TestSchemaGetSubjectArg pins that the subject is an argument, that -S
// still works, and that the combinations are refused.
func TestSchemaGetSubjectArg(t *testing.T) {
	for _, test := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "nothing", args: []string{}, wantErr: "exactly one of a subject or --id"},
		{name: "argument and --subject", args: []string{"s", "-S", "s"}, wantErr: "both as an argument and with --subject"},
		{name: "argument and --id", args: []string{"s", "--id", "1"}, wantErr: "exactly one of a subject or --id"},
		{name: "two arguments", args: []string{"a", "b"}, wantErr: "accepts at most 1 arg"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
			cl := client.New(root)
			root.AddCommand(Command(cl))
			root.SetArgs(append([]string{"--no-config-file", "registry", "schema", "get"}, test.args...))
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, test.wantErr)
			}
			var ce *out.ExitCodeError
			if errors.As(err, &ce) && ce.Code != out.ExitUsage {
				t.Errorf("exit code = %d, want %d", ce.Code, out.ExitUsage)
			}
		})
	}

	// The subject flag still resolves, but is out of the help.
	root := &cobra.Command{Use: "kcl"}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	get, _, err := root.Find([]string{"registry", "schema", "get"})
	if err != nil {
		t.Fatal(err)
	}
	if f := get.Flags().Lookup("subject"); f == nil || !f.Hidden {
		t.Errorf("--subject should exist and be hidden, got %v", f)
	}
	if get.Use != "get [SUBJECT]" {
		t.Errorf("Use = %q", get.Use)
	}
}
