package misc

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func runMisc(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs(append([]string{"--no-config-file"}, args...))
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	execErr := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	return string(b), execErr
}

func TestRawReqNeedsKey(t *testing.T) {
	_, err := runMisc(t, "misc", "raw-req")
	var ce *out.ExitCodeError
	if err == nil || !strings.Contains(err.Error(), "--key is required") || !errors.As(err, &ce) || ce.Code != out.ExitUsage {
		t.Fatalf("err = %v, want exit 2 naming --key", err)
	}
}

func TestErrcodeAndErrtextFormats(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want string // substring of stdout
		json bool
	}{
		{name: "errcode text", args: []string{"misc", "errcode", "6"}, want: "NOT_LEADER_FOR_PARTITION\nThis server is not the leader"},
		{name: "errcode json", args: []string{"--format", "json", "misc", "errcode", "6"}, want: `"name": "NOT_LEADER_FOR_PARTITION"`, json: true},
		{name: "errcode awk", args: []string{"--format", "awk", "misc", "errcode", "6"}, want: "NOT_LEADER_FOR_PARTITION\t6\tThis server"},
		{name: "errcode none json", args: []string{"--format", "json", "misc", "errcode", "0"}, want: `"name": "NONE"`, json: true},
		{name: "errtext text", args: []string{"misc", "errtext", "NOT_LEADER_FOR_PARTITION"}, want: "NOT_LEADER_FOR_PARTITION (6)\n"},
		{name: "errtext json", args: []string{"--format", "json", "misc", "errtext", "NOT_LEADER_FOR_PARTITION"}, want: `"code": 6`, json: true},
		{name: "errtext list awk", args: []string{"--format", "awk", "misc", "errtext", "--list"}, want: "\nNOT_LEADER_FOR_PARTITION\t6\t"},
		{name: "errtext list json", args: []string{"--format", "json", "misc", "errtext", "--list"}, want: `"_command": "misc.errtext"`, json: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := runMisc(t, test.args...)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(got, test.want) {
				t.Errorf("stdout lacks %q:\n%s", test.want, got)
			}
			if test.json && !json.Valid([]byte(got)) {
				t.Errorf("not JSON:\n%s", got)
			}
		})
	}
}
