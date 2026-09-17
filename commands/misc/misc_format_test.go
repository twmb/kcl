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
		{name: "errcode text", args: []string{"misc", "errcode", "6"}, want: "NOT_LEADER_FOR_PARTITION (6)\nThis server is not the leader"},
		{name: "errcode json", args: []string{"--format", "json", "misc", "errcode", "6"}, want: `"error_code":6,"name":"NOT_LEADER_FOR_PARTITION"`, json: true},
		{name: "errcode awk", args: []string{"--format", "awk", "misc", "errcode", "6"}, want: "NOT_LEADER_FOR_PARTITION\t6\tThis server"},
		{name: "errcode none json", args: []string{"--format", "json", "misc", "errcode", "0"}, want: `"error_code":0,"name":"NONE"`, json: true},
		{name: "errcode unknown server error", args: []string{"misc", "errcode", "--", "-1"}, want: "UNKNOWN_SERVER_ERROR (-1)\n"},
		{name: "errtext text", args: []string{"misc", "errtext", "NOT_LEADER_FOR_PARTITION"}, want: "NOT_LEADER_FOR_PARTITION (6)\n"},
		{name: "errtext normalized", args: []string{"misc", "errtext", "not-leader-for-partition"}, want: "NOT_LEADER_FOR_PARTITION (6)\n"},
		{name: "errtext json", args: []string{"--format", "json", "misc", "errtext", "NOT_LEADER_FOR_PARTITION"}, want: `"error_code":6`, json: true},
		{name: "errtext list awk", args: []string{"--format", "awk", "misc", "errtext", "--list"}, want: "\nNOT_LEADER_FOR_PARTITION\t6\t"},
		{name: "errtext list json", args: []string{"--format", "json", "misc", "errtext", "--list"}, want: `"_command":"misc.errtext"`, json: true},
		{name: "errtext list json codes", args: []string{"--format", "json", "misc", "errtext", "--list"}, want: `"error_code":6`, json: true},
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
			if test.json && strings.Contains(got, `"code":`) {
				t.Errorf("JSON uses code, the error envelope's key, for the Kafka code:\n%s", got)
			}
		})
	}
}

// TestErrorLookupMisses pins the exit codes: a code that does not parse is
// usage, and a code or name no error has is a lookup miss naming it.
func TestErrorLookupMisses(t *testing.T) {
	for _, test := range []struct {
		args []string
		code int
		want string
	}{
		{[]string{"misc", "errcode", "abc"}, out.ExitUsage, `unable to parse error code "abc"`},
		{[]string{"misc", "errcode", "9999"}, out.ExitError, "no Kafka error has code 9999"},
		{[]string{"misc", "errtext", "BOGUS"}, out.ExitError, `no Kafka error is named "BOGUS"`},
		{[]string{"misc", "errtext"}, out.ExitUsage, "missing error name"},
		{[]string{"misc", "errtext", "--list", "x"}, out.ExitUsage, "invalid extra args"},
	} {
		got, err := runMisc(t, test.args...)
		if err == nil {
			t.Errorf("%v: no error, stdout %q", test.args, got)
			continue
		}
		if code := out.ExitCode(err); code != test.code || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%v: err = %v (exit %d), want exit %d containing %q", test.args, err, code, test.code, test.want)
		}
		if got != "" {
			t.Errorf("%v: printed %q on stdout before failing", test.args, got)
		}
	}
}

// TestAllErrorsListed pins that the list is every error kerr knows: the old
// walk stopped at the first code kerr did not know, so a gap would have
// truncated it.
func TestAllErrorsListed(t *testing.T) {
	errs := allErrors()
	if len(errs) < 100 {
		t.Fatalf("%d errors listed, want kerr's full set", len(errs))
	}
	if errs[0].Code != -1 {
		t.Errorf("first error has code %d, want UNKNOWN_SERVER_ERROR at -1", errs[0].Code)
	}
	for i, e := range errs[1:] {
		if e.Code != int16(i+1) {
			t.Fatalf("error %d has code %d; the list is not every code in order", i+1, e.Code)
		}
	}
}

// TestAPIVersionsKeyColumn pins that KEY is always a column in JSON and awk,
// unknown without --with-key-nums, and that text hides it then. -v keeps the
// command offline.
func TestAPIVersionsKeyColumn(t *testing.T) {
	awk, err := runMisc(t, "--format", "awk", "misc", "api-versions", "-v", "3.5.0")
	if err != nil {
		t.Fatal(err)
	}
	rows := strings.Split(strings.TrimSuffix(awk, "\n"), "\n")
	if len(rows) < 50 {
		t.Fatalf("%d rows, want a Kafka 3.5.0 worth of requests", len(rows))
	}
	for _, row := range rows {
		fields := strings.Split(row, "\t")
		if len(fields) != len(apiVersionsHeaders) || fields[1] != "-" {
			t.Errorf("awk row = %q, want %d fields with KEY -", row, len(apiVersionsHeaders))
			break
		}
	}

	awk, err = runMisc(t, "--format", "awk", "misc", "api-versions", "-v", "3.5.0", "--with-key-nums")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(awk, "Produce\t0\t") {
		t.Errorf("awk --with-key-nums = %q, want Produce with key 0 first", strings.SplitN(awk, "\n", 2)[0])
	}

	js, err := runMisc(t, "--format", "json", "misc", "api-versions", "-v", "3.5.0")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Versions []struct {
			Name string `json:"name"`
			Key  *int16 `json:"key"`
			Max  int16  `json:"max"`
		} `json:"api_versions"`
	}
	if err := json.Unmarshal([]byte(js), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, js)
	}
	if len(doc.Versions) == 0 || doc.Versions[0].Key != nil {
		t.Errorf("JSON key without the flag = %v, want null", doc.Versions[0].Key)
	}

	text, err := runMisc(t, "misc", "api-versions", "-v", "3.5.0")
	if err != nil {
		t.Fatal(err)
	}
	if header := strings.SplitN(text, "\n", 2)[0]; strings.Contains(header, "KEY") {
		t.Errorf("text shows KEY without the flag: %q", header)
	}
}
