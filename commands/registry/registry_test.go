package registry

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

// --- pure-function helpers ---

func TestParseReferences(t *testing.T) {
	got, err := parseReferences([]string{"com.ex.A:a-value:2", "B:b-value:1"})
	if err != nil {
		t.Fatalf("parseReferences: %v", err)
	}
	if len(got) != 2 ||
		got[0].Name != "com.ex.A" || got[0].Subject != "a-value" || got[0].Version != 2 ||
		got[1].Name != "B" || got[1].Subject != "b-value" || got[1].Version != 1 {
		t.Fatalf("parseReferences = %+v", got)
	}
	for _, bad := range []string{"noColons", "name:subject", "name:subject:notint"} {
		if _, err := parseReferences([]string{bad}); err == nil {
			t.Errorf("parseReferences(%q) expected error", bad)
		}
	}
}

func TestParseVersion(t *testing.T) {
	cases := map[string]int{"": -1, "latest": -1, "LATEST": -1, "3": 3}
	for in, want := range cases {
		got, err := parseVersion(in)
		if err != nil || got != want {
			t.Errorf("parseVersion(%q) = %d,%v want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"0", "-1", "x", "all"} {
		if _, err := parseVersion(bad); err == nil {
			t.Errorf("parseVersion(%q) expected error", bad)
		}
	}
}

func TestParseCheckVersion(t *testing.T) {
	if v, err := parseCheckVersion("all"); err != nil || v != -2 {
		t.Errorf(`parseCheckVersion("all") = %d,%v want -2`, v, err)
	}
	if v, err := parseCheckVersion("latest"); err != nil || v != -1 {
		t.Errorf(`parseCheckVersion("latest") = %d,%v want -1`, v, err)
	}
	if v, err := parseCheckVersion("5"); err != nil || v != 5 {
		t.Errorf(`parseCheckVersion("5") = %d,%v want 5`, v, err)
	}
	_, err := parseCheckVersion("x")
	if err == nil || !strings.Contains(err.Error(), "'all'") || out.ExitCode(err) != out.ExitUsage {
		t.Errorf(`parseCheckVersion("x") = %v, want a usage error naming 'all'`, err)
	}
}

func TestParseSchemaType(t *testing.T) {
	for in := range map[string]bool{"avro": true, "AVRO": true, "protobuf": true, "json": true, "": true} {
		if _, err := parseSchemaType(in); err != nil {
			t.Errorf("parseSchemaType(%q) unexpected error: %v", in, err)
		}
	}
	if _, err := parseSchemaType("yaml"); err == nil {
		t.Error("parseSchemaType(yaml) expected error")
	}
}

func TestReadSchemaMissingFile(t *testing.T) {
	_, err := readSchema("NOPE")
	if err == nil {
		t.Fatal("got nil err, want a read failure")
	}
	if code := out.ExitCode(err); code != out.ExitUsage {
		t.Errorf("got exit code %d, want %d", code, out.ExitUsage)
	}
	const want = "unable to read schema file NOPE: no such file or directory"
	if got := err.Error(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAWKText(t *testing.T) {
	for _, test := range []struct {
		name string
		in   string
		exp  string
	}{
		{"one line stays as it is", `{"type":"record"}`, `{"type":"record"}`},
		{"newlines", "a\nb\n", `a\nb\n`},
		{"tabs", "a\tb", `a\tb`},
		{"carriage return", "a\r\nb", `a\r\nb`},
		{"a backslash first", `a\nb`, `a\\nb`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := awkText(test.in); got != test.exp {
				t.Errorf("awkText(%q) = %q != exp %q", test.in, got, test.exp)
			}
		})
	}
}

// --- command-level integration tests, driven through the real cobra tree
// against an in-process srfake registry ---

// newRoot builds the kcl root with the registry tree under it, the way main
// does, so that a hidden alias resolves and the persistent pre-run records
// the command.
func newRoot() (*cobra.Command, *client.Client) {
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	return root, cl
}

// run executes "kcl registry <args...>" against the registry at url and
// returns stdout and the command's error.
func run(t *testing.T, url string, args ...string) (string, error) {
	t.Helper()
	root, _ := newRoot()
	root.SetArgs(append(append([]string{"registry"}, args...), "--no-config-file", "-R", url))

	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	runErr := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	r.Close()
	return string(b), runErr
}

// runJSON runs the command under --format json and parses the one document
// it prints. The command's error, if any, is returned alongside.
func runJSON(t *testing.T, url string, args ...string) (map[string]any, error) {
	t.Helper()
	stdout, err := run(t, url, append([]string{"--format", "json"}, args...)...)
	var m map[string]any
	if strings.TrimSpace(stdout) != "" {
		if uerr := json.Unmarshal([]byte(stdout), &m); uerr != nil {
			t.Fatalf("output is not JSON (%v): %s", uerr, stdout)
		}
		if strings.Count(strings.TrimSpace(stdout), "\n") != 0 {
			t.Errorf("JSON output spans lines: %q", stdout)
		}
	}
	return m, err
}

// rows is the array under key, each row a map.
func rows(t *testing.T, doc map[string]any, key string) []map[string]any {
	t.Helper()
	raw, ok := doc[key].([]any)
	if !ok {
		t.Fatalf("no %q array in %v", key, doc)
	}
	var rs []map[string]any
	for _, r := range raw {
		rs = append(rs, r.(map[string]any))
	}
	return rs
}

func writeSchema(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "schema.avsc")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const (
	avroSchema  = `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`
	avroSchema2 = `{"type":"record","name":"User","fields":[{"name":"id","type":"string"},{"name":"n","type":"int","default":0}]}`
)

// answer makes reg answer method path with status and body, in place of its
// own handler, until the test ends.
func answer(t *testing.T, reg *srfake.Registry, method, path string, status int, body string) {
	t.Helper()
	reg.Intercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != method || r.URL.Path != path {
			return false
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, body)
		return true
	})
	t.Cleanup(reg.ClearInterceptors)
}

// seed registers avroSchema under each subject, in order, and fails the test
// if the registry refuses one.
func seed(t *testing.T, url string, subjects ...string) {
	t.Helper()
	file := writeSchema(t, avroSchema)
	for _, s := range subjects {
		if _, err := run(t, url, "schema", "create", s, "-s", file); err != nil {
			t.Fatalf("seed %s: %v", s, err)
		}
	}
}

// TestTree pins the tree: every new leaf answers with its own path, every old
// name still resolves, is out of the help, and answers with the new path, and
// the old delete dispatches on -v.
func TestTree(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	url := reg.URL()
	seed(t, url, "t-value")
	file := writeSchema(t, avroSchema2)

	for _, test := range []struct {
		args    []string
		command string
		hidden  bool
	}{
		{[]string{"subject", "list"}, "registry.subject.list", false},
		{[]string{"subject", "delete", "nope"}, "registry.subject.delete", false},
		{[]string{"schema", "list"}, "registry.schema.list", false},
		{[]string{"schema", "list", "t-value"}, "registry.schema.list", false},
		{[]string{"schema", "get", "t-value"}, "registry.schema.get", false},
		{[]string{"schema", "create", "t-value", "-s", file}, "registry.schema.create", false},
		{[]string{"schema", "delete", "nope", "-v", "1"}, "registry.schema.delete", false},
		{[]string{"schema", "references", "t-value"}, "registry.schema.references", false},
		{[]string{"schema", "check-compatibility", "t-value", "-s", file}, "registry.schema.check-compatibility", false},
		{[]string{"compatibility", "get"}, "registry.compatibility.get", false},
		{[]string{"compatibility", "set", "FULL"}, "registry.compatibility.set", false},
		{[]string{"mode", "get"}, "registry.mode.get", false},
		{[]string{"mode", "set", "READWRITE"}, "registry.mode.set", false},
		{[]string{"context", "list"}, "registry.context.list", false},
		{[]string{"context", "delete", "nope"}, "registry.context.delete", false},

		{[]string{"subjects"}, "registry.subject.list", true},
		{[]string{"ls"}, "registry.subject.list", true},
		{[]string{"versions", "t-value"}, "registry.schema.list", true},
		{[]string{"vs", "t-value"}, "registry.schema.list", true},
		{[]string{"references", "t-value"}, "registry.schema.references", true},
		{[]string{"refs", "t-value"}, "registry.schema.references", true},
		{[]string{"delete", "nope"}, "registry.subject.delete", true},
		{[]string{"delete", "nope", "-v", "1"}, "registry.schema.delete", true},
		{[]string{"compatibility", "test", "t-value", "-s", file}, "registry.schema.check-compatibility", true},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			root, _ := newRoot()
			cmd, _, err := root.Find(append([]string{"registry"}, test.args...))
			if err != nil {
				t.Fatalf("find: %v", err)
			}
			if cmd.Hidden != test.hidden {
				t.Errorf("hidden = %v, want %v", cmd.Hidden, test.hidden)
			}
			if test.hidden && cmd.Deprecated == "" {
				t.Errorf("an old name should say what replaced it")
			}
			doc, _ := runJSON(t, url, test.args...)
			if got := doc["_command"]; got != test.command {
				t.Errorf("_command = %v, want %q in %v", got, test.command, doc)
			}
		})
	}
}

// TestSorted pins that lists come back sorted by name and then version,
// whatever order the registry answers in.
func TestSorted(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	url := reg.URL()
	seed(t, url, "c-value", "a-value", "b-value")
	if _, err := run(t, url, "schema", "create", "a-value", "-s", writeSchema(t, avroSchema2)); err != nil {
		t.Fatal(err)
	}
	answer(t, reg, http.MethodGet, "/subjects", http.StatusOK, `["c-value","a-value","b-value"]`)
	answer(t, reg, http.MethodGet, "/contexts", http.StatusOK, `["b",".","a"]`)

	for _, test := range []struct {
		name string
		args []string
		key  string
		want []string
	}{
		{"subject list", []string{"subject", "list"}, "subject", []string{"a-value", "b-value", "c-value"}},
		{"schema list", []string{"schema", "list"}, "subject", []string{"a-value", "a-value", "b-value", "c-value"}},
		{"schema list SUBJECT", []string{"schema", "list", "a-value"}, "version", []string{"1", "2"}},
		{"context list", []string{"context", "list"}, "context", []string{".", "a", "b"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			stdout, err := run(t, url, append(test.args, "--format", "awk")...)
			if err != nil {
				t.Fatalf("%v", err)
			}
			doc, _ := runJSON(t, url, test.args...)
			var got []string
			for _, r := range rows(t, doc, test.args[0]+"s") {
				got = append(got, fmt.Sprint(r[test.key]))
			}
			if !slices.Equal(got, test.want) {
				t.Errorf("%s = %v, want %v", test.key, got, test.want)
			}
			if n := strings.Count(stdout, "\n"); n != len(test.want) {
				t.Errorf("awk printed %d rows, want %d: %q", n, len(test.want), stdout)
			}
		})
	}

	// schema list sorts a subject's versions and, across subjects, by
	// subject and then version.
	doc, _ := runJSON(t, url, "schema", "list")
	var got []string
	for _, r := range rows(t, doc, "schemas") {
		got = append(got, fmt.Sprintf("%v:%v", r["subject"], r["version"]))
	}
	if want := []string{"a-value:1", "a-value:2", "b-value:1", "c-value:1"}; !slices.Equal(got, want) {
		t.Errorf("schema list = %v, want %v", got, want)
	}
}

// TestResultRows pins the per-item result shape of every mutator: ERROR then
// MESSAGE last, "" on success and the registry's error name on failure, the
// command exiting 1 when any row failed, and the keys present either way.
func TestResultRows(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	url := reg.URL()
	seed(t, url, "r-value", "gone-value")
	if _, err := run(t, url, "subject", "delete", "gone-value"); err != nil {
		t.Fatal(err)
	}
	good := writeSchema(t, avroSchema2)
	bad := writeSchema(t, "not a schema")

	for _, test := range []struct {
		name    string
		args    []string
		key     string
		columns []string
		want    map[string]any // the cells of the one row that matters
		exit    int
	}{
		{
			"schema create", []string{"schema", "create", "r-value", "-s", good}, "schemas",
			[]string{"SUBJECT", "VERSION", "ID", "ERROR", "MESSAGE"},
			map[string]any{"subject": "r-value", "version": 2.0, "error": "", "message": ""}, 0,
		},
		{
			"schema create refused", []string{"schema", "create", "r-value", "-s", bad}, "schemas",
			[]string{"SUBJECT", "VERSION", "ID", "ERROR", "MESSAGE"},
			map[string]any{"subject": "r-value", "version": nil, "id": nil, "error": "INVALID_SCHEMA"}, 1,
		},
		{
			"schema delete", []string{"schema", "delete", "r-value", "-v", "2"}, "deleted",
			[]string{"SUBJECT", "VERSION", "ERROR", "MESSAGE"},
			map[string]any{"subject": "r-value", "version": 2.0, "error": "", "message": ""}, 0,
		},
		{
			"schema delete missing version", []string{"schema", "delete", "r-value", "-v", "9"}, "deleted",
			[]string{"SUBJECT", "VERSION", "ERROR", "MESSAGE"},
			map[string]any{"subject": "r-value", "version": 9.0, "error": "VERSION_NOT_FOUND"}, 1,
		},
		{
			"schema delete missing subject latest", []string{"schema", "delete", "nope", "-v", "latest"}, "deleted",
			[]string{"SUBJECT", "VERSION", "ERROR", "MESSAGE"},
			map[string]any{"subject": "nope", "version": nil, "error": "SUBJECT_NOT_FOUND"}, 1,
		},
		{
			"subject delete", []string{"subject", "delete", "r-value"}, "deleted",
			[]string{"SUBJECT", "ERROR", "MESSAGE"},
			map[string]any{"subject": "r-value", "error": "", "message": ""}, 0,
		},
		{
			"subject delete missing", []string{"subject", "delete", "nope"}, "deleted",
			[]string{"SUBJECT", "ERROR", "MESSAGE"},
			map[string]any{"subject": "nope", "error": "SUBJECT_NOT_FOUND"}, 1,
		},
		{
			"compatibility set", []string{"compatibility", "set", "FULL", "r-value"}, "compatibility",
			[]string{"SUBJECT", "LEVEL", "ERROR", "MESSAGE"},
			map[string]any{"subject": "r-value", "level": "FULL", "error": "", "message": ""}, 0,
		},
		{
			"compatibility set global", []string{"compatibility", "set", "BACKWARD"}, "compatibility",
			[]string{"SUBJECT", "LEVEL", "ERROR", "MESSAGE"},
			map[string]any{"subject": "(global)", "level": "BACKWARD", "error": ""}, 0,
		},
		{
			"mode set", []string{"mode", "set", "READONLY", "r-value"}, "modes",
			[]string{"SUBJECT", "MODE", "ERROR", "MESSAGE"},
			map[string]any{"subject": "r-value", "mode": "READONLY", "error": "", "message": ""}, 0,
		},
		{
			"context delete", []string{"context", "delete", "empty"}, "deleted",
			[]string{"CONTEXT", "ERROR", "MESSAGE"},
			map[string]any{"context": "empty", "error": "", "message": ""}, 0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			doc, err := runJSON(t, url, test.args...)
			if code := exitCode(err); code != test.exit {
				t.Errorf("exit %d (%v), want %d", code, err, test.exit)
			}
			rs := rows(t, doc, test.key)
			if len(rs) != 1 {
				t.Fatalf("%d rows, want 1: %v", len(rs), doc)
			}
			row := rs[0]
			for _, col := range test.columns {
				key := strings.ToLower(col)
				if _, ok := row[key]; !ok {
					t.Errorf("row has no %q: %v", key, row)
				}
			}
			for key, want := range test.want {
				if got := row[key]; got != want {
					t.Errorf("%s = %#v, want %#v", key, got, want)
				}
			}
			if test.exit == 1 && row["message"] == "" {
				t.Errorf("a failed row should carry the registry's message: %v", row)
			}

			awk, _ := run(t, url, append(test.args, "--format", "awk")...)
			if n := len(strings.Split(strings.TrimSuffix(awk, "\n"), "\t")); n != len(test.columns) {
				t.Errorf("awk row has %d fields, want %d: %q", n, len(test.columns), awk)
			}
			if test.exit == 0 && !strings.HasSuffix(awk, "\t-\t-\n") {
				t.Errorf("awk success row should end in two dashes: %q", awk)
			}
		})
	}

	// A registry error with a code we do not know, and one with no JSON at
	// all, still fill ERROR and MESSAGE rather than fail the command.
	answer(t, reg, http.MethodPut, "/mode/odd", http.StatusUnprocessableEntity, `{"error_code":42299,"message":"odd"}`)
	answer(t, reg, http.MethodPut, "/mode/plain", http.StatusBadGateway, `bad gateway`)
	doc, err := runJSON(t, url, "mode", "set", "READONLY", "odd", "plain", "r-value")
	if exitCode(err) != 1 {
		t.Errorf("exit = %v, want 1", err)
	}
	rs := rows(t, doc, "modes")
	if len(rs) != 3 || rs[0]["error"] != "42299" || rs[0]["message"] != "odd" || rs[1]["error"] != "HTTP_502" || rs[1]["message"] != "bad gateway" || rs[2]["error"] != "" {
		t.Errorf("mode set rows = %v", rs)
	}
	if rs[0]["mode"] != nil {
		t.Errorf("a failed row's mode should be null, got %v", rs[0]["mode"])
	}
}

// TestNotARegistryAnswer pins that a failure that is not the registry's
// answer, a refused connection, fails the command with an error document
// rather than filling a row.
func TestNotARegistryAnswer(t *testing.T) {
	for _, args := range [][]string{
		{"subject", "delete", "x"},
		{"schema", "delete", "x", "-v", "1"},
		{"compatibility", "get"},
		{"mode", "set", "READONLY"},
		{"subject", "list"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			doc, err := runJSON(t, "http://127.0.0.1:1", args...)
			if err == nil || exitCode(err) != 1 || !strings.Contains(err.Error(), "connection refused") {
				t.Errorf("err = %v, want a refused connection exiting 1", err)
			}
			if doc != nil {
				t.Errorf("printed %v before failing", doc)
			}
		})
	}
}

// TestVersionIsANumber pins that version is a JSON number wherever it is
// known and null where it is not: latest resolves to the number checked or
// deleted, -v all has no one version, and --id has no subject or version.
func TestVersionIsANumber(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	url := reg.URL()
	seed(t, url, "v-value")
	file := writeSchema(t, avroSchema2)
	if _, err := run(t, url, "schema", "create", "v-value", "-s", file); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name string
		args []string
		want map[string]any
	}{
		{"check latest", []string{"schema", "check-compatibility", "v-value", "-s", file}, map[string]any{"subject": "v-value", "version": 2.0, "compatible": true}},
		{"check -v 1", []string{"schema", "check-compatibility", "v-value", "-s", file, "-v", "1"}, map[string]any{"version": 1.0}},
		{"check -v all", []string{"schema", "check-compatibility", "v-value", "-s", file, "-v", "all"}, map[string]any{"version": nil, "compatible": true}},
		{"get", []string{"schema", "get", "v-value"}, map[string]any{"subject": "v-value", "version": 2.0, "id": 2.0, "type": "AVRO"}},
		{"get -v 1", []string{"schema", "get", "v-value", "-v", "1"}, map[string]any{"version": 1.0, "id": 1.0}},
		{"get --id", []string{"schema", "get", "--id", "1"}, map[string]any{"subject": nil, "version": nil, "id": 1.0}},
		{"delete latest", []string{"schema", "delete", "v-value", "-v", "latest"}, map[string]any{"version": 2.0, "error": ""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			doc, err := runJSON(t, url, test.args...)
			if err != nil {
				t.Fatalf("%v", err)
			}
			row := doc
			if rs, ok := doc["deleted"]; ok {
				row = rs.([]any)[0].(map[string]any)
			}
			for key, want := range test.want {
				got, ok := row[key]
				if !ok {
					t.Errorf("no %q in %v", key, row)
				} else if got != want {
					t.Errorf("%s = %#v, want %#v", key, got, want)
				}
			}
		})
	}

	// messages is an array, empty rather than null; references likewise.
	doc, _ := runJSON(t, url, "schema", "check-compatibility", "v-value", "-s", file)
	if msgs, ok := doc["messages"].([]any); !ok || len(msgs) != 0 {
		t.Errorf("messages = %#v, want []", doc["messages"])
	}
	doc, _ = runJSON(t, url, "schema", "get", "v-value")
	if refs, ok := doc["references"].([]any); !ok || len(refs) != 0 {
		t.Errorf("references = %#v, want []", doc["references"])
	}
}

// TestCheckCompatibility pins the row and exit code when the registry says
// no, and that the reasons reach JSON and, in text and awk, stderr only.
func TestCheckCompatibility(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	url := reg.URL()
	seed(t, url, "cc-value")
	file := writeSchema(t, avroSchema2)
	answer(t, reg, http.MethodPost, "/compatibility/subjects/cc-value/versions/1", http.StatusOK, `{"is_compatible":false,"messages":["field n has no default"]}`)

	doc, err := runJSON(t, url, "schema", "check-compatibility", "cc-value", "-s", file, "--verbose")
	if exitCode(err) != 1 {
		t.Errorf("exit = %v, want 1 for an incompatible schema", err)
	}
	if doc["compatible"] != false || doc["version"] != 1.0 || fmt.Sprint(doc["messages"]) != "[field n has no default]" {
		t.Errorf("doc = %v", doc)
	}
	for _, format := range []string{"text", "awk"} {
		stdout, err := run(t, url, "schema", "check-compatibility", "cc-value", "-s", file, "--format", format)
		if exitCode(err) != 1 {
			t.Errorf("%s: exit = %v, want 1", format, err)
		}
		if strings.Contains(stdout, "no default") {
			t.Errorf("%s: the reason belongs on stderr: %q", format, stdout)
		}
		if !strings.Contains(stdout, "cc-value") || !strings.Contains(stdout, "false") {
			t.Errorf("%s: no SUBJECT VERSION COMPATIBLE row: %q", format, stdout)
		}
	}
	awk, _ := run(t, url, "schema", "check-compatibility", "cc-value", "-s", file, "--format", "awk")
	if awk != "cc-value\t1\tfalse\n" {
		t.Errorf("awk = %q", awk)
	}
}

// TestUsageErrors pins the arguments a command refuses before it dials.
func TestUsageErrors(t *testing.T) {
	file := writeSchema(t, avroSchema)
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"schema", "delete", "s"}, "needs -v"},
		{[]string{"schema", "delete", "s", "-v", "0"}, "invalid version"},
		{[]string{"schema", "check-compatibility", "s", "-s", file, "-v", "x"}, "'latest', or 'all'"},
		{[]string{"schema", "check-compatibility", "s", "-s", "NOPE"}, "unable to read schema file"},
		{[]string{"schema", "create", "s", "-s", file, "-t", "yaml"}, "valid: avro, protobuf, json"},
		{[]string{"schema", "create", "s", "-s", file, "-r", "nocolons"}, "invalid reference"},
		{[]string{"compatibility", "set", "BOGUS"}, "unknown compatibility level"},
		{[]string{"mode", "set", "BOGUS"}, "unknown mode"},
		{[]string{"schema", "get"}, "exactly one of a subject or --id"},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			_, err := runJSON(t, "http://127.0.0.1:1", test.args...)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err = %v, want containing %q", err, test.want)
			}
			if code := exitCode(err); code != out.ExitUsage {
				t.Errorf("exit = %d, want %d", code, out.ExitUsage)
			}
		})
	}
}

// TestAWKHeaders pins that every leaf with a table registered its columns,
// that the row it prints has that many fields, and that the old delete
// registers the shape -v selects.
func TestAWKHeaders(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	url := reg.URL()
	seed(t, url, "h-value")
	multiline := "{\n\t\"type\":\"record\",\n\t\"name\":\"User\",\n\t\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]\n}"
	if _, err := run(t, url, "schema", "create", "ml-value", "-s", writeSchema(t, multiline)); err != nil {
		t.Fatal(err)
	}
	file := writeSchema(t, avroSchema2)

	for _, test := range []struct {
		args   []string
		header string
	}{
		{[]string{"subject", "list"}, "SUBJECT"},
		{[]string{"subject", "delete", "h-value"}, "SUBJECT\tERROR\tMESSAGE"},
		{[]string{"schema", "list"}, "SUBJECT\tVERSION\tID\tTYPE"},
		{[]string{"schema", "get", "ml-value"}, "SUBJECT\tVERSION\tID\tTYPE\tSCHEMA"},
		{[]string{"schema", "get", "--id", "1"}, "SUBJECT\tVERSION\tID\tTYPE\tSCHEMA"},
		{[]string{"schema", "create", "h-value", "-s", file}, "SUBJECT\tVERSION\tID\tERROR\tMESSAGE"},
		{[]string{"schema", "delete", "h-value", "-v", "1"}, "SUBJECT\tVERSION\tERROR\tMESSAGE"},
		{[]string{"schema", "references", "h-value"}, "SUBJECT\tVERSION\tID"},
		{[]string{"schema", "check-compatibility", "h-value", "-s", file}, "SUBJECT\tVERSION\tCOMPATIBLE"},
		{[]string{"compatibility", "get"}, "SUBJECT\tLEVEL\tERROR"},
		{[]string{"compatibility", "set", "FULL"}, "SUBJECT\tLEVEL\tERROR\tMESSAGE"},
		{[]string{"mode", "get"}, "SUBJECT\tMODE\tERROR"},
		{[]string{"mode", "set", "READWRITE"}, "SUBJECT\tMODE\tERROR\tMESSAGE"},
		{[]string{"context", "list"}, "CONTEXT"},
		{[]string{"context", "delete", "x"}, "CONTEXT\tERROR\tMESSAGE"},
		{[]string{"subjects"}, "SUBJECT"},
		{[]string{"versions", "h-value"}, "SUBJECT\tVERSION\tID\tTYPE"},
		{[]string{"references", "h-value"}, "SUBJECT\tVERSION\tID"},
		{[]string{"delete", "h-value"}, "SUBJECT\tERROR\tMESSAGE"},
		{[]string{"delete", "h-value", "-v", "1"}, "SUBJECT\tVERSION\tERROR\tMESSAGE"},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			// The header, as --awk-header prints it once the flags are
			// parsed: the old delete's depends on -v.
			root, _ := newRoot()
			root.SetArgs(append(append([]string{"registry"}, test.args...), "--no-config-file", "-R", url, "--format", "awk"))
			cmd, _, err := root.Find(append([]string{"registry"}, test.args...))
			if err != nil {
				t.Fatal(err)
			}
			cmd.ParseFlags(test.args)
			if got := strings.TrimSuffix(out.AwkHeader(cmd), "\n"); got != test.header {
				t.Errorf("header = %q, want %q", got, test.header)
			}

			stdout, _ := run(t, url, append(test.args, "--format", "awk")...)
			lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
			if stdout == "" {
				lines = nil // references: nothing references anything here
			}
			want := strings.Count(test.header, "\t") + 1
			for _, line := range lines {
				if got := strings.Count(line, "\t") + 1; got != want {
					t.Errorf("row has %d fields, want %d: %q", got, want, line)
				}
				if strings.Contains(line, "\t\t") || strings.HasSuffix(line, "\t") {
					t.Errorf("row has an empty field: %q", line)
				}
			}
		})
	}
}

// TestSchemaGetAWK pins the one line a multi line schema prints as, and that
// --id leaves subject and version as dashes.
func TestSchemaGetAWK(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	url := reg.URL()
	multiline := "{\n\t\"type\":\"record\",\n\t\"name\":\"User\",\n\t\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]\n}"
	if _, err := run(t, url, "schema", "create", "awk-value", "-s", writeSchema(t, multiline)); err != nil {
		t.Fatal(err)
	}

	got, err := run(t, url, "schema", "get", "awk-value", "--format", "awk")
	if err != nil {
		t.Fatalf("schema get: %v", err)
	}
	if strings.Count(got, "\n") != 1 {
		t.Errorf("schema get awk spans lines: %q", got)
	}
	fields := strings.Split(strings.TrimSuffix(got, "\n"), "\t")
	if len(fields) != 5 {
		t.Fatalf("schema get awk = %d fields, want subject, version, id, type, schema: %q", len(fields), got)
	}
	if fields[0] != "awk-value" || fields[1] != "1" || fields[2] != "1" || fields[3] != "AVRO" {
		t.Errorf("schema get awk = %q", fields[:4])
	}
	if !strings.Contains(fields[4], `\n`) || strings.Contains(fields[4], "\n") {
		t.Errorf("schema get awk schema = %q, want the newlines escaped", fields[4])
	}

	got, _ = run(t, url, "schema", "get", "--id", "1", "--format", "awk")
	if !strings.HasPrefix(got, "-\t-\t1\tAVRO\t") {
		t.Errorf("schema get --id awk = %q, want dashes for subject and version", got)
	}
}

func exitCode(err error) int {
	if err == nil {
		return out.ExitOK
	}
	return out.ExitCode(err)
}
