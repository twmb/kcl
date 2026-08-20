package registry

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/twmb/kcl/client"
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

func TestVersionString(t *testing.T) {
	if versionString(-1) != "latest" || versionString(3) != "3" {
		t.Errorf("versionString: got %q,%q", versionString(-1), versionString(3))
	}
}

// --- command-level integration tests (driven through the real cobra tree
// against an in-process srfake registry) ---

// run executes "kcl registry <args...>" against the given registry URL, with
// JSON output, and returns the parsed envelope plus any command error.
func runJSON(t *testing.T, url string, args ...string) (map[string]any, error) {
	t.Helper()
	out, err := runText(t, url, append([]string{"--format", "json"}, args...)...)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if len(out) > 0 {
		if uerr := json.Unmarshal([]byte(out), &m); uerr != nil {
			t.Fatalf("output is not JSON (%v): %s", uerr, out)
		}
	}
	return m, nil
}

func runText(t *testing.T, url string, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
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

func writeSchema(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "schema.avsc")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const avroSchema = `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`

func TestRegistryCommands(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	url := reg.URL()
	schemaFile := writeSchema(t, avroSchema)

	// subjects: empty to start.
	if m, err := runJSON(t, url, "subjects"); err != nil {
		t.Fatalf("subjects: %v", err)
	} else if subs, _ := m["subjects"].([]any); len(subs) != 0 {
		t.Errorf("expected no subjects, got %v", m["subjects"])
	}

	// schema create.
	m, err := runJSON(t, url, "schema", "create", "user-value", "-s", schemaFile)
	if err != nil {
		t.Fatalf("schema create: %v", err)
	}
	if m["subject"] != "user-value" || m["id"] == nil {
		t.Fatalf("schema create envelope = %v", m)
	}

	// subjects now lists it.
	m, _ = runJSON(t, url, "subjects")
	if !containsSubject(m["subjects"], "user-value") {
		t.Errorf("subjects missing user-value: %v", m["subjects"])
	}

	// versions.
	m, _ = runJSON(t, url, "versions", "user-value")
	if vs, _ := m["versions"].([]any); len(vs) != 1 {
		t.Errorf("expected 1 version, got %v", m["versions"])
	}

	// schema get by subject.
	m, _ = runJSON(t, url, "schema", "get", "-S", "user-value")
	if m["schema"] == nil || m["type"] != "AVRO" {
		t.Errorf("schema get = %v", m)
	}

	// schema list.
	m, _ = runJSON(t, url, "schema", "list")
	if schemas, _ := m["schemas"].([]any); len(schemas) != 1 {
		t.Errorf("schema list = %v", m["schemas"])
	}

	// compatibility set + get.
	if _, err := runJSON(t, url, "compatibility", "set", "FULL", "user-value"); err != nil {
		t.Fatalf("compat set: %v", err)
	}
	m, _ = runJSON(t, url, "compatibility", "get", "user-value")
	if !rowHas(m["compatibility"], "level", "FULL") {
		t.Errorf("compat get = %v", m["compatibility"])
	}

	// compatibility test (srfake is permissive -> compatible).
	m, err = runJSON(t, url, "compatibility", "test", "user-value", "-s", schemaFile)
	if err != nil {
		t.Fatalf("compat test: %v", err)
	}
	if m["compatible"] != true {
		t.Errorf("compat test = %v", m)
	}

	// mode set + get (needs the merged srfake mode support).
	if _, err := runJSON(t, url, "mode", "set", "READONLY", "user-value"); err != nil {
		t.Fatalf("mode set: %v", err)
	}
	m, _ = runJSON(t, url, "mode", "get", "user-value")
	if !rowHas(m["modes"], "mode", "READONLY") {
		t.Errorf("mode get = %v", m["modes"])
	}

	// references (none).
	m, _ = runJSON(t, url, "references", "user-value")
	if refs, _ := m["references"].([]any); len(refs) != 0 {
		t.Errorf("expected no references, got %v", m["references"])
	}

	// context list (default context).
	if _, err := runJSON(t, url, "context", "list"); err != nil {
		t.Fatalf("context list: %v", err)
	}

	// delete the subject.
	if _, err := runJSON(t, url, "delete", "user-value"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	m, _ = runJSON(t, url, "subjects")
	if containsSubject(m["subjects"], "user-value") {
		t.Errorf("user-value still listed after delete: %v", m["subjects"])
	}
}

func TestRegistryNotConfiguredStillDefaults(t *testing.T) {
	// With no registry reachable, a command should fail (connection refused),
	// not panic — exercising the default/build path without a live server.
	if _, err := runText(t, "http://127.0.0.1:1", "subjects"); err == nil {
		t.Error("expected an error against an unreachable registry")
	}
}

func containsSubject(v any, want string) bool {
	rows, _ := v.([]any)
	for _, r := range rows {
		if m, ok := r.(map[string]any); ok && m["subject"] == want {
			return true
		}
	}
	return false
}

func rowHas(v any, key, want string) bool {
	rows, _ := v.([]any)
	for _, r := range rows {
		if m, ok := r.(map[string]any); ok && m[key] == want {
			return true
		}
	}
	return false
}
