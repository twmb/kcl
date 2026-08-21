package acl

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/twmb/kcl/client"
)

// run executes "kcl acl <args...>" against an in-process kfake cluster,
// returning stdout and any command error. kfake validates ACL filters the way
// a real broker does (rejecting UNKNOWN elements with INVALID_REQUEST), so
// these tests catch malformed filters rather than merely exercising the code.
func run(t *testing.T, addrs []string, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs(append(append([]string{"acl"}, args...),
		"--no-config-file", "-B", strings.Join(addrs, ",")))

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

func runJSON(t *testing.T, addrs []string, args ...string) (map[string]any, error) {
	t.Helper()
	out, err := run(t, addrs, append([]string{"--format", "json"}, args...)...)
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

func newCluster(t *testing.T) []string {
	t.Helper()
	c, err := kfake.NewCluster()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

// aclRows pulls the "acls" array out of a JSON list envelope.
func aclRows(t *testing.T, m map[string]any) []map[string]any {
	t.Helper()
	raw, _ := m["acls"].([]any)
	rows := make([]map[string]any, 0, len(raw))
	for _, r := range raw {
		row, ok := r.(map[string]any)
		if !ok {
			t.Fatalf("acl row is not an object: %v", r)
		}
		rows = append(rows, row)
	}
	return rows
}

// TestACLListDefaultFilter is the regression test for #56: a bare "kcl acl
// list" left --type empty, which mapped to the resource type UNKNOWN. UNKNOWN
// is not a valid DescribeACLs filter value, so brokers reject the request while
// parsing it -- in the reported case by closing the connection outright.
func TestACLListDefaultFilter(t *testing.T) {
	addrs := newCluster(t)

	m, err := runJSON(t, addrs, "list")
	if err != nil {
		t.Fatalf("bare acl list: %v", err)
	}
	if rows := aclRows(t, m); len(rows) != 0 {
		t.Errorf("expected no acls on a fresh cluster, got %v", rows)
	}
}

// TestACLCreateAndList round trips a created ACL back through the default list
// filter, then through the ergonomic --topic filter.
func TestACLCreateAndList(t *testing.T) {
	addrs := newCluster(t)

	if _, err := run(t, addrs, "create",
		"--topic", "foo",
		"--allow-principal", "User:alice",
		"--operation", "read",
	); err != nil {
		t.Fatalf("acl create: %v", err)
	}

	// The bare (default) filter must find it.
	m, err := runJSON(t, addrs, "list")
	if err != nil {
		t.Fatalf("acl list: %v", err)
	}
	rows := aclRows(t, m)
	if len(rows) != 1 {
		t.Fatalf("expected 1 acl, got %d: %v", len(rows), rows)
	}
	row := rows[0]
	for k, want := range map[string]string{
		"type":       "TOPIC",
		"name":       "foo",
		"pattern":    "LITERAL",
		"principal":  "User:alice",
		"host":       "*",
		"operation":  "READ",
		"permission": "ALLOW",
	} {
		if got, _ := row[k].(string); got != want {
			t.Errorf("acl %s = %q, want %q", k, got, want)
		}
	}

	// The resource-specific shortcut filter must find it too...
	m, err = runJSON(t, addrs, "list", "--topic", "foo")
	if err != nil {
		t.Fatalf("acl list --topic foo: %v", err)
	}
	if rows := aclRows(t, m); len(rows) != 1 {
		t.Errorf("expected 1 acl for topic foo, got %d: %v", len(rows), rows)
	}

	// ...and must not match a different topic.
	m, err = runJSON(t, addrs, "list", "--topic", "bar")
	if err != nil {
		t.Fatalf("acl list --topic bar: %v", err)
	}
	if rows := aclRows(t, m); len(rows) != 0 {
		t.Errorf("expected no acls for topic bar, got %v", rows)
	}
}

// TestACLListGroupFilter checks the group shortcut, which selects a different
// resource type than the default.
func TestACLListGroupFilter(t *testing.T) {
	addrs := newCluster(t)

	if _, err := run(t, addrs, "create",
		"--group", "g1",
		"--allow-principal", "User:bob",
		"--operation", "read",
	); err != nil {
		t.Fatalf("acl create: %v", err)
	}

	m, err := runJSON(t, addrs, "list", "--group", "g1")
	if err != nil {
		t.Fatalf("acl list --group g1: %v", err)
	}
	rows := aclRows(t, m)
	if len(rows) != 1 {
		t.Fatalf("expected 1 acl, got %d: %v", len(rows), rows)
	}
	if got, _ := rows[0]["type"].(string); got != "GROUP" {
		t.Errorf("acl type = %q, want GROUP", got)
	}

	// A topic filter must not see the group ACL.
	m, err = runJSON(t, addrs, "list", "--topic", "g1")
	if err != nil {
		t.Fatalf("acl list --topic g1: %v", err)
	}
	if rows := aclRows(t, m); len(rows) != 0 {
		t.Errorf("expected no topic acls, got %v", rows)
	}
}

// TestACLDeleteDefaultsToMatchAll pins the model kcl now shares with rpk and
// kafka-acls.sh: every unspecified filter matches everything, and the guard is
// the confirmation rather than a required-flag error. A bare delete used to be
// rejected outright.
func TestACLDeleteDefaultsToMatchAll(t *testing.T) {
	addrs := newCluster(t)
	seedACLs(t, addrs)

	// --dry-run with no filters at all matches both ACLs and deletes nothing.
	out, err := run(t, addrs, "delete", "--dry-run")
	if err != nil {
		t.Fatalf("bare delete --dry-run: %v", err)
	}
	for _, want := range []string{"User:alice", "User:eve"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry run output missing %s: %s", want, out)
		}
	}
	if m, _ := runJSON(t, addrs, "list"); len(aclRows(t, m)) != 2 {
		t.Error("dry run deleted something")
	}

	// A resource filter still narrows.
	out, err = run(t, addrs, "delete", "--topic", "foo", "--dry-run")
	if err != nil {
		t.Fatalf("delete --topic foo --dry-run: %v", err)
	}
	if !strings.Contains(out, "User:alice") || strings.Contains(out, "User:eve") {
		t.Errorf("--topic foo should match only alice's ACL: %s", out)
	}
}

// TestACLDeleteConfirmation covers what now protects a broad delete. Answering
// anything but yes must leave every ACL in place.
func TestACLDeleteConfirmation(t *testing.T) {
	for _, tc := range []struct {
		answer    string
		wantAfter int
	}{
		{"n\n", 2},
		{"\n", 2}, // bare enter declines
		{"y\n", 0},
	} {
		addrs := newCluster(t)
		seedACLs(t, addrs)

		withStdin(t, tc.answer, func() {
			if _, err := run(t, addrs, "delete"); err != nil {
				t.Fatalf("delete with answer %q: %v", tc.answer, err)
			}
		})

		m, _ := runJSON(t, addrs, "list")
		if got := len(aclRows(t, m)); got != tc.wantAfter {
			t.Errorf("answer %q left %d ACLs, want %d", tc.answer, got, tc.wantAfter)
		}
	}
}

// And -y skips the prompt entirely -- the user's own shotgun.
func TestACLDeleteYesSkipsPrompt(t *testing.T) {
	addrs := newCluster(t)
	seedACLs(t, addrs)

	if _, err := run(t, addrs, "delete", "-y"); err != nil {
		t.Fatalf("delete -y: %v", err)
	}
	if m, _ := runJSON(t, addrs, "list"); len(aclRows(t, m)) != 0 {
		t.Error("delete -y did not delete everything")
	}
}

// seedACLs creates two ACLs on different resources.
func seedACLs(t *testing.T, addrs []string) {
	t.Helper()
	if _, err := run(t, addrs, "create", "--topic", "foo",
		"--allow-principal", "User:alice", "--operation", "read"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, addrs, "create", "--group", "g1",
		"--deny-principal", "User:eve", "--operation", "read"); err != nil {
		t.Fatal(err)
	}
}

// withStdin runs fn with os.Stdin replaced by the given input.
func withStdin(t *testing.T, in string, fn func()) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdin
	os.Stdin = r
	go func() { w.WriteString(in); w.Close() }()
	defer func() { os.Stdin = old; r.Close() }()
	fn()
}

// TestACLFilterValidation pins that an unrecognized enum value is caught
// locally, by name, rather than becoming an UNKNOWN element in the request.
// #56 was one instance of this class -- a bare list defaulting --type to
// UNKNOWN -- but any typo produced the same malformed filter, which brokers
// reject while parsing, in the reported case by closing the connection.
func TestACLFilterValidation(t *testing.T) {
	addrs := newCluster(t)

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"list", "--type", "bogus"}, `invalid --type "bogus"`},
		{[]string{"list", "--pattern", "bogus"}, `invalid --pattern "bogus"`},
		{[]string{"list", "--op", "bogus"}, `invalid --operation "bogus"`},
		{[]string{"list", "--perm", "bogus"}, `invalid --permission "bogus"`},
		{[]string{"list", "--operation", "bogus"}, `invalid --operation "bogus"`},
		{[]string{"list", "--permission", "bogus"}, `invalid --permission "bogus"`},
		// delete reports unset before unrecognized.
		{[]string{"delete", "--topic", "f", "--pattern", "bogus", "--op", "read", "--perm", "allow"},
			`invalid --pattern "bogus"`},
		// create takes a narrower set: no filter-only match-anything values.
		{[]string{"create", "--topic", "f", "--allow-principal", "User:a", "--operation", "any"},
			`invalid --operation "any"`},
		{[]string{"create", "--topic", "f", "--allow-principal", "User:a", "--operation", "read", "--pattern", "match"},
			`invalid --pattern "match"`},
	} {
		_, err := run(t, addrs, tc.args...)
		if err == nil {
			t.Errorf("%v: expected an error, got none", tc.args)
		} else if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: error %q does not contain %q", tc.args, err, tc.want)
		}
	}

	// The aliases still work, and valid values still reach the broker.
	for _, args := range [][]string{
		{"list", "--op", "read"},
		{"list", "--operation", "read"},
		{"list", "--perm", "allow"},
		{"list", "--permission", "allow"},
		{"list", "--type", "TRANSACTIONAL-ID"}, // casing and dashes normalize
	} {
		if _, err := run(t, addrs, args...); err != nil {
			t.Errorf("%v: %v", args, err)
		}
	}
}

// TestEnumValuesAreAccepted guards the three-way split in enums.go: the value
// lists drive the error message and shell completion, while validation goes
// through the atoi* conversions. If a list advertised a value the conversion
// does not accept, kcl would suggest something it then rejects.
func TestEnumValuesAreAccepted(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		conv   func(string) int
	}{
		{"type", resourceTypeValues, func(s string) int { return int(atoiResourceType(s)) }},
		{"pattern", patternValues, func(s string) int { return int(atoiResourcePattern(s)) }},
		{"operation", operationValues, func(s string) int { return int(atoiOperation(s)) }},
		{"permission", permissionValues, func(s string) int { return int(atoiPermission(s)) }},
		{"create pattern", createPatternValues, func(s string) int { return int(atoiResourcePattern(s)) }},
		{"create operation", createOperationValues, func(s string) int { return int(atoiOperation(s)) }},
	} {
		if len(tc.values) == 0 {
			t.Errorf("%s: no values advertised", tc.name)
		}
		for _, v := range tc.values {
			if got := tc.conv(v); got == 0 {
				t.Errorf("%s advertises %q, but it converts to UNKNOWN", tc.name, v)
			}
		}
	}

	// The create sets must exclude the filter-only match-anything values,
	// which are what validateCreate rejects.
	for _, v := range createOperationValues {
		if v == "any" {
			t.Error("createOperationValues must not offer 'any'")
		}
	}
	for _, v := range createPatternValues {
		if v == "any" || v == "match" {
			t.Errorf("createPatternValues must not offer %q", v)
		}
	}
}

// A filter that matches nothing says so and does not prompt at all.
func TestACLDeleteNoMatches(t *testing.T) {
	addrs := newCluster(t)
	seedACLs(t, addrs)

	out, err := run(t, addrs, "delete", "--topic", "nosuchtopic", "-y")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if strings.Contains(out, "will be deleted") {
		t.Errorf("printed a deletion header with no matches: %s", out)
	}
	if got := len(aclRows(t, mustJSON(t, addrs, "list"))); got != 2 {
		t.Errorf("deleted something: %d ACLs remain of 2", got)
	}
}

func mustJSON(t *testing.T, addrs []string, args ...string) map[string]any {
	t.Helper()
	m, err := runJSON(t, addrs, args...)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
