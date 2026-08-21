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

// TestACLDeleteRequiresExplicitFilters pins the deliberate asymmetry with list:
// list defaults its filters to match-all, but delete refuses to build a filter
// it was not fully told, so a bare "kcl acl delete" can never delete
// everything -- and can never send the UNKNOWN filter elements of #56 either.
//
// It also pins that every missing filter is named in one error rather than only
// the first found, so discovering the required shape is not four sequential
// rejections.
func TestACLDeleteRequiresExplicitFilters(t *testing.T) {
	addrs := newCluster(t)

	for _, tc := range []struct {
		args []string
		want []string
	}{
		{[]string{"delete", "--dry-run"},
			[]string{"--type", "--pattern", "--operation", "--permission"}},
		{[]string{"delete", "--topic", "foo", "--dry-run"},
			[]string{"--pattern", "--operation", "--permission"}},
		{[]string{"delete", "--topic", "foo", "--pattern", "literal", "--dry-run"},
			[]string{"--operation", "--permission"}},
		{[]string{"delete", "--topic", "foo", "--pattern", "literal", "--op", "read", "--dry-run"},
			[]string{"--permission"}},
	} {
		_, err := run(t, addrs, tc.args...)
		if err == nil {
			t.Errorf("%v: expected an error, got none", tc.args)
			continue
		}
		for _, want := range tc.want {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%v: error %q does not name %s", tc.args, err, want)
			}
		}
		// Filters already supplied must not be reported as missing.
		for _, notWant := range []string{"--type", "--pattern", "--operation", "--permission"} {
			var expected bool
			for _, w := range tc.want {
				if w == notWant {
					expected = true
				}
			}
			if !expected && strings.Contains(err.Error(), notWant) {
				t.Errorf("%v: error %q wrongly names %s", tc.args, err, notWant)
			}
		}
	}

	// Fully specified, it goes through.
	if _, err := run(t, addrs, "delete",
		"--topic", "foo", "--pattern", "literal", "--op", "read", "--perm", "allow", "--dry-run",
	); err != nil {
		t.Errorf("fully specified delete --dry-run: %v", err)
	}
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
