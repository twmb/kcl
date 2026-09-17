package topic

import (
	"slices"
	"strings"
	"testing"
)

// TestCreate pins the result shape: a real create fills TOPIC-ID, a dry run
// leaves it unknown and marks the document, and an error row exits 1.
func TestCreate(t *testing.T) {
	_, addr := newCluster(t, 1, 0, "exists")

	for _, test := range []struct {
		name   string
		args   []string
		code   int
		dryRun bool
		id     bool   // topic_id is a string
		errs   string // substring of the error cell, or ""
	}{
		{name: "creates", args: []string{"create", "fresh", "-p", "2", "-c", "retention.ms=1000"}, id: true},
		{name: "old -k config flag", args: []string{"create", "fresh2", "-k", "retention.ms=1000"}, id: true},
		{name: "dry run", args: []string{"create", "dry", "--dry-run"}, dryRun: true},
		{name: "already exists", args: []string{"create", "exists"}, code: 1, errs: "TOPIC_ALREADY_EXISTS"},
		{name: "bad config", args: []string{"create", "x", "-c", "novalue"}, code: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, code := runKcl(t, addr, append(slices.Clone(test.args), "--format", "json")...)
			if code != test.code {
				t.Fatalf("exit %d, want %d\n%s", code, test.code, got)
			}
			if test.code == 2 {
				return
			}
			doc := jsonDoc(t, got, "topic.create")
			if _, ok := doc["dry_run"]; ok != test.dryRun {
				t.Errorf("dry_run present = %v, want %v: %s", ok, test.dryRun, got)
			}
			rows := rowsOf(t, doc, "topics")
			if len(rows) != 1 {
				t.Fatalf("rows = %v", rows)
			}
			row := rows[0]
			if want := []string{"error", "message", "topic", "topic_id"}; !slices.Equal(keysOf(row), want) {
				t.Errorf("keys = %v, want %v", keysOf(row), want)
			}
			if _, isString := row["topic_id"].(string); isString != test.id {
				t.Errorf("topic_id = %v, want a string: %v", row["topic_id"], test.id)
			}
			if errStr := row["error"].(string); !strings.Contains(errStr, test.errs) || (test.errs == "" && errStr != "") {
				t.Errorf("error = %q, want %q", errStr, test.errs)
			}
		})
	}

	// awk prints the same four columns, a dash for the unknown id.
	got, code := runKcl(t, addr, "create", "dry2", "--dry-run", "--format", "awk")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, got)
	}
	rows := awkRows(t, got, 4)
	if len(rows) != 1 || rows[0][1] != "-" || rows[0][2] != "-" {
		t.Errorf("awk dry run = %q", got)
	}
}

func TestDelete(t *testing.T) {
	_, addr := newCluster(t, 1, 0, "del-a", "del-b", "keep")

	// A dry run prints the rows a real run would, changes nothing, and
	// carries dry_run.
	got, code := runKcl(t, addr, "delete", "-r", "^del-", "--dry-run", "--format", "json")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, got)
	}
	doc := jsonDoc(t, got, "topic.delete")
	if doc["dry_run"] != true {
		t.Errorf("dry_run = %v", doc["dry_run"])
	}
	var names []string
	for _, row := range rowsOf(t, doc, "topics") {
		names = append(names, row["topic"].(string))
		if row["error"] != "" || row["message"] != "" {
			t.Errorf("dry run row = %v", row)
		}
	}
	if want := []string{"del-a", "del-b"}; !slices.Equal(names, want) {
		t.Errorf("dry run topics = %v, want %v", names, want)
	}
	if got, code := runKcl(t, addr, "list", "--format", "awk"); code != 0 || len(awkRows(t, got, len(ListHeaders))) != 3 {
		t.Errorf("dry run deleted something:\n%s", got)
	}

	// A real run: one topic gone, one that does not exist errors the
	// command.
	got, code = runKcl(t, addr, "delete", "del-a", "nosuch", "--format", "awk")
	if code != 1 {
		t.Fatalf("exit %d, want 1\n%s", code, got)
	}
	rows := awkRows(t, got, 3)
	if len(rows) != 2 || rows[0][0] != "del-a" || rows[0][1] != "-" || rows[1][1] != "UNKNOWN_TOPIC_OR_PARTITION" {
		t.Errorf("delete rows = %v", rows)
	}
	if got, code := runKcl(t, addr, "list", "--format", "awk"); code != 0 || len(awkRows(t, got, len(ListHeaders))) != 2 {
		t.Errorf("del-a still listed:\n%s", got)
	}

	// No match under a regex is an empty document, not an error.
	got, code = runKcl(t, addr, "delete", "-r", "^zzz", "--format", "json")
	if code != 0 || !strings.Contains(got, `"topics":[]`) {
		t.Errorf("no match: exit %d %s", code, got)
	}
}
