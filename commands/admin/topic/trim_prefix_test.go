package topic

import (
	"slices"
	"strings"
	"testing"
)

var trimPrefixKeysSorted = []string{"error", "message", "new_offset", "partition", "prior_offset", "topic"}

// TestTrimPrefix pins the document: without -y and with stdin not a
// terminal, the plan alone as a dry run and exit 0; with -y, plan and
// results, and the records gone.
func TestTrimPrefix(t *testing.T) {
	_, addr := newCluster(t, 2, 5, "trim")

	// go test's stdin is not a terminal, so the prompt answers no.
	got, code := runKcl(t, addr, "trim-prefix", "trim", "-o", "3", "--format", "json")
	if code != 0 {
		t.Fatalf("declined: exit %d\n%s", code, got)
	}
	doc := jsonDoc(t, got, "topic.trim-prefix")
	if doc["dry_run"] != true {
		t.Errorf("declined: dry_run = %v", doc["dry_run"])
	}
	plan := rowsOf(t, doc, "plan")
	if len(plan) != 2 {
		t.Fatalf("plan = %v", plan)
	}
	if !slices.Equal(keysOf(plan[0]), trimPrefixKeysSorted) {
		t.Errorf("plan keys = %v, want %v", keysOf(plan[0]), trimPrefixKeysSorted)
	}
	for i, row := range plan {
		if row["topic"] != "trim" || row["partition"] != float64(i) || row["prior_offset"] != float64(0) || row["new_offset"] != float64(3) || row["error"] != "" {
			t.Errorf("plan row = %v", row)
		}
	}
	if results := rowsOf(t, doc, "results"); len(results) != 0 {
		t.Errorf("declined: results = %v, want none", results)
	}

	// awk prints the plan rows, ERROR a dash.
	got, code = runKcl(t, addr, "trim-prefix", "trim", "-o", "3", "-p", "1", "--format", "awk")
	if code != 0 {
		t.Fatalf("declined awk: exit %d\n%s", code, got)
	}
	rows := awkRows(t, got, len(trimPrefixHeaders))
	if len(rows) != 1 || !slices.Equal(rows[0], []string{"trim", "1", "0", "3", "-", "-"}) {
		t.Errorf("declined awk rows = %v", rows)
	}

	// Nothing has moved yet.
	if got, _ := runKcl(t, addr, "list-offsets", "trim", "--format", "awk"); strings.Count(got, "\t0\t5\t5\t") != 2 {
		t.Errorf("declined run moved the start offset:\n%s", got)
	}

	got, code = runKcl(t, addr, "trim-prefix", "trim", "-o", "3", "-y", "--format", "json")
	if code != 0 {
		t.Fatalf("-y: exit %d\n%s", code, got)
	}
	doc = jsonDoc(t, got, "topic.trim-prefix")
	if _, ok := doc["dry_run"]; ok {
		t.Errorf("-y: dry_run present")
	}
	results := rowsOf(t, doc, "results")
	if len(results) != 2 {
		t.Fatalf("results = %v", results)
	}
	for i, row := range results {
		if row["partition"] != float64(i) || row["prior_offset"] != float64(0) || row["new_offset"] != float64(3) || row["error"] != "" || row["message"] != "" {
			t.Errorf("result row = %v", row)
		}
	}
	if got, _ := runKcl(t, addr, "list-offsets", "trim", "--format", "awk"); strings.Count(got, "\t3\t5\t5\t") != 2 {
		t.Errorf("start offsets after the trim:\n%s", got)
	}

	// A second run prints the new prior offsets, and text says OK.
	got, code = runKcl(t, addr, "trim-prefix", "trim", "-o", "end", "-y")
	if code != 0 || !strings.Contains(got, "PRIOR-OFFSET") || !strings.Contains(got, "OK") || strings.Count(got, "\n") != 7 {
		t.Errorf("text: exit %d\n%s", code, got)
	}

	// Errors.
	for _, test := range []struct {
		name string
		args []string
		code int
	}{
		{"missing topic", []string{"trim-prefix", "nosuch", "-o", "1", "-y"}, 1},
		{"range offset", []string{"trim-prefix", "trim", "-o", "1:2", "-y"}, 2},
		{"no offset", []string{"trim-prefix", "trim", "-y"}, 2},
		{"bad offset", []string{"trim-prefix", "trim", "-o", "abc", "-y"}, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, code := runKcl(t, addr, test.args...); code != test.code {
				t.Errorf("exit %d, want %d\n%s", code, test.code, got)
			}
		})
	}
}
