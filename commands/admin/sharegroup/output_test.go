package sharegroup

import (
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/out"
)

// TestListAndDescribe pins the shapes of share-group list and describe
// against two share groups: list sorted by group under the key "group",
// every awk row of every section led by its group and as wide as its
// header, JSON arrays present and empty on a group the broker does not
// know, and TOTAL-LAG in text as the number alone. Each group fetched the
// four records without acknowledging them, so its start offset is 0 and
// its lag 4.
func TestListAndDescribe(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(t.Context(), 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	produceN(t, cl, "t", 4)
	joinShareGroup(t, c, "sg-b", 4, "t")
	joinShareGroup(t, c, "sg-a", 4, "t")

	t.Run("list", func(t *testing.T) {
		args := []string{"share-group", "list", "--format", "awk"}
		stdout, err := runShareGroup(t, c, "", args...)
		if err != nil {
			t.Fatal(err)
		}
		checkAwkFields(t, stdout, args...)
		rows := awkRows(stdout)
		if len(rows) != 2 || rows[0][1] != "sg-a" || rows[1][1] != "sg-b" || rows[0][3] != "-" {
			t.Errorf("rows = %q, want sg-a then sg-b with no error", rows)
		}
		stdout, err = runShareGroup(t, c, "", "share-group", "list", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		groups := parseJSON(t, stdout)["groups"].([]any)
		first := groups[0].(map[string]any)
		if first["group"] != "sg-a" || first["error"] != "" {
			t.Errorf("first row = %v, want sg-a with an empty error", first)
		}
		if _, ok := first["group_id"]; ok {
			t.Errorf("group_id is still a key: %s", stdout)
		}
	})

	t.Run("list --state", func(t *testing.T) {
		stdout, err := runShareGroup(t, c, "", "share-group", "list", "--state", "stable", "--format", "awk")
		if err != nil {
			t.Fatal(err)
		}
		if rows := awkRows(stdout); len(rows) != 0 {
			t.Errorf("want no Stable group, got %q", rows)
		}
		stdout, err = runShareGroup(t, c, "", "share-group", "list", "-f", "empty", "--format", "awk")
		if err != nil {
			t.Fatal(err)
		}
		if rows := awkRows(stdout); len(rows) != 2 {
			t.Errorf("want both Empty groups under the old -f, got %q", rows)
		}
	})

	for _, section := range []string{"offsets", "summary", "members"} {
		t.Run("awk "+section, func(t *testing.T) {
			args := []string{"share-group", "describe", "--section", section, "--format", "awk"}
			stdout, err := runShareGroup(t, c, "", args...)
			if err != nil {
				t.Fatal(err)
			}
			checkAwkFields(t, stdout, args...)
			rows := awkRows(stdout)
			switch section {
			case "offsets":
				want := [][]string{{"sg-a", "t", "0", "0", "0", "4", "-", "-"}, {"sg-b", "t", "0", "0", "0", "4", "-", "-"}}
				if !slices.EqualFunc(rows, want, slices.Equal) {
					t.Errorf("rows = %q, want %q", rows, want)
				}
			case "summary":
				if len(rows) != 2 || rows[0][0] != "sg-a" || rows[0][2] != "Empty" || rows[0][6] != "0" || rows[0][7] != "4" || rows[0][8] != "-" {
					t.Errorf("rows = %q, want sg-a Empty with 0 members, lag 4, no error", rows)
				}
			case "members":
				if len(rows) != 0 {
					t.Errorf("want no member rows for Empty groups, got %q", rows)
				}
			}
		})
	}

	t.Run("text total lag is a number", func(t *testing.T) {
		stdout, err := runShareGroup(t, c, "", "share-group", "describe", "sg-a", "--section", "summary")
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for line := range strings.SplitSeq(stdout, "\n") {
			if f := strings.Fields(line); len(f) > 0 && f[0] == "TOTAL-LAG" {
				found = true
				if len(f) != 2 || f[1] != "4" {
					t.Errorf("TOTAL-LAG line = %q, want the number alone", line)
				}
			}
		}
		if !found {
			t.Errorf("no TOTAL-LAG line:\n%s", stdout)
		}
	})

	t.Run("json keys and a missing group", func(t *testing.T) {
		stdout, err := runShareGroup(t, c, "", "share-group", "describe", "sg-a", "nope", "--format", "json")
		if err != out.ErrSilent {
			t.Fatalf("err = %v, want ErrSilent\n%s", err, stdout)
		}
		groups := parseJSON(t, stdout)["groups"].([]any)
		if len(groups) != 2 {
			t.Fatalf("want two groups: %s", stdout)
		}
		nope := groups[0].(map[string]any)
		if nope["group"] != "nope" {
			nope = groups[1].(map[string]any)
		}
		if _, ok := nope["group_id"]; ok {
			t.Errorf("group_id is still a key: %s", stdout)
		}
		if err, _ := nope["error"].(string); !strings.HasPrefix(err, "GROUP_ID_NOT_FOUND") {
			t.Errorf("error = %v, want GROUP_ID_NOT_FOUND", nope["error"])
		}
		for _, key := range []string{"members", "offsets"} {
			if v, ok := nope[key].([]any); !ok || len(v) != 0 {
				t.Errorf("%s = %v, want []", key, nope[key])
			}
		}
		if nope["total_lag"] != nil {
			t.Errorf("total_lag = %v, want null", nope["total_lag"])
		}
		sga := groups[0].(map[string]any)
		if sga["group"] != "sg-a" {
			sga = groups[1].(map[string]any)
		}
		if sga["error"] != "" || sga["total_lag"] != float64(4) {
			t.Errorf("sg-a error = %v total_lag = %v, want \"\" and 4", sga["error"], sga["total_lag"])
		}
		offsets := sga["offsets"].([]any)
		if len(offsets) != 1 || offsets[0].(map[string]any)["start_offset"] != float64(0) || offsets[0].(map[string]any)["lag"] != float64(4) {
			t.Errorf("offsets = %v, want one row at start offset 0 with lag 4", offsets)
		}
	})

	// A missing group exits 1 under awk too: the default offsets section
	// has no row for it, so the exit code is all that says so, and the
	// summary section carries the error in its row.
	t.Run("a missing group exits 1 under awk", func(t *testing.T) {
		stdout, err := runShareGroup(t, c, "", "share-group", "describe", "nope", "--format", "awk")
		if err != out.ErrSilent {
			t.Fatalf("err = %v, want ErrSilent\n%s", err, stdout)
		}
		if stdout != "" {
			t.Errorf("offsets rows = %q, want none for a group the broker does not know", stdout)
		}
		args := []string{"share-group", "describe", "nope", "--section", "summary", "--format", "awk"}
		stdout, err = runShareGroup(t, c, "", args...)
		if err != out.ErrSilent {
			t.Fatalf("err = %v, want ErrSilent\n%s", err, stdout)
		}
		checkAwkFields(t, stdout, args...)
		rows := awkRows(stdout)
		if len(rows) != 1 || rows[0][0] != "nope" || rows[0][len(rows[0])-2] != "GROUP_ID_NOT_FOUND" {
			t.Errorf("summary rows = %q, want one row for nope with GROUP_ID_NOT_FOUND under ERROR", rows)
		}
	})
}

// TestDeleteDryRunDocument pins that --dry-run prints the document a real
// run would, marked dry_run, and deletes nothing.
func TestDeleteDryRunDocument(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(t.Context(), 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	produceN(t, cl, "t", 1)
	joinShareGroup(t, c, "keep", 1, "t")

	stdout, err := runShareGroup(t, c, "", "share-group", "delete", "keep", "--dry-run", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	doc := parseJSON(t, stdout)
	results := doc["results"].([]any)
	if doc["dry_run"] != true || len(results) != 1 {
		t.Fatalf("want dry_run true and one result: %s", stdout)
	}
	row := results[0].(map[string]any)
	if row["group"] != "keep" || row["broker"] != nil || row["error"] != nil {
		t.Errorf("row = %v, want group keep with broker and error null", row)
	}
	stdout, err = runShareGroup(t, c, "", "share-group", "list", "--format", "awk")
	if err != nil {
		t.Fatal(err)
	}
	if rows := awkRows(stdout); len(rows) != 1 || rows[0][1] != "keep" {
		t.Errorf("the dry run deleted the group: %q", rows)
	}
}

// TestOffsetDeleteResult pins the TOPIC ERROR MESSAGE row and that a topic
// the broker refuses fails the command after every row prints.
func TestOffsetDeleteResult(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopics(t.Context(), 1, 1, nil, "t", "u"); err != nil {
		t.Fatal(err)
	}
	produceN(t, cl, "t", 1)
	produceN(t, cl, "u", 1)
	joinShareGroup(t, c, "od", 2, "t", "u")

	c.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.DeleteShareGroupOffsets},
		Topic: "u",
		Err:   kerr.UnknownTopicOrPartition,
	})
	args := []string{"share-group", "offset-delete", "od", "-t", "t", "-t", "u", "--format", "awk"}
	stdout, err := runShareGroup(t, c, "", args...)
	if err != out.ErrSilent {
		t.Fatalf("err = %v, want ErrSilent\n%s", err, stdout)
	}
	checkAwkFields(t, stdout, args...)
	rows := awkRows(stdout)
	if len(rows) != 2 || rows[0][0] != "t" || rows[0][1] != "-" || rows[1][0] != "u" || !strings.HasPrefix(rows[1][1], "UNKNOWN_TOPIC_OR_PARTITION") {
		t.Errorf("rows = %q, want t fine and u refused", rows)
	}
}

// TestSeekDocument pins the seek document: the plan with the current start
// offset beside the new one, results empty under --dry-run and on a
// declined prompt, and one row per partition after a run.
func TestSeekDocument(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(t.Context(), 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	produceN(t, cl, "t", 4)
	joinShareGroup(t, c, "sk", 4, "t")

	type doc struct {
		Command string           `json:"_command"`
		DryRun  bool             `json:"dry_run"`
		Group   string           `json:"group"`
		Plan    []map[string]any `json:"plan"`
		Results []map[string]any `json:"results"`
	}
	get := func(t *testing.T, args ...string) (doc, error) {
		t.Helper()
		stdout, err := runShareGroup(t, c, "", append([]string{"share-group", "seek", "sk", "-t", "t", "--format", "json"}, args...)...)
		var d doc
		if uerr := unmarshalDoc(stdout, &d); uerr != nil {
			t.Fatalf("stdout is not JSON: %v\n%s", uerr, stdout)
		}
		if d.Command != "share-group.seek" || d.Group != "sk" {
			t.Errorf("document names %s %s, want share-group.seek sk", d.Command, d.Group)
		}
		return d, err
	}
	planRow := func(t *testing.T, d doc, prior, at float64) {
		t.Helper()
		if len(d.Plan) != 1 {
			t.Fatalf("plan = %v, want one row", d.Plan)
		}
		p := d.Plan[0]
		if p["topic"] != "t" || p["partition"] != float64(0) || p["prior_offset"] != prior || p["new_offset"] != at || p["error"] != nil {
			t.Errorf("plan row = %v, want t 0 from %v to %v with error null", p, prior, at)
		}
	}

	t.Run("dry run", func(t *testing.T) {
		d, err := get(t, "--to", "start", "--dry-run")
		if err != nil {
			t.Fatal(err)
		}
		planRow(t, d, 0, 0)
		if !d.DryRun || d.Results == nil || len(d.Results) != 0 {
			t.Errorf("want dry_run true and results []: %+v", d)
		}
	})

	t.Run("declined", func(t *testing.T) {
		d, err := get(t, "--to", "start")
		if err != nil {
			t.Fatal(err)
		}
		planRow(t, d, 0, 0)
		if !d.DryRun || len(d.Results) != 0 {
			t.Errorf("want dry_run true and no results: %+v", d)
		}
	})

	t.Run("run", func(t *testing.T) {
		d, err := get(t, "--to", "2", "-y")
		if err != nil {
			t.Fatal(err)
		}
		planRow(t, d, 0, 2)
		if d.DryRun || len(d.Results) != 1 || d.Results[0]["error"] != "" || d.Results[0]["message"] != "" {
			t.Errorf("want one clean result and no dry_run: %+v", d)
		}
		// The next plan sees the new start offset.
		d, err = get(t, "--to", "start", "--dry-run")
		if err != nil {
			t.Fatal(err)
		}
		planRow(t, d, 2, 0)
	})

	t.Run("awk plan and result rows share a shape", func(t *testing.T) {
		args := []string{"share-group", "seek", "sk", "-t", "t", "--to", "1", "-y", "--format", "awk"}
		stdout, err := runShareGroup(t, c, "", args...)
		if err != nil {
			t.Fatal(err)
		}
		checkAwkFields(t, stdout, args...)
		want := [][]string{{"t", "0", "2", "1", "-", "-"}, {"t", "0", "2", "1", "-", "-"}}
		if rows := awkRows(stdout); !slices.EqualFunc(rows, want, slices.Equal) {
			t.Errorf("awk = %q, want the plan row then the result row", rows)
		}
	})

	t.Run("a refused partition exits 1", func(t *testing.T) {
		c.Fault(kfake.Fault{
			Keys:  []kmsg.Key{kmsg.AlterShareGroupOffsets},
			Topic: "t",
			Err:   kerr.NonEmptyGroup,
		})
		d, err := get(t, "--to", "start", "-y")
		if err != out.ErrSilent {
			t.Fatalf("err = %v, want ErrSilent", err)
		}
		if len(d.Results) != 1 || !strings.HasPrefix(d.Results[0]["error"].(string), "NON_EMPTY_GROUP") {
			t.Errorf("results = %v, want one NON_EMPTY_GROUP row", d.Results)
		}
	})
}
