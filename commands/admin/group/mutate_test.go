package group

import (
	"context"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/out"
)

// TestDeleteDryRunDocument pins that --dry-run prints the document a real
// run would, marked dry_run, with the cells only the run fills null, and
// deletes nothing.
func TestDeleteDryRunDocument(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(t.Context(), 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	commitAt(t, adm, "keep", "t", 0, 1)

	stdout, err := runGroup(t, c, "", "group", "delete", "keep", "--dry-run", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	doc := parseJSON(t, stdout)
	if doc["dry_run"] != true || doc["_command"] != "group.delete" {
		t.Errorf("want dry_run true under group.delete: %s", stdout)
	}
	results := doc["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("want one result: %s", stdout)
	}
	row := results[0].(map[string]any)
	if row["group"] != "keep" || row["broker"] != nil || row["error"] != nil || row["message"] != nil {
		t.Errorf("row = %v, want group keep with broker, error, and message null", row)
	}

	stdout, err = runGroup(t, c, "", "group", "delete", "keep", "--dry-run", "--format", "awk")
	if err != nil {
		t.Fatal(err)
	}
	checkAwkFields(t, stdout, "group", "delete", "--format", "awk")
	if rows := awkRows(stdout); len(rows) != 1 || !slices.Equal(rows[0], []string{"-", "keep", "-", "-"}) {
		t.Errorf("awk = %q, want one row - keep - -", rows)
	}

	// The group is still there.
	stdout, err = runGroup(t, c, "", "group", "list", "--format", "awk")
	if err != nil {
		t.Fatal(err)
	}
	if rows := awkRows(stdout); len(rows) != 1 || rows[0][1] != "keep" {
		t.Errorf("the dry run deleted the group: %q", rows)
	}
}

// TestOffsetDelete pins that -t TOPIC alone deletes every partition of the
// topic, that a topic the cluster does not know is an error, and that a
// partition the broker refuses fails the command after every row prints.
func TestOffsetDelete(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	if _, err := adm.CreateTopics(ctx, 2, 1, nil, "t", "u"); err != nil {
		t.Fatal(err)
	}
	produceTo(t, c, "t", 1, 0, 1)
	produceTo(t, c, "u", 1, 0)
	commitAt(t, adm, "od", "t", 0, 1)
	commitAt(t, adm, "od", "t", 1, 1)
	commitAt(t, adm, "od", "u", 0, 1)

	t.Run("whole topic", func(t *testing.T) {
		args := []string{"group", "offset-delete", "od", "-t", "t", "--format", "awk"}
		stdout, err := runGroup(t, c, "", args...)
		if err != nil {
			t.Fatal(err)
		}
		checkAwkFields(t, stdout, args...)
		want := [][]string{{"t", "0", "-", "-"}, {"t", "1", "-", "-"}}
		if rows := awkRows(stdout); !slices.EqualFunc(rows, want, slices.Equal) {
			t.Errorf("rows = %q, want %q", rows, want)
		}
		fetched, err := adm.FetchOffsets(ctx, "od")
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := fetched.Lookup("t", 0); ok {
			t.Error("t partition 0 still has a committed offset")
		}
		if _, ok := fetched.Lookup("u", 0); !ok {
			t.Error("u partition 0 lost its committed offset")
		}
	})

	t.Run("unknown topic", func(t *testing.T) {
		_, err := runGroup(t, c, "", "group", "offset-delete", "od", "-t", "nosuch", "--format", "json")
		if err == nil || out.ExitCode(err) != out.ExitError {
			t.Errorf("err = %v, want an exit 1 error naming the topic", err)
		}
	})

	t.Run("no topic", func(t *testing.T) {
		_, err := runGroup(t, c, "", "group", "offset-delete", "od", "--format", "json")
		if err == nil || out.ExitCode(err) != out.ExitUsage {
			t.Errorf("err = %v, want a usage error", err)
		}
	})

	t.Run("a refused partition fails the command", func(t *testing.T) {
		c.Fault(kfake.Fault{
			Keys:       []kmsg.Key{kmsg.OffsetDelete},
			Topic:      "u",
			Partitions: []int32{0},
			Err:        kerr.GroupSubscribedToTopic,
		})
		stdout, err := runGroup(t, c, "", "group", "offset-delete", "od", "-t", "u", "--format", "json")
		if err != out.ErrSilent {
			t.Fatalf("err = %v, want ErrSilent\n%s", err, stdout)
		}
		doc := parseJSON(t, stdout)
		results := doc["results"].([]any)
		if len(results) != 2 {
			t.Fatalf("want both partitions of u: %s", stdout)
		}
		refused, fine := results[0].(map[string]any), results[1].(map[string]any)
		if refused["topic"] != "u" || refused["error"] != kerr.GroupSubscribedToTopic.Error() || refused["message"] != "" {
			t.Errorf("row = %v, want u 0 with GROUP_SUBSCRIBED_TO_TOPIC and an empty message", refused)
		}
		if fine["partition"] != float64(1) || fine["error"] != "" {
			t.Errorf("row = %v, want u 1 with an empty error", fine)
		}
	})
}

// TestSeekDocument pins the seek document: the plan under --dry-run and on
// a declined prompt with results empty and dry_run set, both under a real
// run, and exit 1 when a commit fails. Under awk the plan rows and the
// result rows have the same fields.
func TestSeekDocument(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	produceN(t, cl, "t", 5)
	commitAt(t, adm, "sk", "t", 0, 5)

	type doc struct {
		Command string           `json:"_command"`
		DryRun  bool             `json:"dry_run"`
		Group   string           `json:"group"`
		Plan    []map[string]any `json:"plan"`
		Results []map[string]any `json:"results"`
	}
	get := func(t *testing.T, stdin string, args ...string) (doc, error) {
		t.Helper()
		stdout, err := runGroup(t, c, stdin, append([]string{"group", "seek", "sk", "--format", "json"}, args...)...)
		var d doc
		if uerr := unmarshalJSON(stdout, &d); uerr != nil {
			t.Fatalf("stdout is not JSON: %v\n%s", uerr, stdout)
		}
		if d.Command != "group.seek" || d.Group != "sk" {
			t.Errorf("document names %s %s, want group.seek sk", d.Command, d.Group)
		}
		return d, err
	}
	planRow := func(t *testing.T, d doc) {
		t.Helper()
		if len(d.Plan) != 1 {
			t.Fatalf("plan = %v, want one row", d.Plan)
		}
		p := d.Plan[0]
		if p["topic"] != "t" || p["partition"] != float64(0) || p["prior_offset"] != float64(5) || p["new_offset"] != float64(0) || p["error"] != nil || p["message"] != nil {
			t.Errorf("plan row = %v, want t 0 from 5 to 0 with error and message null", p)
		}
	}

	t.Run("dry run", func(t *testing.T) {
		d, err := get(t, "", "--to", "start", "--dry-run")
		if err != nil {
			t.Fatal(err)
		}
		planRow(t, d)
		if !d.DryRun || len(d.Results) != 0 || d.Results == nil {
			t.Errorf("want dry_run true and results []: %+v", d)
		}
	})

	t.Run("declined, stdin is not a terminal", func(t *testing.T) {
		d, err := get(t, "y\n", "--to", "start")
		if err != nil {
			t.Fatal(err)
		}
		planRow(t, d)
		if !d.DryRun || len(d.Results) != 0 {
			t.Errorf("want dry_run true and no results: %+v", d)
		}
		fetched, err := adm.FetchOffsets(ctx, "sk")
		if err != nil {
			t.Fatal(err)
		}
		if o, _ := fetched.Lookup("t", 0); o.At != 5 {
			t.Errorf("committed offset = %d, want 5 untouched", o.At)
		}
	})

	t.Run("awk plan and result rows share a shape", func(t *testing.T) {
		args := []string{"group", "seek", "sk", "--to", "start", "--format", "awk"}
		stdout, err := runGroup(t, c, "", args...)
		if err != nil {
			t.Fatal(err)
		}
		checkAwkFields(t, stdout, args...)
		if rows := awkRows(stdout); len(rows) != 1 || !slices.Equal(rows[0], []string{"t", "0", "5", "0", "-", "-"}) {
			t.Errorf("declined awk = %q, want the plan row alone", rows)
		}
		stdout, err = runGroup(t, c, "", append(args, "-y")...)
		if err != nil {
			t.Fatal(err)
		}
		checkAwkFields(t, stdout, args...)
		want := [][]string{{"t", "0", "5", "0", "-", "-"}, {"t", "0", "5", "0", "-", "-"}}
		if rows := awkRows(stdout); !slices.EqualFunc(rows, want, slices.Equal) {
			t.Errorf("committed awk = %q, want the plan row then the result row", rows)
		}
	})

	t.Run("committed", func(t *testing.T) {
		commitAt(t, adm, "sk", "t", 0, 5)
		d, err := get(t, "", "--to", "start", "-y")
		if err != nil {
			t.Fatal(err)
		}
		planRow(t, d)
		if d.DryRun || len(d.Results) != 1 {
			t.Fatalf("want one result and no dry_run: %+v", d)
		}
		r := d.Results[0]
		if r["topic"] != "t" || r["new_offset"] != float64(0) || r["error"] != "" || r["message"] != "" {
			t.Errorf("result row = %v, want t to 0 with error and message empty", r)
		}
		fetched, err := adm.FetchOffsets(ctx, "sk")
		if err != nil {
			t.Fatal(err)
		}
		if o, _ := fetched.Lookup("t", 0); o.At != 0 {
			t.Errorf("committed offset = %d, want 0", o.At)
		}
	})

	t.Run("a failed commit exits 1", func(t *testing.T) {
		c.Fault(kfake.Fault{
			Keys:  []kmsg.Key{kmsg.OffsetCommit},
			Group: "sk",
			Err:   kerr.UnknownMemberID,
		})
		d, err := get(t, "", "--to", "end", "-y")
		if err != out.ErrSilent {
			t.Fatalf("err = %v, want ErrSilent", err)
		}
		if len(d.Results) != 1 {
			t.Fatalf("want one result: %+v", d)
		}
		r := d.Results[0]
		if r["error"] != kerr.UnknownMemberID.Error() || r["message"] != "group is not empty (a consumer may have joined)" {
			t.Errorf("result row = %v, want UNKNOWN_MEMBER_ID with the not-empty hint", r)
		}
	})
}
