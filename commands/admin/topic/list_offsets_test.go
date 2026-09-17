package topic

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
)

var listOffsetsKeysSorted = []string{"at", "broker", "end", "end_epoch", "error", "partition", "stable", "stable_epoch", "start", "start_epoch", "topic"}

// TestListOffsets pins the one row shape: epochs are always numbers, AT is
// unknown without --at, a missing topic or partition is an error row and
// exit 1, and text hides the epoch and AT columns until asked.
func TestListOffsets(t *testing.T) {
	_, addr := newCluster(t, 2, 4, "lo-a", "lo-b")

	got, code := runKcl(t, addr, "list-offsets", "--format", "json")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, got)
	}
	rows := rowsOf(t, jsonDoc(t, got, "topic.list-offsets"), "offsets")
	if len(rows) != 4 {
		t.Fatalf("rows = %d, want 4 (two topics, two partitions)", len(rows))
	}
	if !slices.Equal(keysOf(rows[0]), listOffsetsKeysSorted) {
		t.Errorf("keys = %v, want %v", keysOf(rows[0]), listOffsetsKeysSorted)
	}
	for _, row := range rows {
		if row["start"] != float64(0) || row["stable"] != float64(4) || row["end"] != float64(4) || row["at"] != nil || row["error"] != "" || row["broker"] != float64(0) {
			t.Errorf("row = %v", row)
		}
		for _, k := range []string{"start_epoch", "stable_epoch", "end_epoch"} {
			if _, ok := row[k].(float64); !ok {
				t.Errorf("%s = %v, want a number", k, row[k])
			}
		}
	}
	if rows[0]["topic"] != "lo-a" || rows[3]["topic"] != "lo-b" || rows[1]["partition"] != float64(1) {
		t.Errorf("rows are not sorted by topic then partition: %v", rows)
	}

	// awk: every column, one row per partition named.
	got, code = runKcl(t, addr, "list-offsets", "lo-b:1", "--format", "awk")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, got)
	}
	awk := awkRows(t, got, len(listOffsetsHeaders))
	if len(awk) != 1 || awk[0][1] != "lo-b" || awk[0][2] != "1" || awk[0][9] != "-" || awk[0][10] != "-" {
		t.Errorf("awk = %v", awk)
	}

	// --at: a timestamp before every record is offset 0, one after them
	// all is the end.
	got, code = runKcl(t, addr, "list-offsets", "lo-a:0", "--at", "2000-01-01", "--format", "awk")
	if code != 0 || awkRows(t, got, len(listOffsetsHeaders))[0][9] != "0" {
		t.Errorf("--at past: exit %d %q", code, got)
	}
	future := strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10)
	got, code = runKcl(t, addr, "list-offsets", "lo-a:0", "--at", future, "--format", "awk")
	if code != 0 || awkRows(t, got, len(listOffsetsHeaders))[0][9] != "4" {
		t.Errorf("--at future: exit %d %q", code, got)
	}
	if got, code := runKcl(t, addr, "list-offsets", "lo-a", "--at", "yesterday"); code != 2 {
		t.Errorf("bad --at: exit %d %q", code, got)
	}

	// text hides epochs and AT until asked.
	got, _ = runKcl(t, addr, "list-offsets", "lo-a:0")
	if header := strings.Fields(strings.SplitN(got, "\n", 2)[0]); !slices.Equal(header, []string{"BROKER", "TOPIC", "PARTITION", "START", "STABLE", "END", "ERROR"}) {
		t.Errorf("text header = %v", header)
	}
	got, _ = runKcl(t, addr, "list-offsets", "lo-a:0", "--with-epochs", "--at", "-1h")
	if header := strings.Fields(strings.SplitN(got, "\n", 2)[0]); !slices.Equal(header, listOffsetsHeaders) {
		t.Errorf("text header with flags = %v", header)
	}

	// Errors are rows, and the command exits 1.
	got, code = runKcl(t, addr, "list-offsets", "nosuch", "lo-a:9", "lo-a:0", "--format", "awk")
	if code != 1 {
		t.Fatalf("exit %d, want 1\n%s", code, got)
	}
	awk = awkRows(t, got, len(listOffsetsHeaders))
	if len(awk) != 3 {
		t.Fatalf("awk = %v", awk)
	}
	if awk[0][1] != "lo-a" || awk[0][10] != "-" || awk[1][2] != "9" || awk[1][10] == "-" || awk[2][1] != "nosuch" || awk[2][0] != "-" || !strings.Contains(awk[2][10], "UNKNOWN_TOPIC_OR_PARTITION") {
		t.Errorf("error rows = %v", awk)
	}
}

// TestListOffsetsAlias pins that the old "kcl misc list-offsets" still runs
// and names topic.list-offsets.
func TestListOffsetsAlias(t *testing.T) {
	_, addr := newCluster(t, 1, 1, "alias")
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	misc := &cobra.Command{Use: "misc"}
	alias := ListOffsetsCommand(cl)
	alias.Hidden = true
	misc.AddCommand(alias)
	root.AddCommand(misc)
	root.SetArgs([]string{"--no-config-file", "-B", addr, "misc", "list-offsets", "alias", "--format", "json"})
	got := captureStdout(t, func() {
		if err := root.Execute(); err != nil {
			t.Error(err)
		}
	})
	rows := rowsOf(t, jsonDoc(t, got, "topic.list-offsets"), "offsets")
	if len(rows) != 1 || rows[0]["end"] != float64(1) {
		t.Errorf("rows = %v", rows)
	}
}
