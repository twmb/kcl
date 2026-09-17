package out

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func captureStdout(fn func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	buf.ReadFrom(r)
	return buf.String()
}

func TestFormattedTableText(t *testing.T) {
	output := captureStdout(func() {
		table := NewFormattedTable("text", "test.cmd", 1, "items",
			"NAME", "COUNT", "STATUS")
		table.Row("alpha", 10, "ok")
		table.Row("beta", 20, "error")
		table.Flush()
	})

	if !strings.Contains(output, "NAME") {
		t.Error("text output should contain headers")
	}
	if !strings.Contains(output, "alpha") {
		t.Error("text output should contain row data")
	}
	if !strings.Contains(output, "beta") {
		t.Error("text output should contain all rows")
	}

	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 lines (header + 2 rows), got %d", len(lines))
	}
}

func TestFormattedTableJSON(t *testing.T) {
	output := captureStdout(func() {
		table := NewFormattedTable("json", "group.list", 1, "groups",
			"BROKER", "GROUP-ID", "STATE")
		table.Row(1, "mygroup", "Stable")
		table.Row(2, "other", "Empty")
		table.Flush()
	})

	var result map[string]any
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("JSON output should be valid JSON: %v\noutput: %s", err, output)
	}

	if result["_command"] != "group.list" {
		t.Errorf("_command = %v, want group.list", result["_command"])
	}
	if result["_version"] != float64(1) {
		t.Errorf("_version = %v, want 1", result["_version"])
	}

	groups, ok := result["groups"].([]any)
	if !ok {
		t.Fatalf("groups field missing or wrong type")
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}

	first := groups[0].(map[string]any)
	if first["broker"] != float64(1) {
		t.Errorf("first group broker = %v, want 1", first["broker"])
	}
	if first["group_id"] != "mygroup" {
		t.Errorf("first group group_id = %v, want mygroup", first["group_id"])
	}
	if first["state"] != "Stable" {
		t.Errorf("first group state = %v, want Stable", first["state"])
	}
}

// JSON output is a single line so it pipes into jq and line tools.
func TestJSONIsOneLine(t *testing.T) {
	outputs := []string{
		captureStdout(func() {
			table := NewFormattedTable("json", "test.cmd", 1, "items", "NAME", "COUNT")
			table.Row("a", 1)
			table.Row("b", 2)
			table.Flush()
		}),
		captureStdout(func() {
			MarshalJSON("test.cmd", 1, map[string]any{"nested": map[string]any{"a": []int{1, 2}}})
		}),
	}
	for _, output := range outputs {
		if n := strings.Count(output, "\n"); n != 1 {
			t.Errorf("output has %d newlines, want 1 (trailing):\n%s", n, output)
		}
		if strings.Contains(output, "\n  ") {
			t.Errorf("output is indented:\n%s", output)
		}
		var v any
		if err := json.Unmarshal([]byte(output), &v); err != nil {
			t.Errorf("output is not valid JSON: %v\n%s", err, output)
		}
	}
}

func TestFormattedTableAWK(t *testing.T) {
	output := captureStdout(func() {
		table := NewFormattedTable("awk", "test.cmd", 1, "items",
			"NAME", "COUNT", "STATUS")
		table.Row("alpha", 10, "ok")
		table.Row("beta", 20, "error")
		table.Flush()
	})

	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 2 {
		t.Errorf("awk output should have 2 rows (no header), got %d: %q", len(lines), output)
	}

	// Verify tab-separated
	fields := strings.Split(lines[0], "\t")
	if len(fields) != 3 {
		t.Errorf("expected 3 tab-separated fields, got %d: %q", len(fields), lines[0])
	}
	if fields[0] != "alpha" || fields[1] != "10" || fields[2] != "ok" {
		t.Errorf("unexpected fields: %v", fields)
	}
}

func TestFormattedTableEmpty(t *testing.T) {
	// Text with no rows should still print headers.
	output := captureStdout(func() {
		table := NewFormattedTable("text", "test.cmd", 1, "items", "A", "B")
		table.Flush()
	})
	if !strings.Contains(output, "A") {
		t.Error("text output with no rows should still print headers")
	}

	// JSON with no rows should produce empty array.
	output = captureStdout(func() {
		table := NewFormattedTable("json", "test.cmd", 1, "items", "A", "B")
		table.Flush()
	})
	var result map[string]any
	json.Unmarshal([]byte(output), &result)
	items := result["items"].([]any)
	if len(items) != 0 {
		t.Errorf("expected empty array, got %d items", len(items))
	}

	// AWK with no rows should produce empty output.
	output = captureStdout(func() {
		table := NewFormattedTable("awk", "test.cmd", 1, "items", "A", "B")
		table.Flush()
	})
	if strings.TrimSpace(output) != "" {
		t.Errorf("awk output with no rows should be empty, got %q", output)
	}
}

func TestFormattedTableJSONKeyConversion(t *testing.T) {
	output := captureStdout(func() {
		table := NewFormattedTable("json", "test.cmd", 1, "data",
			"CURRENT-OFFSET", "LOG END OFFSET", "MEMBER_ID")
		table.Row(100, 200, "m-1")
		table.Flush()
	})

	var result map[string]any
	json.Unmarshal([]byte(output), &result)
	data := result["data"].([]any)
	row := data[0].(map[string]any)

	// Hyphens and spaces in headers become underscores in JSON keys.
	if _, ok := row["current_offset"]; !ok {
		t.Errorf("expected key current_offset, got keys: %v", row)
	}
	if _, ok := row["log_end_offset"]; !ok {
		t.Errorf("expected key log_end_offset, got keys: %v", row)
	}
	if _, ok := row["member_id"]; !ok {
		t.Errorf("expected key member_id, got keys: %v", row)
	}
}

func TestMarshalJSON(t *testing.T) {
	output := captureStdout(func() {
		MarshalJSON("cluster.describe", 1, map[string]any{
			"cluster_id":    "abc-123",
			"controller_id": 1,
			"brokers":       []string{"kafka-1", "kafka-2"},
		})
	})

	var result map[string]any
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if result["_command"] != "cluster.describe" {
		t.Errorf("_command = %v", result["_command"])
	}
	if result["_version"] != float64(1) {
		t.Errorf("_version = %v", result["_version"])
	}
	if result["cluster_id"] != "abc-123" {
		t.Errorf("cluster_id = %v", result["cluster_id"])
	}
}

func TestFormattedTableJSONTypesPreserved(t *testing.T) {
	output := captureStdout(func() {
		table := NewFormattedTable("json", "test.cmd", 1, "data", "NAME", "COUNT", "ACTIVE")
		table.Row("alpha", 42, true)
		table.Row("beta", 0, false)
		table.Flush()
	})

	var result map[string]any
	json.Unmarshal([]byte(output), &result)
	data := result["data"].([]any)

	first := data[0].(map[string]any)
	// JSON encoding preserves Go types: an int reads back as a float64, a
	// bool as a bool, a string as a string.
	if first["name"] != "alpha" {
		t.Errorf("name = %v", first["name"])
	}
	if first["count"] != float64(42) {
		t.Errorf("count = %v (type %T)", first["count"], first["count"])
	}
	if first["active"] != true {
		t.Errorf("active = %v", first["active"])
	}

	second := data[1].(map[string]any)
	if second["active"] != false {
		t.Errorf("second active = %v", second["active"])
	}
}

func TestFormattedTableAWKNoHeaders(t *testing.T) {
	output := captureStdout(func() {
		table := NewFormattedTable("awk", "test.cmd", 1, "data", "HEADER1", "HEADER2")
		table.Row("a", "b")
		table.Flush()
	})

	// AWK output must NOT contain header names.
	if strings.Contains(output, "HEADER") {
		t.Error("awk output should not contain headers")
	}
	if strings.TrimSpace(output) != "a\tb" {
		t.Errorf("awk output = %q, want %q", strings.TrimSpace(output), "a\tb")
	}
}

func TestFormattedTableTextAlignment(t *testing.T) {
	output := captureStdout(func() {
		table := NewFormattedTable("text", "test.cmd", 1, "data", "SHORT", "LONG HEADER")
		table.Row("x", "y")
		table.Flush()
	})

	// Headers should be present and the output should have at least 2 lines.
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 2 {
		t.Errorf("expected 2 lines, got %d: %q", len(lines), output)
	}
	if !strings.Contains(lines[0], "SHORT") || !strings.Contains(lines[0], "LONG HEADER") {
		t.Errorf("header line missing expected columns: %q", lines[0])
	}
}

// TestEmptyCommandOmitted pins that a document with no command to name leaves
// _command out rather than carrying an empty one, the rule ErrorDoc follows.
// Only the bare root has no command.
func TestEmptyCommandOmitted(t *testing.T) {
	for _, test := range []struct {
		name   string
		output string
	}{
		{"table", captureStdout(func() {
			table := NewFormattedTable("json", "", 1, "keys", "KEY")
			table.Row("seed_brokers")
			table.Flush()
		})},
		{"MarshalJSON", captureStdout(func() {
			MarshalJSON("", 1, map[string]any{"profile": ""})
		})},
	} {
		t.Run(test.name, func(t *testing.T) {
			var result map[string]any
			if err := json.Unmarshal([]byte(test.output), &result); err != nil {
				t.Fatalf("Unmarshal: %v: %s", err, test.output)
			}
			if _, ok := result["_command"]; ok {
				t.Errorf("_command is present: %s", test.output)
			}
			if result["_version"] != float64(1) {
				t.Errorf("_version = %v", result["_version"])
			}
		})
	}
}

// TestCellRules pins how one cell prints per format: Unknown is "-" in text
// and awk and null in JSON; "" is "-" in awk only; 0 and false are values.
func TestCellRules(t *testing.T) {
	for _, test := range []struct {
		name     string
		cell     any
		text     string
		awk      string
		jsonText string
	}{
		{"unknown", Unknown, "-", "-", "null"},
		{"nil", nil, "-", "-", "null"},
		{"empty string", "", "", "-", `""`},
		{"string", "x", "x", "x", `"x"`},
		{"zero", 0, "0", "0", "0"},
		{"false", false, "false", "false", "false"},
		{"int64", int64(-1), "-1", "-1", "-1"},
		{"int32 slice", []int32{0, 1, 2}, "0,1,2", "0,1,2", "[0,1,2]"},
		{"int64 slice", []int64{5}, "5", "5", "[5]"},
		{"string slice", []string{"a", "b"}, "a,b", "a,b", `["a","b"]`},
		{"any slice", []any{"a", 1, nil}, "a,1,-", "a,1,-", `["a",1,null]`},
		{"empty slice", []int32{}, "", "-", "[]"},
		{"nil string pointer", (*string)(nil), "-", "-", "null"},
		{"nil int64 pointer", (*int64)(nil), "-", "-", "null"},
		{"string pointer", ptr("x"), "x", "x", `"x"`},
		{"int64 pointer", ptr(int64(7)), "7", "7", "7"},
		{"empty string pointer", ptr(""), "", "-", `""`},
		{"bytes", []byte("raw"), "cmF3", "cmF3", `"cmF3"`},
		{"bytes with a tab", []byte("a\tb\n"), "YQliCg==", "YQliCg==", `"YQliCg=="`},
		{"stringer", stringer("s"), "s", "s", `"s"`},
		{"kmsg enum", kmsg.ConfigSourceDynamicTopicConfig, "DYNAMIC_TOPIC_CONFIG", "DYNAMIC_TOPIC_CONFIG", `"DYNAMIC_TOPIC_CONFIG"`},
		{"duration", 90 * time.Second, "1m30s", "1m30s", `"1m30s"`},
		{"marshaler wins over stringer", marshaler{}, "s", "s", `{"m":1}`},
		{"nil stringer pointer", (*marshaler)(nil), "-", "-", "null"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := textCell(test.cell); got != test.text {
				t.Errorf("text = %q, want %q", got, test.text)
			}
			if got := awkCell(test.cell); got != test.awk {
				t.Errorf("awk = %q, want %q", got, test.awk)
			}
			raw, err := json.Marshal(jsonCell(test.cell))
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != test.jsonText {
				t.Errorf("json = %s, want %s", raw, test.jsonText)
			}
		})
	}
}

// marshaler is both a Stringer and a json.Marshaler; JSON is what it
// marshals, not its String.
type marshaler struct{}

func (marshaler) String() string               { return "s" }
func (marshaler) MarshalJSON() ([]byte, error) { return []byte(`{"m":1}`), nil }

func ptr[T any](v T) *T { return &v }

type stringer string

func (s stringer) String() string { return string(s) }

// TestSliceInTable pins that a list cell is one awk field with no brackets,
// the same in text, and an array in JSON, with a nil slice printed as [].
func TestSliceInTable(t *testing.T) {
	rows := func(format string) string {
		return captureStdout(func() {
			table := NewFormattedTable(format, "topic.describe", 1, "partitions", "PARTITION", "REPLICAS", "ISR", "OFFLINE-REPLICAS", "NIL")
			table.Row(0, []int32{0, 1, 2}, []int32{1}, []int32{}, []int32(nil))
			table.Flush()
		})
	}
	if got, exp := rows("awk"), "0\t0,1,2\t1\t-\t-\n"; got != exp {
		t.Errorf("awk = %q, want %q", got, exp)
	}
	if got := rows("text"); !strings.Contains(got, "0          0,1,2     1") || strings.Contains(got, "[") {
		t.Errorf("text = %q", got)
	}
	got := rows("json")
	for _, want := range []string{`"replicas":[0,1,2]`, `"isr":[1]`, `"offline_replicas":[]`, `"nil":[]`} {
		if !strings.Contains(got, want) {
			t.Errorf("json = %s, want %s", got, want)
		}
	}
}

// TestUnknownInTable drives the cell rules through the three writers, and
// pins that the awk dash never reaches JSON: the "" a command wrote is the ""
// JSON prints.
func TestUnknownInTable(t *testing.T) {
	rows := func(format string) string {
		return captureStdout(func() {
			table := NewFormattedTable(format, "test.cmd", 1, "data", "NAME", "SIZE", "LAG", "ERROR", "N", "B")
			table.Row("alpha", int64(395), Unknown, "", 0, false)
			table.Flush()
		})
	}
	if got, exp := rows("awk"), "alpha\t395\t-\t-\t0\tfalse\n"; got != exp {
		t.Errorf("awk = %q, want %q", got, exp)
	}
	if got := rows("text"); !strings.Contains(got, "alpha  395   -            0     false") {
		t.Errorf("text = %q", got)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(rows("json")), &result); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	first := result["data"].([]any)[0].(map[string]any)
	if first["size"] != float64(395) || first["lag"] != nil || first["error"] != "" || first["n"] != float64(0) || first["b"] != false {
		t.Errorf("json row = %v", first)
	}
	if _, ok := first["lag"]; !ok {
		t.Error("lag is absent, want null")
	}
}

func TestAwkRow(t *testing.T) {
	got := captureStdout(func() { AwkRow("topic", 0, "", Unknown, nil, false) })
	if want := "topic\t0\t-\t-\t-\tfalse\n"; got != want {
		t.Errorf("AwkRow = %q, want %q", got, want)
	}
}

func TestWithKeys(t *testing.T) {
	output := captureStdout(func() {
		table := NewFormattedTable("json", "group.describe", 1, "groups", "GROUP", "STATE", "MEMBERS", "PARTITIONS", "LAG").
			WithKeys(map[string]string{"MEMBERS": "member_count", "PARTITIONS": "partition_count", "LAG": "total_lag"})
		table.Row("g", "Stable", 2, 4, 10)
		table.Flush()
	})
	var result map[string]any
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("Unmarshal: %v: %s", err, output)
	}
	row := result["groups"].([]any)[0].(map[string]any)
	for key, want := range map[string]any{"group": "g", "state": "Stable", "member_count": float64(2), "partition_count": float64(4), "total_lag": float64(10)} {
		if row[key] != want {
			t.Errorf("%s = %v, want %v", key, row[key], want)
		}
	}
	for _, gone := range []string{"members", "partitions", "lag"} {
		if _, ok := row[gone]; ok {
			t.Errorf("derived key %q is still present: %v", gone, row)
		}
	}

	defer func() {
		if recover() == nil {
			t.Error("WithKeys with a header the table lacks did not panic")
		}
	}()
	NewFormattedTable("json", "x", 1, "rows", "A").WithKeys(map[string]string{"B": "b"})
}

// TestResultColumns pins the result shape: text prints OK for a "" ERROR,
// awk prints "-", JSON keeps "", and Flush returns ErrSilent only when a row
// carries an error. An Unknown ERROR, a plan row not yet run, is no error.
func TestResultColumns(t *testing.T) {
	for _, test := range []struct {
		name    string
		headers []string
		rows    [][]any
		wantErr bool
		text    string
		awk     string
	}{
		{
			name:    "all ok",
			headers: []string{"TOPIC", "ERROR", "MESSAGE"},
			rows:    [][]any{{"a", "", ""}, {"b", "", ""}},
			text:    "TOPIC  ERROR  MESSAGE\na      OK     \nb      OK     \n",
			awk:     "a\t-\t-\nb\t-\t-\n",
		},
		{
			name:    "one error",
			headers: []string{"TOPIC", "ERROR", "MESSAGE"},
			rows:    [][]any{{"a", "", ""}, {"b", "UNKNOWN_TOPIC_OR_PARTITION", "no such topic"}},
			wantErr: true,
			text:    "TOPIC  ERROR                       MESSAGE\na      OK                          \nb      UNKNOWN_TOPIC_OR_PARTITION  no such topic\n",
			awk:     "a\t-\t-\nb\tUNKNOWN_TOPIC_OR_PARTITION\tno such topic\n",
		},
		{
			name:    "unknown is not an error",
			headers: []string{"TOPIC", "ERROR", "MESSAGE"},
			rows:    [][]any{{"a", Unknown, Unknown}},
			text:    "TOPIC  ERROR  MESSAGE\na      -      -\n",
			awk:     "a\t-\t-\n",
		},
		{
			name:    "error alone",
			headers: []string{"TOPIC", "ERROR"},
			rows:    [][]any{{"a", ""}, {"b", "boom"}},
			wantErr: true,
			text:    "TOPIC  ERROR\na      OK\nb      boom\n",
			awk:     "a\t-\nb\tboom\n",
		},
		{
			name:    "no rows",
			headers: []string{"TOPIC", "ERROR", "MESSAGE"},
			text:    "TOPIC  ERROR  MESSAGE\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, format := range []string{"text", "awk", "json"} {
				var err error
				got := captureStdout(func() {
					table := NewFormattedTable(format, "topic.delete", 1, "results", test.headers...).ResultColumns()
					for _, row := range test.rows {
						table.Row(row...)
					}
					err = table.Flush()
				})
				if (err == ErrSilent) != test.wantErr || (err != nil && err != ErrSilent) {
					t.Errorf("%s: Flush = %v, want ErrSilent %v", format, err, test.wantErr)
				}
				switch format {
				case "text":
					if got != test.text {
						t.Errorf("text = %q, want %q", got, test.text)
					}
				case "awk":
					if got != test.awk {
						t.Errorf("awk = %q, want %q", got, test.awk)
					}
				case "json":
					var doc map[string]any
					if err := json.Unmarshal([]byte(got), &doc); err != nil {
						t.Fatalf("json: %v: %s", err, got)
					}
					for i, row := range doc["results"].([]any) {
						cell := row.(map[string]any)["error"]
						switch want := test.rows[i][1]; want {
						case Unknown:
							if cell != nil {
								t.Errorf("row %d error = %v, want null", i, cell)
							}
						default:
							if cell != want {
								t.Errorf("row %d error = %v, want %v", i, cell, want)
							}
						}
					}
				}
			}
		})
	}

	defer func() {
		if recover() == nil {
			t.Error("ResultColumns on a table that does not end in ERROR did not panic")
		}
	}()
	NewFormattedTable("json", "x", 1, "rows", "ERROR", "TOPIC").ResultColumns()
}

// TestErrorColumn pins the read-only variant: a "" ERROR prints as nothing in
// text rather than OK, "-" in awk, "" in JSON, and Flush still returns
// ErrSilent when a row carries an error.
func TestErrorColumn(t *testing.T) {
	rows := [][]any{{"a", "", ""}, {"b", "NOT_LEADER_FOR_PARTITION", "moved"}}
	for _, test := range []struct {
		format string
		want   string
	}{
		{"text", "TOPIC  ERROR                     MESSAGE\na                                \nb      NOT_LEADER_FOR_PARTITION  moved\n"},
		{"awk", "a\t-\t-\nb\tNOT_LEADER_FOR_PARTITION\tmoved\n"},
	} {
		var err error
		got := captureStdout(func() {
			table := NewFormattedTable(test.format, "txn.list", 1, "rows", "TOPIC", "ERROR", "MESSAGE").ErrorColumn()
			for _, row := range rows {
				table.Row(row...)
			}
			err = table.Flush()
		})
		if err != ErrSilent {
			t.Errorf("%s: Flush = %v, want ErrSilent", test.format, err)
		}
		if got != test.want {
			t.Errorf("%s = %q, want %q", test.format, got, test.want)
		}
	}
	var err error
	got := captureStdout(func() {
		table := NewFormattedTable("json", "txn.list", 1, "rows", "TOPIC", "ERROR").ErrorColumn()
		table.Row("a", "")
		err = table.Flush()
	})
	if err != nil || !strings.Contains(got, `"error":""`) {
		t.Errorf("json: Flush = %v, out = %q", err, got)
	}
}

// TestErrorCellTypes pins that a ResultColumns or ErrorColumn table's ERROR
// and MESSAGE cells are strings by the time they print, whatever a command
// handed Row: an error is its kerr name or its text, a Stringer its String,
// a *string what it points to, and only a non-empty string is a failure. A
// type Row cannot turn into a string panics under go test.
func TestErrorCellTypes(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     any
		msg     any
		wantErr bool
		awk     string
		json    string
	}{
		{"strings", "UNKNOWN_TOPIC_OR_PARTITION", "gone", true, "a\tUNKNOWN_TOPIC_OR_PARTITION\tgone\n", `"error":"UNKNOWN_TOPIC_OR_PARTITION","message":"gone"`},
		{"kerr", kerr.UnknownTopicOrPartition, ptr("gone"), true, "a\tUNKNOWN_TOPIC_OR_PARTITION\tgone\n", `"error":"UNKNOWN_TOPIC_OR_PARTITION","message":"gone"`},
		{"wrapped kerr", fmt.Errorf("commit: %w", kerr.NotCoordinator), "", true, "a\tNOT_COORDINATOR\t-\n", `"error":"NOT_COORDINATOR","message":""`},
		{"other error", errors.New("dial tcp: refused"), (*string)(nil), true, "a\tdial tcp: refused\t-\n", `"error":"dial tcp: refused","message":""`},
		{"stringer", stringer("boom"), "", true, "a\tboom\t-\n", `"error":"boom","message":""`},
		{"unknown", Unknown, Unknown, false, "a\t-\t-\n", `"error":null,"message":null`},
		{"nil", nil, nil, false, "a\t-\t-\n", `"error":null,"message":null`},
		{"nil kerr pointer", (*kerr.Error)(nil), (*int64)(nil), false, "a\t-\t-\n", `"error":null,"message":null`},
		{"enum ERROR", kmsg.ConfigSourceDefaultConfig, "", true, "a\tDEFAULT_CONFIG\t-\n", `"error":"DEFAULT_CONFIG","message":""`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var err error
			awk := captureStdout(func() {
				table := NewFormattedTable("awk", "topic.delete", 1, "results", "TOPIC", "ERROR", "MESSAGE").ResultColumns()
				table.Row("a", test.err, test.msg)
				err = table.Flush()
			})
			if (err == ErrSilent) != test.wantErr || (err != nil && err != ErrSilent) {
				t.Errorf("Flush = %v, want ErrSilent %v", err, test.wantErr)
			}
			if awk != test.awk {
				t.Errorf("awk = %q, want %q", awk, test.awk)
			}
			js := captureStdout(func() {
				table := NewFormattedTable("json", "topic.delete", 1, "results", "TOPIC", "ERROR", "MESSAGE").ResultColumns()
				table.Row("a", test.err, test.msg)
				table.Flush()
			})
			if !strings.Contains(js, test.json) {
				t.Errorf("json = %s, want %s", js, test.json)
			}
		})
	}

	// The ERROR column alone is normalized the same way, and a row that the
	// caller reuses is not written to.
	row := []any{"a", kerr.NotController}
	var err error
	awk := captureStdout(func() {
		table := NewFormattedTable("awk", "x", 1, "rows", "TOPIC", "ERROR").ErrorColumn()
		table.Row(row...)
		err = table.Flush()
	})
	if err != ErrSilent || awk != "a\tNOT_CONTROLLER\n" {
		t.Errorf("ERROR alone: Flush = %v, awk = %q", err, awk)
	}
	if row[1] != kerr.NotController {
		t.Errorf("Row wrote %v into the caller's slice", row[1])
	}

	for _, test := range []struct {
		name string
		row  []any
	}{
		{"int16 ERROR", []any{"a", int16(0), ""}},
		{"bool ERROR", []any{"a", false, ""}},
		{"error MESSAGE", []any{"a", "", errors.New("x")}},
		{"int MESSAGE", []any{"a", "", 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("Row(%v) did not panic", test.row)
				}
			}()
			NewFormattedTable("awk", "x", 1, "rows", "TOPIC", "ERROR", "MESSAGE").ResultColumns().Row(test.row...)
		})
	}
}

// TestDryRun pins the one phrasing: "dry_run":true at the top level of a JSON
// document, the text line first, and nothing at all in awk.
func TestDryRun(t *testing.T) {
	table := func(format string, dry bool) string {
		return captureStdout(func() {
			table := NewFormattedTable(format, "topic.delete", 1, "results", "TOPIC", "ERROR", "MESSAGE").ResultColumns()
			table.SetDryRun(dry)
			table.Row("a", "", "")
			table.Flush()
		})
	}
	if got := table("text", true); got != "Dry run: nothing was changed.\nTOPIC  ERROR  MESSAGE\na      OK     \n" {
		t.Errorf("text = %q", got)
	}
	if got := table("text", false); strings.Contains(got, "Dry run") {
		t.Errorf("text without dry run = %q", got)
	}
	if got := table("awk", true); got != "a\t-\t-\n" {
		t.Errorf("awk = %q", got)
	}
	for _, test := range []struct {
		name string
		out  string
		want any
	}{
		{"table dry", table("json", true), true},
		{"table real", table("json", false), nil},
		{"MarshalJSON dry", captureStdout(func() { MarshalJSON("group.seek", 1, map[string]any{"group": "g"}, DryRun(true)) }), true},
		{"MarshalJSON real", captureStdout(func() { MarshalJSON("group.seek", 1, map[string]any{"group": "g"}, DryRun(false)) }), nil},
		{"MarshalJSON no opt", captureStdout(func() { MarshalJSON("group.seek", 1, map[string]any{"group": "g"}) }), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			var doc map[string]any
			if err := json.Unmarshal([]byte(test.out), &doc); err != nil {
				t.Fatalf("json: %v: %s", err, test.out)
			}
			got, ok := doc["dry_run"]
			if test.want == nil && ok {
				t.Errorf("dry_run is %v, want absent", got)
			}
			if test.want != nil && got != test.want {
				t.Errorf("dry_run = %v, want %v", got, test.want)
			}
		})
	}
}

// TestRowWidth pins that a row must have one cell per header: under go test
// a short or long row panics, since a script counts on every column.
func TestRowWidth(t *testing.T) {
	for _, row := range [][]any{{"a"}, {"a", "b", "c"}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Row(%v) under two headers did not panic", row)
				}
			}()
			NewFormattedTable("awk", "x", 1, "rows", "A", "B").Row(row...)
		}()
	}
	NewFormattedTable("awk", "x", 1, "rows", "A", "B").Row("a", "b")
}
