package out

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
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

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("JSON output should be valid JSON: %v\noutput: %s", err, output)
	}

	if result["_command"] != "group.list" {
		t.Errorf("_command = %v, want group.list", result["_command"])
	}
	if result["_version"] != float64(1) {
		t.Errorf("_version = %v, want 1", result["_version"])
	}

	groups, ok := result["groups"].([]interface{})
	if !ok {
		t.Fatalf("groups field missing or wrong type")
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}

	first := groups[0].(map[string]interface{})
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
	var result map[string]interface{}
	json.Unmarshal([]byte(output), &result)
	items := result["items"].([]interface{})
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

	var result map[string]interface{}
	json.Unmarshal([]byte(output), &result)
	data := result["data"].([]interface{})
	row := data[0].(map[string]interface{})

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
		MarshalJSON("cluster.describe", 1, map[string]interface{}{
			"cluster_id":    "abc-123",
			"controller_id": 1,
			"brokers":       []string{"kafka-1", "kafka-2"},
		})
	})

	var result map[string]interface{}
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

	var result map[string]interface{}
	json.Unmarshal([]byte(output), &result)
	data := result["data"].([]interface{})

	first := data[0].(map[string]interface{})
	// JSON encoding preserves Go types: int→float64, bool→bool, string→string.
	if first["name"] != "alpha" {
		t.Errorf("name = %v", first["name"])
	}
	if first["count"] != float64(42) {
		t.Errorf("count = %v (type %T)", first["count"], first["count"])
	}
	if first["active"] != true {
		t.Errorf("active = %v", first["active"])
	}

	second := data[1].(map[string]interface{})
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

func TestDieJSON(t *testing.T) {
	// DieJSON calls os.Exit, so we can't test it directly.
	// But we can test the JSON output it would produce via writeJSON.
	output := captureStdout(func() {
		writeJSON(map[string]interface{}{
			"_command": "test.cmd",
			"error":    "NOT_FOUND",
			"message":  "resource not found",
		})
	})

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if result["error"] != "NOT_FOUND" {
		t.Errorf("error = %v", result["error"])
	}
	if result["message"] != "resource not found" {
		t.Errorf("message = %v", result["message"])
	}
}
