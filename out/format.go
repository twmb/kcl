package out

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"strings"
	"text/tabwriter"
)

const (
	FormatText = "text"
	FormatJSON = "json"
	FormatAWK  = "awk"
)

// FormattedTable buffers tabular output and can flush in text, json, or awk
// format. Use this for commands whose output is a single table.
type FormattedTable struct {
	format   string
	command  string
	version  int
	jsonKey  string
	headers  []string
	jsonKeys []string
	rows     [][]any
}

// NewFormattedTable creates a table that outputs in the specified format.
// The jsonKey parameter names the top-level array in JSON output (e.g.,
// "groups" for group list). Headers are used for text column headers
// and are lowercased with hyphens/spaces replaced by underscores for
// JSON keys.
func NewFormattedTable(format, command string, version int, jsonKey string, headers ...string) *FormattedTable {
	keys := make([]string, len(headers))
	for i, h := range headers {
		k := strings.ToLower(h)
		k = strings.ReplaceAll(k, " ", "_")
		k = strings.ReplaceAll(k, "-", "_")
		keys[i] = k
	}
	return &FormattedTable{
		format:   format,
		command:  command,
		version:  version,
		jsonKey:  jsonKey,
		headers:  headers,
		jsonKeys: keys,
	}
}

// Row adds a row of values to the table.
func (t *FormattedTable) Row(values ...any) {
	t.rows = append(t.rows, values)
}

// Flush writes the buffered data in the configured format to stdout.
func (t *FormattedTable) Flush() {
	switch t.format {
	case FormatJSON:
		t.flushJSON()
	case FormatAWK:
		t.flushAWK()
	default:
		t.flushText()
	}
}

func (t *FormattedTable) flushText() {
	tw := tabwriter.NewWriter(os.Stdout, 6, 4, 2, ' ', 0)
	fmt.Fprint(tw, strings.Join(t.headers, "\t")+"\n")
	for _, row := range t.rows {
		strs := make([]string, len(row))
		for i, v := range row {
			strs[i] = fmt.Sprint(v)
		}
		fmt.Fprint(tw, strings.Join(strs, "\t")+"\n")
	}
	tw.Flush()
}

func (t *FormattedTable) flushJSON() {
	data := make([]map[string]any, 0, len(t.rows))
	for _, row := range t.rows {
		m := make(map[string]any, len(t.jsonKeys))
		for j, key := range t.jsonKeys {
			if j < len(row) {
				m[key] = row[j]
			}
		}
		data = append(data, m)
	}
	writeJSON(map[string]any{
		"_command": t.command,
		"_version": t.version,
		t.jsonKey:  data,
	})
}

func (t *FormattedTable) flushAWK() {
	for _, row := range t.rows {
		strs := make([]string, len(row))
		for i, v := range row {
			strs[i] = fmt.Sprint(v)
		}
		fmt.Println(strings.Join(strs, "\t"))
	}
}

// MarshalJSON outputs structured JSON with _command and _version metadata
// alongside arbitrary additional fields. Use this for commands with
// non-tabular or mixed output.
func MarshalJSON(command string, version int, fields map[string]any) {
	output := make(map[string]any, len(fields)+2)
	output["_command"] = command
	output["_version"] = version
	maps.Copy(output, fields)
	writeJSON(output)
}

// DieJSON outputs a JSON error to stdout and exits with code 1.
func DieJSON(command string, errCode string, message string) {
	writeJSON(map[string]any{
		"_command": command,
		"error":    errCode,
		"message":  message,
	})
	os.Exit(1)
}

func writeJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		Die("unable to marshal JSON: %v", err)
	}
}
