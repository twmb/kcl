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

// Unknown is the table cell for a value we do not have: an offset the cluster
// did not report, or a flag's column when the flag is off. It prints as "-"
// in text and awk and as null in JSON.
//
// A known empty string is "" instead, the ERROR of a row that succeeded: awk
// prints it as "-" as well, and JSON keeps the "". 0 and false are values and
// print as themselves everywhere. Commands never write "-" into a cell; the
// awk and text writers do.
var Unknown unknown

type unknown struct{}

func (unknown) String() string               { return "-" }
func (unknown) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

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
// The command is the _command the JSON document carries, which every command
// takes from Client.Command rather than naming itself; see CommandName. The
// jsonKey parameter names the top-level array in JSON output (e.g., "groups"
// for group list). Headers are used for text column headers and are
// lowercased with hyphens/spaces replaced by underscores for JSON keys.
func NewFormattedTable(format, command string, version int, jsonKey string, headers ...string) *FormattedTable {
	keys := make([]string, len(headers))
	for i, h := range headers {
		keys[i] = jsonKeyOf(h)
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

func jsonKeyOf(header string) string {
	k := strings.ToLower(header)
	k = strings.ReplaceAll(k, " ", "_")
	k = strings.ReplaceAll(k, "-", "_")
	return k
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
			strs[i] = textCell(v)
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
	doc := map[string]any{
		"_version": t.version,
		t.jsonKey:  data,
	}
	if t.command != "" {
		doc["_command"] = t.command
	}
	writeJSON(doc)
}

func (t *FormattedTable) flushAWK() {
	for _, row := range t.rows {
		AwkRow(row...)
	}
}

// AwkRow prints one awk row, the values tab separated, with the cell rules a
// table follows under --format awk. A command that prints awk rows by hand
// uses this rather than fmt.Printf, so that the "-" for an empty cell is
// written in one place.
func AwkRow(values ...any) {
	strs := make([]string, len(values))
	for i, v := range values {
		strs[i] = awkCell(v)
	}
	fmt.Println(strings.Join(strs, "\t"))
}

// awkCell is the awk text of one cell. An awk row has no empty field: a cell
// that is Unknown, nil, or prints as "" is "-", so a script can count on every
// column being there.
func awkCell(v any) string {
	switch v := v.(type) {
	case nil, unknown:
		return "-"
	case string:
		if v == "" {
			return "-"
		}
		return v
	}
	if s := fmt.Sprint(v); s != "" {
		return s
	}
	return "-"
}

// textCell is the text of one cell: "-" for Unknown and nil, and otherwise
// what the value prints as, an empty string included.
func textCell(v any) string {
	switch v.(type) {
	case nil, unknown:
		return "-"
	}
	return fmt.Sprint(v)
}

// MarshalJSON outputs structured JSON with _command and _version metadata
// alongside arbitrary additional fields. Use this for commands with
// non-tabular or mixed output. Like an error document, this leaves _command
// out when we have no command to name, which is only the bare root.
func MarshalJSON(command string, version int, fields map[string]any) {
	output := make(map[string]any, len(fields)+2)
	if command != "" {
		output["_command"] = command
	}
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

// writeJSON writes v as one line. JSON output is for machines: a single line
// pipes into jq, greps, and captures into a shell variable, none of which a
// pretty printed value does. Pipe to jq if you want it wide.
func writeJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false) // topic names and error text are data, not HTML
	if err := enc.Encode(v); err != nil {
		Die("unable to marshal JSON: %v", err)
	}
}
