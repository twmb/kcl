package out

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"strconv"
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
// The command is the _command the JSON document carries, which every command
// takes from Client.Command rather than naming itself; see CommandName. The
// jsonKey parameter names the top-level array in JSON output (e.g., "groups"
// for group list). Headers are used for text column headers and are
// lowercased with hyphens/spaces replaced by underscores for JSON keys.
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
		strs := make([]string, len(row))
		for i, v := range row {
			strs[i] = fmt.Sprint(v)
		}
		fmt.Println(strings.Join(strs, "\t"))
	}
}

// Number is a number for a table cell whose value the cluster may not have
// reported. JSON gets the number itself, or null when there is none; text and
// awk get the digits, or the dash those columns already showed. Build one
// with Num, or use NoNum.
//
// A cell holding strconv.FormatInt of a number reaches JSON as a string, and
// "start":"0" sat beside "stable":5 in one list-offsets document. A cell
// holding the int64 itself is a JSON number, so reach for Number only where
// a dash is also possible.
type Number struct {
	v  int64
	ok bool
}

// Num is the table cell for v.
func Num[T int | int32 | int64](v T) Number { return Number{v: int64(v), ok: true} }

// NoNum is the table cell for a number the cluster did not report.
var NoNum Number

func (n Number) String() string {
	if !n.ok {
		return "-"
	}
	return strconv.FormatInt(n.v, 10)
}

func (n Number) MarshalJSON() ([]byte, error) {
	if !n.ok {
		return []byte("null"), nil
	}
	return strconv.AppendInt(nil, n.v, 10), nil
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
	if err := enc.Encode(v); err != nil {
		Die("unable to marshal JSON: %v", err)
	}
}
