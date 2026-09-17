package out

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
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
	dryRun   bool
	errCol   int // the ERROR column under ResultColumns, else -1
}

// NewFormattedTable creates a table that outputs in the specified format.
// The command is the _command the JSON document carries, which every command
// takes from Client.Command rather than naming itself; see CommandName. The
// jsonKey parameter names the top-level array in JSON output (e.g., "groups"
// for group list). Headers are used for text column headers and are
// lowercased with hyphens/spaces replaced by underscores for JSON keys; see
// WithKeys for the tables where a key must differ from its header.
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
		errCol:   -1,
	}
}

func jsonKeyOf(header string) string {
	k := strings.ToLower(header)
	k = strings.ReplaceAll(k, " ", "_")
	k = strings.ReplaceAll(k, "-", "_")
	return k
}

// WithKeys overrides the JSON key a header derives, for the tables where the
// two must differ: group describe --by group prints MEMBERS, PARTITIONS, and
// LAG under member_count, partition_count, and total_lag. The map is header
// to key. A header the table does not have is a programming error and
// panics.
func (t *FormattedTable) WithKeys(keys map[string]string) *FormattedTable {
	for header, key := range keys {
		i := slices.Index(t.headers, header)
		if i < 0 {
			panic(fmt.Sprintf("out: WithKeys names header %q, which the table does not have: %v", header, t.headers))
		}
		t.jsonKeys[i] = key
	}
	return t
}

// ResultColumns declares that the table's last two columns are ERROR and then
// MESSAGE, the per-item result of a mutating command, so that no command
// checks its own results. A row's ERROR is "" on success and the error name
// otherwise; text prints OK for the "", awk prints "-", and JSON keeps the "".
// Flush returns ErrSilent when any row's ERROR is set, so the command exits 1
// after printing every result. A table that ends in ERROR alone may declare
// this too. Any other shape is a programming error and panics.
func (t *FormattedTable) ResultColumns() *FormattedTable {
	n := len(t.headers)
	switch {
	case n >= 2 && t.headers[n-2] == "ERROR" && t.headers[n-1] == "MESSAGE":
		t.errCol = n - 2
	case n >= 1 && t.headers[n-1] == "ERROR":
		t.errCol = n - 1
	default:
		panic(fmt.Sprintf("out: ResultColumns needs the headers to end in ERROR or ERROR, MESSAGE: %v", t.headers))
	}
	return t
}

// SetDryRun marks the table as the output of a dry run: JSON carries
// "dry_run":true at the top level and text opens with the line PrintDryRun
// prints. awk is unchanged.
func (t *FormattedTable) SetDryRun(dry bool) {
	t.dryRun = dry
}

// Row adds a row of values to the table.
func (t *FormattedTable) Row(values ...any) {
	t.rows = append(t.rows, values)
}

// Flush writes the buffered data in the configured format to stdout. It
// returns ErrSilent when the table declares ResultColumns and a row's ERROR is
// set, and nil otherwise, so a command ends with "return table.Flush()".
func (t *FormattedTable) Flush() error {
	switch t.format {
	case FormatJSON:
		t.flushJSON()
	case FormatAWK:
		t.flushAWK()
	default:
		t.flushText()
	}
	if t.errCol >= 0 {
		for _, row := range t.rows {
			if t.errCol < len(row) && isError(row[t.errCol]) {
				return ErrSilent
			}
		}
	}
	return nil
}

// isError reports whether an ERROR cell names an error. Unknown and "" do not.
func isError(v any) bool {
	switch v := v.(type) {
	case nil, unknown:
		return false
	case string:
		return v != ""
	}
	return fmt.Sprint(v) != ""
}

func (t *FormattedTable) flushText() {
	if t.dryRun {
		PrintDryRun()
	}
	tw := tabwriter.NewWriter(os.Stdout, 6, 4, 2, ' ', 0)
	fmt.Fprint(tw, strings.Join(t.headers, "\t")+"\n")
	for _, row := range t.rows {
		strs := make([]string, len(row))
		for i, v := range row {
			if i == t.errCol && v == "" {
				strs[i] = "OK"
				continue
			}
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
	if t.dryRun {
		doc[dryRunKey] = true
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

// dryRunKey is the top-level JSON key a dry run carries, from both a table
// under SetDryRun and MarshalJSON with DryRun.
const dryRunKey = "dry_run"

// PrintDryRun prints the text line that says a dry run changed nothing. A
// table prints it itself under SetDryRun; a command that prints text of its
// own calls this once, before its output.
func PrintDryRun() {
	fmt.Println("Dry run: nothing was changed.")
}

// Opt shapes the document MarshalJSON prints.
type Opt func(doc map[string]any)

// DryRun marks the document as the output of a dry run, adding "dry_run":true
// at the top level when dry is true. It is the key a table adds under
// SetDryRun.
func DryRun(dry bool) Opt {
	return func(doc map[string]any) {
		if dry {
			doc[dryRunKey] = true
		}
	}
}

// MarshalJSON outputs structured JSON with _command and _version metadata
// alongside arbitrary additional fields. Use this for commands with
// non-tabular or mixed output. Like an error document, this leaves _command
// out when we have no command to name, which is only the bare root.
func MarshalJSON(command string, version int, fields map[string]any, opts ...Opt) {
	output := make(map[string]any, len(fields)+3)
	if command != "" {
		output["_command"] = command
	}
	output["_version"] = version
	maps.Copy(output, fields)
	for _, opt := range opts {
		opt(output)
	}
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
