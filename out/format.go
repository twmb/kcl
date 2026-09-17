package out

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"text/tabwriter"
)

const (
	FormatText = "text"
	FormatJSON = "json"
	FormatAWK  = "awk"

	// FormatAwkHeader is the --format that prints the command's awk header
	// row and exits, before the command runs; see Columns.
	FormatAwkHeader = "awk-header"
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
	errCol   int  // the ERROR column under ResultColumns or ErrorColumn, else -1
	msgCol   int  // the MESSAGE column after errCol, else -1
	okText   bool // ResultColumns: text prints OK for a "" ERROR
}

// NewFormattedTable creates a table that outputs in the specified format.
// The command is the _command the JSON document carries, which every command
// takes from Client.Command rather than naming itself; see CommandName. The
// jsonKey parameter names the top-level array in JSON output (e.g., "groups"
// for group list). Headers are used for text column headers and are
// lowercased with hyphens/spaces replaced by underscores for JSON keys; see
// WithKeys for the tables where a key must differ from its header.
//
// Under the awk format the headers are checked against what the running
// command registered with Columns.
func NewFormattedTable(format, command string, version int, jsonKey string, headers ...string) *FormattedTable {
	if format == FormatAWK {
		checkColumns(command, headers)
	}
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
		msgCol:   -1,
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
	t.ErrorColumn()
	t.okText = true
	return t
}

// ErrorColumn is ResultColumns for a table that describes rather than
// changes: logdirs describe, txn list, user list. A row's ERROR is "" when
// the broker answered it and the error name otherwise, and Flush returns
// ErrSilent when any is set, but text prints nothing for the "" rather than
// OK, since nothing was done. awk prints "-" and JSON keeps the "".
func (t *FormattedTable) ErrorColumn() *FormattedTable {
	n := len(t.headers)
	switch {
	case n >= 2 && t.headers[n-2] == "ERROR" && t.headers[n-1] == "MESSAGE":
		t.errCol, t.msgCol = n-2, n-1
	case n >= 1 && t.headers[n-1] == "ERROR":
		t.errCol = n - 1
	default:
		panic(fmt.Sprintf("out: ErrorColumn needs the headers to end in ERROR or ERROR, MESSAGE: %v", t.headers))
	}
	return t
}

// SetDryRun marks the table as the output of a dry run: JSON carries
// "dry_run":true at the top level and text opens with the line PrintDryRun
// prints. awk is unchanged.
func (t *FormattedTable) SetDryRun(dry bool) {
	t.dryRun = dry
}

// Row adds a row of values to the table, one per header. Under ResultColumns
// or ErrorColumn the ERROR and MESSAGE cells are made strings first: see
// errorCell and messageCell. A row with the wrong number of cells is a
// programming error: it panics under go test, and otherwise warns on stderr
// and is padded with Unknown or cut to the headers, so that every awk row
// has every column.
func (t *FormattedTable) Row(values ...any) {
	if len(values) != len(t.headers) {
		msg := fmt.Sprintf("kcl: %s prints a row of %d cells under the %d columns %v; please report this", t.command, len(values), len(t.headers), t.headers)
		if testing.Testing() {
			panic(msg)
		}
		fmt.Fprintln(os.Stderr, msg)
		values = slices.Clone(values)
		for len(values) < len(t.headers) {
			values = append(values, Unknown)
		}
		values = values[:len(t.headers)]
	}
	if t.errCol >= 0 && t.errCol < len(values) {
		values = slices.Clone(values)
		values[t.errCol] = errorCell(values[t.errCol])
		if t.msgCol >= 0 && t.msgCol < len(values) {
			values[t.msgCol] = messageCell(values[t.msgCol])
		}
	}
	t.rows = append(t.rows, values)
}

// errorCell is the ERROR cell for v, as a string: an error is its kerr name
// or else its text, as ErrCell has it, and a Stringer is its String. A
// string, Unknown, and nil are themselves. Any other type is a programming
// error: it panics under go test and prints as fmt.Sprint otherwise, so a
// user sees the cell rather than a crash.
func errorCell(v any) any {
	switch v := v.(type) {
	case nil, unknown, string:
		return v
	case error:
		return ErrCell(v)
	case fmt.Stringer:
		return v.String()
	}
	return badCell("ERROR", v)
}

// messageCell is the MESSAGE cell for v, as a string: a *string from a kmsg
// response is what it points to, "" when nil, as BrokerMessage has it.
func messageCell(v any) any {
	switch v := v.(type) {
	case nil, unknown, string:
		return v
	case *string:
		return BrokerMessage(v)
	}
	return badCell("MESSAGE", v)
}

func badCell(column string, v any) string {
	msg := fmt.Sprintf("out: a %s cell holds a %T, which is not a string; please report this", column, v)
	if testing.Testing() {
		panic(msg)
	}
	return fmt.Sprint(v)
}

// Flush writes the buffered data in the configured format to stdout. It
// returns ErrSilent when the table declares ResultColumns or ErrorColumn and
// a row's ERROR is set, and nil otherwise, so a command ends with "return
// table.Flush()".
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

// isError reports whether an ERROR cell names an error: Row made it a string,
// and "", Unknown, and nil do not.
func isError(v any) bool {
	s, ok := v.(string)
	return ok && s != ""
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
			if i == t.errCol && v == "" && t.okText {
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
				m[key] = jsonCell(row[j])
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
	if s, ok := cellText(v); ok && s != "" {
		return s
	}
	return "-"
}

// textCell is the text of one cell: "-" for Unknown, nil, and a nil pointer,
// and otherwise what the value prints as, an empty string included.
func textCell(v any) string {
	if s, ok := cellText(v); ok {
		return s
	}
	return "-"
}

// cellText is what v prints as in text and awk, and false when v is not a
// value at all: Unknown, nil, or a nil pointer. A slice is its elements
// joined by "," with no brackets, so that a replica list is one awk field,
// and an empty slice is "". A pointer, the *string or *int64 a kmsg response
// carries, is what it points to. A Stringer is its String, and anything else
// prints as fmt.Sprint does.
func cellText(v any) (string, bool) {
	switch v := v.(type) {
	case nil, unknown:
		return "", false
	case string:
		return v, true
	case []string:
		return strings.Join(v, ","), true
	case []byte:
		return string(v), true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer && rv.IsNil() {
		return "", false
	}
	if s, ok := v.(fmt.Stringer); ok {
		return s.String(), true
	}
	switch rv.Kind() {
	case reflect.Pointer:
		return cellText(rv.Elem().Interface())
	case reflect.Slice, reflect.Array:
		strs := make([]string, rv.Len())
		for i := range strs {
			strs[i] = textCell(rv.Index(i).Interface())
		}
		return strings.Join(strs, ","), true
	}
	return fmt.Sprint(v), true
}

// jsonCell is v as JSON prints it. A nil slice is [], since a list a row
// carries is known and empty rather than unknown; a type that marshals
// itself is left to do so.
func jsonCell(v any) any {
	if _, ok := v.(json.Marshaler); ok {
		return v
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Slice && rv.IsNil() {
		return []any{}
	}
	return v
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
