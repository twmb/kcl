// Package out contains output formatting and error handling for kcl commands.
package out

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
)

// BeginTabWrite returns a new tabwriter that prints to stdout.
func BeginTabWrite() *tabwriter.Writer {
	return BeginTabWriteTo(os.Stdout)
}

// BeginTabWriteTo returns a new tabwriter that prints to w.
func BeginTabWriteTo(w io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(w, 6, 4, 2, ' ', 0)
}

// Standard exit codes.
const (
	ExitOK    = 0 // success
	ExitError = 1 // general / Kafka-level error
	ExitUsage = 2 // invalid usage (bad flags, args, parse errors)
)

// ExitError is an error with a specific exit code. Commands should return
// these to indicate non-default exit codes.
type ExitCodeError struct {
	Code int
	Err  error
}

func (e *ExitCodeError) Error() string { return e.Err.Error() }
func (e *ExitCodeError) Unwrap() error { return e.Err }

// Errf returns an error that, when handled by HandleError, exits with the
// given code.
func Errf(code int, format string, args ...any) error {
	return &ExitCodeError{Code: code, Err: fmt.Errorf(format, args...)}
}

// HandleError formats an error for output and calls os.Exit. If format is
// "json", the error is written as JSON to stdout. Otherwise it is written
// as plain text to stderr. The exit code is extracted from ExitCodeError
// if present, otherwise defaults to 1.
func HandleError(err error, format string) {
	code := ExitError
	var ce *ExitCodeError
	if errors.As(err, &ce) {
		code = ce.Code
	}
	if format == FormatJSON {
		writeJSON(map[string]any{
			"error": err.Error(),
			"code":  code,
		})
	} else {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}

// MaybeDie, if err is non-nil, prints the message and exits with 1.
//
// Deprecated: Commands should return errors instead. This remains for use
// in goroutines and callbacks where returning an error is not possible.
func MaybeDie(err error, msg string, args ...any) {
	if err != nil {
		Die(msg, args...)
	}
}

// Die prints a message to stderr and exits with 1.
//
// Deprecated: Commands should return errors instead. This remains for use
// in goroutines and callbacks where returning an error is not possible.
func Die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}


func args2strings(args []any) []string {
	sargs := make([]string, len(args))
	for i, arg := range args {
		sargs[i] = fmt.Sprint(arg)
	}
	return sargs
}

// TabWriter writes tab delimited output.
type TabWriter struct {
	*tabwriter.Writer
}

// NewTable returns a TabWriter that is meant to output a "table". The headers
// are uppercased and immediately printed; Print can be used to append
// additional rows.
func NewTable(headers ...string) *TabWriter {
	for i, header := range headers {
		headers[i] = strings.ToUpper(header)
	}
	t := NewTabWriter()
	t.PrintStrings(headers...)
	return t
}

// NewTabWriter returns a TabWriter. For table formatted output, prefer
// NewTable. This function is meant to be used when you may want some column
// style output (i.e., headers on the left).
func NewTabWriter() *TabWriter {
	return &TabWriter{tabwriter.NewWriter(os.Stdout, 6, 4, 2, ' ', 0)}
}

// Print stringifies the arguments and calls PrintStrings.
func (t *TabWriter) Print(args ...any) {
	t.PrintStrings(args2strings(args)...)
}

// PrintStrings prints the arguments tab-delimited and newline-suffixed to the
// tab writer.
func (t *TabWriter) PrintStrings(args ...string) {
	fmt.Fprint(t.Writer, strings.Join(args, "\t")+"\n")
}

// Line prints a newline in our tab writer. This will reset tab spacing.
func (t *TabWriter) Line(sprint ...any) {
	fmt.Fprint(t.Writer, append(sprint, "\n")...)
}
