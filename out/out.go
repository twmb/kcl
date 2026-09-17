// Package out contains output formatting and error handling for kcl commands.
package out

import (
	"bufio"
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

// ErrSilent is a sentinel error that causes HandleError to exit non-zero
// without printing anything. Commands return this when they have already
// written per-item errors to stderr and just need a non-zero exit code
// so callers (shells, LLMs, CI) can detect failure.
var ErrSilent = &ExitCodeError{Code: ExitError, Err: errors.New("")}

// HandleError formats an error for output and calls os.Exit. If format is
// "json", the error is written as JSON to stdout, carrying _version and, when
// command is not empty, the _command a success document would have. Otherwise
// it is written as plain text to stderr. The exit code is extracted from
// ExitCodeError if present, otherwise defaults to 1. ErrSilent exits
// non-zero without printing anything.
func HandleError(err error, format, command string) {
	code := ExitCode(err)
	if err == ErrSilent {
		os.Exit(code)
	}
	if format == FormatJSON {
		writeJSON(ErrorDoc(err, command))
	} else {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}

// ExitCode is the code err exits with: its own if it carries one, else
// ExitError.
func ExitCode(err error) int {
	var ce *ExitCodeError
	if errors.As(err, &ce) {
		return ce.Code
	}
	return ExitError
}

// ErrorDoc is the JSON document for err. Every error carries _version, and
// _command as well whenever we know which command was asked for; only a
// failure at the bare root does not.
func ErrorDoc(err error, command string) map[string]any {
	doc := map[string]any{
		"_version": 1,
		"error":    err.Error(),
		"code":     ExitCode(err),
	}
	if command != "" {
		doc["_command"] = command
	}
	return doc
}

// CommandName is the _command a document carries, the dotted path of the
// command under kcl: "topic.list" for "kcl topic list", and "" for kcl
// itself. Pass cobra's CommandPath.
//
// Every document names the command it came from this way, rather than each
// call site typing a name of its own: "kcl topic list" used to say
// metadata.topics when it worked and topic.list when it failed.
func CommandName(path string) string {
	return strings.ReplaceAll(strings.TrimSpace(strings.TrimPrefix(path, "kcl")), " ", ".")
}

// Answer is what Confirm heard.
type Answer int

const (
	// Yes: you typed y or yes.
	Yes Answer = iota
	// No: you typed anything else.
	No
	// NotATerminal: stdin is not a terminal, or ended before an answer, so
	// nothing was read. A script that forgot -y gets this, and so does
	// "< /dev/null", which makes that the scriptable dry run.
	NotATerminal
)

// Confirm asks prompt on stderr, with " [y/N] " appended, and reads one line
// from stdin. It reads only when stdin is a terminal, and it prints no
// document: on No and NotATerminal the caller prints the plan it would have
// carried out and exits 0.
func Confirm(prompt string) Answer {
	return confirm(os.Stdin, os.Stderr, prompt, isTerminal(os.Stdin))
}

func confirm(r io.Reader, w io.Writer, prompt string, terminal bool) Answer {
	fmt.Fprint(w, prompt+" [y/N] ")
	if !terminal {
		fmt.Fprintln(w, "no (stdin is not a terminal)")
		return NotATerminal
	}
	line, err := bufio.NewReader(r).ReadString('\n')
	if err != nil && line == "" {
		fmt.Fprintln(w, "no (end of input)")
		return NotATerminal
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return Yes
	}
	return No
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
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
