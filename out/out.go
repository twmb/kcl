// Package out contains simple functions to print messages out and maybe die.
package out

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/twmb/franz-go/pkg/kerr"
)

// BeginTabWrite returns a new tabwriter that prints to stdout.
func BeginTabWrite() *tabwriter.Writer {
	return BeginTabWriteTo(os.Stdout)
}

// BeginTabWriteTo returns a new tabwriter that prints to w.
func BeginTabWriteTo(w io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(w, 6, 4, 2, ' ', 0)
}

// Exit calls os.Exit(1).
func Exit() {
	os.Exit(1)
}

// MaybeDie, if err is non-nil, prints the message and exits with 1.
func MaybeDie(err error, msg string, args ...interface{}) {
	if err != nil {
		Die(msg, args...)
	}
}

// Die prints a message to stderr and exits with 1.
func Die(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

// ExitErrJSON prints a message to stderr, dumps json to stdout, and exits with 1.
func ExitErrJSON(j interface{}, msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	DumpJSON(j)
	os.Exit(1)
}

// ExitJSON dumps json to stdout and exits with 0.
func ExitJSON(j interface{}) {
	DumpJSON(j)
	os.Exit(0)
}

// DumpJSON prints json to stderr. This exits with 1 if the input is unmarshalable.
func DumpJSON(j interface{}) {
	out, err := json.MarshalIndent(j, "", "  ")
	MaybeDie(err, "unable to json marshal response: %v", err)
	fmt.Printf("%s\n", out)
}

// ErrAndMsg prints OK to stdout if code is 0, otherwise the error name to
// stderr as well as a message if non-nil.
//
// This returns true if the code was an error.
func ErrAndMsg(code int16, msg *string) bool {
	if err := kerr.ErrorForCode(code); err != nil {
		additional := ""
		if msg != nil {
			additional = ": " + *msg
		}
		fmt.Fprintf(os.Stderr, "%s%s\n", err, additional)
		return true
	}
	fmt.Println("OK")
	return false
}

func MaybeExitErrMsg(code int16, msg *string) {
	if err := kerr.ErrorForCode(code); err != nil {
		additional := ""
		if msg != nil {
			additional = ": " + *msg
		}
		Die("%s%s\n", err, additional)
	}
}

func args2strings(args []interface{}) []string {
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
func (t *TabWriter) Print(args ...interface{}) {
	t.PrintStrings(args2strings(args)...)
}

// PrintStrings prints the arguments tab-delimited and newline-suffixed to the
// tab writer.
func (t *TabWriter) PrintStrings(args ...string) {
	fmt.Fprint(t.Writer, strings.Join(args, "\t")+"\n")
}

// Line prints a newline in our tab writer. This will reset tab spacing.
func (t *TabWriter) Line(sprint ...interface{}) {
	fmt.Fprint(t.Writer, append(sprint, "\n")...)
}
