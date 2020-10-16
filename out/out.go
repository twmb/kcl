// Package out contains simple functions to print messages out and maybe die.
package out

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
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
