// Package out contains simple functions to print messages out and maybe die.
package out

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/twmb/kgo/kerr"
)

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

// DieJSON prints a message to stderr, dumps json to stdout, and exits with 1.
func DieJSON(j interface{}, msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	DumpJSON(j)
	os.Exit(1)
}

// QuitJSON dumps json to stdout and exits with 0.
func QuitJSON(j interface{}) {
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
func ErrAndMsg(code int16, msg *string) {
	if err := kerr.ErrorForCode(code); err != nil {
		additional := ""
		if msg != nil {
			additional = ": " + *msg
		}
		fmt.Fprintf(os.Stderr, "%s%s\n", err, additional)
		return
	}
	fmt.Println("OK")
}
