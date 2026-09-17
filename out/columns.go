package out

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// Columns registers the awk row cmd prints, which is the contract a script
// reads: --format awk-header prints these headers as one tab separated line
// and exits 0 before the command runs or anything is dialed, and a table
// built under --format awk while cmd runs must have exactly these headers.
//
// Register at construction, one call per command, next to its flags:
//
//	out.Columns(cmd, "TOPIC", "PARTITION", "ERROR", "MESSAGE")
//
// When a flag selects which table prints (--section, --by, --aggregate-into),
// register a function of the parsed flags with ColumnsFunc instead. It runs
// after cobra has parsed them, so it reads the same variables RunE does:
//
//	out.ColumnsFunc(cmd, func() []string {
//		switch section {
//		case "summary":
//			return []string{"GROUP", "STATE", "MEMBERS"}
//		case "members":
//			return []string{"GROUP", "MEMBER-ID", "CLIENT-ID", "HOST"}
//		}
//		return []string{"GROUP", "TOPIC", "PARTITION", "LAG"}
//	})
//
// A flag that fills a cell rather than choosing a table (--stable, --at) does
// not change the registration: its column is always present, and holds
// Unknown when the flag is off. A command that prints key and value rows
// registers KEY and VALUE. A command with no table (consume, produce) registers
// nothing, and --format awk-header prints nothing for it. A constructor that
// is called twice, once for the command and once for its hidden alias,
// registers each cobra command it builds, since the map is keyed by the
// command.
//
// The check runs in NewFormattedTable for the awk format only: text may print
// several tables, and JSON prints them under keys of their own. A mismatch
// panics under go test and warns on stderr otherwise. The rows a user sees are
// right either way and only the header row could disagree, so we tell them
// rather than fail the command.
func Columns(cmd *cobra.Command, headers ...string) {
	ColumnsFunc(cmd, func() []string { return headers })
}

// ColumnsFunc is Columns for a command whose row depends on its flags.
func ColumnsFunc(cmd *cobra.Command, headers func() []string) {
	columns[cmd] = headers
}

var (
	columns = make(map[*cobra.Command]func() []string)
	running *cobra.Command
)

// SetRunning records the command that is running, so that a table built while
// it runs is checked against its Columns. The root's persistent pre-run calls
// this once, after answering --format awk-header.
func SetRunning(cmd *cobra.Command) {
	running = cmd
}

// AwkHeader is what --format awk-header prints for cmd: its registered
// headers, tab separated and newline terminated, or "" when cmd registered
// none.
func AwkHeader(cmd *cobra.Command) string {
	fn, ok := columns[cmd]
	if !ok {
		return ""
	}
	return strings.Join(fn(), "\t") + "\n"
}

func checkColumns(command string, headers []string) {
	if running == nil {
		return
	}
	fn, ok := columns[running]
	if !ok {
		return
	}
	want := fn()
	if slices.Equal(want, headers) {
		return
	}
	msg := fmt.Sprintf("kcl: %s prints the awk columns %v but registered %v; please report this", command, headers, want)
	if testing.Testing() {
		panic(msg)
	}
	fmt.Fprintln(os.Stderr, msg)
}
