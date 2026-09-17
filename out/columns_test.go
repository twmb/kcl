package out

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestColumns pins the registry: --awk-header prints what a command
// registered, for the flag state its function reads, and nothing for a command
// that registered no table. A table built under awk while a registered command
// runs must match, and under go test a mismatch panics.
func TestColumns(t *testing.T) {
	defer SetRunning(nil)

	fixed := &cobra.Command{Use: "list"}
	Columns(fixed, "NAME", "CURRENT")

	section := ""
	byFlag := &cobra.Command{Use: "describe"}
	ColumnsFunc(byFlag, func() []string {
		if section == "members" {
			return []string{"GROUP", "MEMBER-ID"}
		}
		return []string{"GROUP", "TOPIC", "PARTITION", "LAG"}
	})

	none := &cobra.Command{Use: "consume"}

	for _, test := range []struct {
		name    string
		cmd     *cobra.Command
		section string
		want    string
	}{
		{"fixed", fixed, "", "NAME\tCURRENT\n"},
		{"flag default", byFlag, "", "GROUP\tTOPIC\tPARTITION\tLAG\n"},
		{"flag members", byFlag, "members", "GROUP\tMEMBER-ID\n"},
		{"unregistered", none, "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			section = test.section
			if got := AwkHeader(test.cmd); got != test.want {
				t.Errorf("AwkHeader = %q, want %q", got, test.want)
			}
		})
	}

	// The running command's table is checked in awk, and only in awk: text
	// prints several tables per command and is free to.
	section = ""
	SetRunning(byFlag)
	captureStdout(func() {
		NewFormattedTable("awk", "group.describe", 1, "lag", "GROUP", "TOPIC", "PARTITION", "LAG").Flush()
		NewFormattedTable("text", "group.describe", 1, "members", "GROUP", "MEMBER-ID").Flush()
		NewFormattedTable("json", "group.describe", 1, "members", "GROUP", "MEMBER-ID").Flush()
	})
	section = "members"
	captureStdout(func() {
		NewFormattedTable("awk", "group.describe", 1, "members", "GROUP", "MEMBER-ID").Flush()
	})

	// An unregistered running command is not checked.
	SetRunning(none)
	captureStdout(func() { NewFormattedTable("awk", "consume", 1, "x", "A").Flush() })

	SetRunning(byFlag)
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("a mismatched awk table did not panic under go test")
		}
		if msg := r.(string); !strings.Contains(msg, "[GROUP TOPIC]") || !strings.Contains(msg, "[GROUP MEMBER-ID]") {
			t.Errorf("panic names neither side: %s", msg)
		}
	}()
	NewFormattedTable("awk", "group.describe", 1, "lag", "GROUP", "TOPIC")
}
