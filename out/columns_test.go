package out

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestColumns pins the registry: --format awk-header prints what a command
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

	// An unregistered running command that prints an awk table is the
	// same mistake as a mismatch: --format awk-header would print nothing
	// for it. Text and JSON are not checked.
	SetRunning(none)
	captureStdout(func() {
		NewFormattedTable("text", "consume", 1, "x", "A").Flush()
		NewFormattedTable("json", "consume", 1, "x", "A").Flush()
	})
	for _, test := range []struct {
		name    string
		running *cobra.Command
		headers []string
		want    []string
	}{
		{"mismatch", byFlag, []string{"GROUP", "TOPIC"}, []string{"[GROUP TOPIC]", "[GROUP MEMBER-ID]"}},
		{"unregistered", none, []string{"A"}, []string{"[A]", "registered none"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			SetRunning(test.running)
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("the awk table did not panic under go test")
				}
				for _, want := range test.want {
					if msg := r.(string); !strings.Contains(msg, want) {
						t.Errorf("panic %q does not say %q", msg, want)
					}
				}
			}()
			NewFormattedTable("awk", "group.describe", 1, "lag", test.headers...)
		})
	}
}

// TestCommandOf pins how a hidden alias names the command it forwards to:
// a marked subtree renames its prefix, "" dropping it, a marked leaf names
// its own target even under a marked subtree, and an unmarked command is
// its cobra path.
func TestCommandOf(t *testing.T) {
	root := &cobra.Command{Use: "kcl"}
	admin := &cobra.Command{Use: "admin"}
	AliasOf(admin, "")
	adminTopic := &cobra.Command{Use: "topic"}
	adminTopicList := &cobra.Command{Use: "list"}
	adminTopic.AddCommand(adminTopicList)
	electLeaders := &cobra.Command{Use: "elect-leaders TOPIC:P..."}
	AliasOf(electLeaders, "cluster.elect-leaders")
	admin.AddCommand(adminTopic, electLeaders)
	myconfig := &cobra.Command{Use: "myconfig"}
	AliasOf(myconfig, "profile")
	use := &cobra.Command{Use: "use NAME"}
	setup := &cobra.Command{Use: "setup NAME"}
	AliasOf(setup, "profile.create")
	myconfig.AddCommand(use, setup)
	topic := &cobra.Command{Use: "topic"}
	topicList := &cobra.Command{Use: "list"}
	topic.AddCommand(topicList)
	root.AddCommand(admin, myconfig, topic)

	for _, test := range []struct {
		cmd  *cobra.Command
		want string
	}{
		{root, ""},
		{topicList, "topic.list"},
		{admin, ""},
		{adminTopic, "topic"},
		{adminTopicList, "topic.list"},
		{electLeaders, "cluster.elect-leaders"},
		{myconfig, "profile"},
		{use, "profile.use"},
		{setup, "profile.create"},
	} {
		if got := CommandOf(test.cmd); got != test.want {
			t.Errorf("CommandOf(%s) = %q, want %q", test.cmd.CommandPath(), got, test.want)
		}
	}
}
