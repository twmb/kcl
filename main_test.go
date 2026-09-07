package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestBuildCommandJSONHiddenPropagates pins that --help-json marks a hidden
// command's descendants hidden too. Cobra hides a parent without touching its
// children, so kcl's whole deprecated "admin" subtree looked visible in the
// JSON while being absent from --help -- a consumer filtering per node had to
// know to discard entire subtrees instead.
func TestBuildCommandJSONHiddenPropagates(t *testing.T) {
	leaf := &cobra.Command{Use: "leaf", Run: func(*cobra.Command, []string) {}}
	mid := &cobra.Command{Use: "mid"}
	mid.AddCommand(leaf)
	hidden := &cobra.Command{Use: "legacy", Hidden: true}
	hidden.AddCommand(mid)

	visible := &cobra.Command{Use: "current", Run: func(*cobra.Command, []string) {}}

	root := &cobra.Command{Use: "kcl"}
	root.AddCommand(hidden, visible)

	tree := buildCommandJSON(root, false)

	if tree.Hidden {
		t.Error("root should not be hidden")
	}
	if got := tree.Commands["current"]; got.Hidden {
		t.Error("a visible sibling should not be hidden")
	}
	legacy := tree.Commands["legacy"]
	if !legacy.Hidden {
		t.Error("the hidden parent should be marked hidden")
	}
	if !legacy.Commands["mid"].Hidden {
		t.Error("a child of a hidden command should be marked hidden")
	}
	if !legacy.Commands["mid"].Commands["leaf"].Hidden {
		t.Error("a grandchild of a hidden command should be marked hidden")
	}
}

func TestWantsHelpJSON(t *testing.T) {
	for _, test := range []struct {
		args []string
		want bool
	}{
		{nil, false},
		{[]string{"topic", "list"}, false},
		{[]string{"--help-json"}, true},
		{[]string{"topic", "list", "--help-json"}, true},
		{[]string{"--help-json=true"}, true},
		{[]string{"--help-json=1"}, true},
		{[]string{"--help-json=false"}, false},
		{[]string{"--help-json=nope"}, false},
		{[]string{"--help-json=false", "--help-json"}, true},
		{[]string{"--help-json", "--help-json=false"}, false},
		{[]string{"produce", "foo", "--", "--help-json"}, false},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			if got := wantsHelpJSON(test.args); got != test.want {
				t.Errorf("wantsHelpJSON(%q) = %v, want %v", test.args, got, test.want)
			}
		})
	}
}
