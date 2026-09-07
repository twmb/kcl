package main

import (
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
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

func TestUsageErrorsExitTwo(t *testing.T) {
	for _, test := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"argument count", []string{"leaf"}, "accepts 1 arg(s)"},
		{"unknown flag", []string{"leaf", "x", "--nope"}, "unknown flag"},
		{"unknown command", []string{"nope"}, "unknown command"},
		{"unknown subcommand of a group", []string{"group", "nope"}, `unknown command "nope" for "kcl group"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
			root.SetOut(io.Discard)
			root.AddCommand(&cobra.Command{Use: "leaf", Args: cobra.ExactArgs(1), Run: func(*cobra.Command, []string) {}})
			group := &cobra.Command{Use: "group"}
			group.AddCommand(&cobra.Command{Use: "sub", Run: func(*cobra.Command, []string) {}})
			root.AddCommand(group)
			usageErrors(root, nil)
			root.SetArgs(test.args)
			err := asUsageError(root.Execute())
			var ce *out.ExitCodeError
			if err == nil || !errors.As(err, &ce) || ce.Code != out.ExitUsage || !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("err = %v, want exit %d containing %q", err, out.ExitUsage, test.wantErr)
			}
		})
	}
	if asUsageError(nil) != nil {
		t.Error("nil should stay nil")
	}

	// A bare group still shows its help and succeeds, unless the flag check
	// fails, which is a usage error.
	for _, test := range []struct {
		name    string
		check   func() error
		wantErr string
	}{
		{name: "help", check: nil},
		{name: "flags ok", check: func() error { return nil }},
		{name: "bad -X", check: func() error { return errors.New(`unknown opt key "hlep"`) }, wantErr: "hlep"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
			root.SetOut(io.Discard)
			group := &cobra.Command{Use: "group"}
			group.AddCommand(&cobra.Command{Use: "sub", Run: func(*cobra.Command, []string) {}})
			root.AddCommand(group)
			usageErrors(root, test.check)
			root.SetArgs([]string{"group"})
			err := root.Execute()
			if test.wantErr == "" {
				if err != nil {
					t.Errorf("bare group: %v", err)
				}
				return
			}
			var ce *out.ExitCodeError
			if err == nil || !errors.As(err, &ce) || ce.Code != out.ExitUsage || !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("err = %v, want exit 2 containing %q", err, test.wantErr)
			}
		})
	}
}

func TestUsageLineOncePerGroup(t *testing.T) {
	root := &cobra.Command{Use: "kcl"}
	root.SetUsageTemplate(usageTmpl)
	group := &cobra.Command{Use: "group"}
	group.AddCommand(&cobra.Command{Use: "sub", Run: func(*cobra.Command, []string) {}})
	root.AddCommand(group)
	usageErrors(root, nil)
	got := group.UsageString()
	if strings.Contains(got, "\n  kcl group\n") || !strings.Contains(got, "\n  kcl group [command]\n") {
		t.Errorf("usage:\n%s", got)
	}
}

func TestXCompletionRegistered(t *testing.T) {
	root := &cobra.Command{Use: "kcl"}
	root.PersistentFlags().StringArrayP("config-opt", "X", nil, "")
	root.RegisterFlagCompletionFunc("config-opt", func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		return client.XCompletions(), cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
	})
	f, ok := root.GetFlagCompletionFunc("config-opt")
	if !ok {
		t.Fatal("no completion registered for -X")
	}
	got, _ := f(root, nil, "")
	if len(got) == 0 || got[0] != "broker_timeout=\t5s" || !slices.Contains(got, "seed_brokers=\thost1:9092,host2:9092") {
		t.Errorf("completions = %v", got)
	}
}
