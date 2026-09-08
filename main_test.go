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

func TestCommandName(t *testing.T) {
	root := &cobra.Command{Use: "kcl"}
	topic := &cobra.Command{Use: "topic"}
	list := &cobra.Command{Use: "list", Run: func(*cobra.Command, []string) {}}
	topic.AddCommand(list)
	root.AddCommand(topic)
	for cmd, want := range map[*cobra.Command]string{root: "", topic: "topic", list: "topic.list", nil: ""} {
		if got := commandName(cmd); got != want {
			t.Errorf("commandName = %q, want %q", got, want)
		}
	}
}

// TestTreeShorthandsAndUsage builds the whole tree, which panics on a
// shorthand collision, and pins the letters and usage lines that were made
// consistent across commands.
func TestTreeShorthandsAndUsage(t *testing.T) {
	root, _ := buildRoot()
	byPath := map[string]*cobra.Command{}
	allCommands(root, func(c *cobra.Command) { byPath[c.CommandPath()] = c })

	want := map[string]map[string]string{
		"kcl acl create":                {"dry-run": "d"},
		"kcl acl delete":                {"dry-run": "d"},
		"kcl cluster elect-leaders":     {"dry-run": "d"},
		"kcl cluster features update":   {"dry-run": "d"},
		"kcl group delete":              {"dry-run": "d", "regex": "r"},
		"kcl group describe":            {"regex": "r"},
		"kcl group seek":                {"dry-run": "d", "topic": "t"},
		"kcl group offset-delete":       {"topic": "t"},
		"kcl quota alter":               {"dry-run": "d"},
		"kcl share-group delete":        {"dry-run": "d", "regex": "r"},
		"kcl share-group describe":      {"regex": "r"},
		"kcl share-group seek":          {"dry-run": "d", "topic": "t"},
		"kcl share-group offset-delete": {"topic": "t"},
		"kcl topic delete":              {"dry-run": "d", "regex": "r"},
		"kcl topic list":                {"regex": "r"},
		"kcl topic trim-prefix":         {"offset": "o", "partitions": "p"},
		"kcl topic add-partitions":      {"num": "n", "assignment": "a"},
		"kcl user alter":                {"set": "s"},
		"kcl produce":                   {"schema": "s"},
	}
	for path, flags := range want {
		cmd := byPath[path]
		if cmd == nil {
			t.Errorf("no command %q", path)
			continue
		}
		for name, sh := range flags {
			f := cmd.Flags().Lookup(name)
			if f == nil || f.Shorthand != sh {
				t.Errorf("%s --%s: shorthand = %v, want -%s", path, name, f, sh)
			}
		}
	}
	for path, old := range map[string]string{"kcl group seek": "topics", "kcl share-group seek": "topics", "kcl topic add-partitions": "topic"} {
		if f := byPath[path].Flags().Lookup(old); f == nil || !f.Hidden {
			t.Errorf("%s --%s should still exist, hidden", path, old)
		}
	}
	for path, use := range map[string]string{
		"kcl topic create":              "create TOPICS...",
		"kcl misc list-offsets":         "list-offsets TOPICS...",
		"kcl topic add-partitions":      "add-partitions TOPIC",
		"kcl share-group offset-delete": "offset-delete GROUP",
	} {
		if got := byPath[path].Use; got != use {
			t.Errorf("%s Use = %q, want %q", path, got, use)
		}
	}
}
