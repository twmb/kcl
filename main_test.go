package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

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

func TestFormatFromArgs(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{"absent", []string{"topic", "list"}, ""},
		{"nil", nil, ""},
		{"space", []string{"topic", "list", "--format", "json"}, "json"},
		{"equals", []string{"--format=awk", "topic", "list"}, "awk"},
		// --format has no shorthand; -f is --filter on group list and the
		// record format on consume, so it must not be picked up here.
		{"no shorthand", []string{"group", "list", "-f", "json"}, ""},
		{"invalid value", []string{"--format", "yaml"}, ""},
		{"invalid equals", []string{"--format=yaml"}, ""},
		{"no value", []string{"topic", "list", "--format"}, ""},
		{"value is a flag", []string{"--format", "--nosuchflag"}, ""},
		{"last wins", []string{"--format=json", "--format", "awk"}, "awk"},
		{"last wins invalid", []string{"--format=json", "--format", "yaml"}, ""},
		{"after a bare dash dash", []string{"produce", "foo", "--", "--format", "json"}, ""},
		{"text", []string{"--format", "text"}, "text"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := formatFromArgs(test.args); got != test.want {
				t.Errorf("formatFromArgs(%q) = %q, want %q", test.args, got, test.want)
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
	if len(got) == 0 || got[0] != "broker_timeout=\t5s" || !slices.Contains(got, "seed_brokers=\tlocalhost:9092") {
		t.Errorf("completions = %v", got)
	}
}

func TestCommandName(t *testing.T) {
	root := &cobra.Command{Use: "kcl"}
	topic := &cobra.Command{Use: "topic"}
	list := &cobra.Command{Use: "list", Run: func(*cobra.Command, []string) {}}
	topic.AddCommand(list)
	root.AddCommand(topic)
	for cmd, want := range map[*cobra.Command]string{root: "", topic: "topic", list: "topic.list"} {
		if got := out.CommandName(cmd.CommandPath()); got != want {
			t.Errorf("CommandName(%q) = %q, want %q", cmd.CommandPath(), got, want)
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

	// --help-json says so too, and carries the version of its own shape.
	tree := helpJSON{Version: 1, commandJSON: buildCommandJSON(root, false)}
	raw, err := json.Marshal(tree)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["_version"] != float64(1) || doc["name"] != "kcl" {
		t.Errorf("--help-json top level = %v", doc)
	}
	seek := tree.Commands["group"].Commands["seek"]
	if f := seek.Flags["topics"]; !f.Hidden {
		t.Errorf("group seek --topics in --help-json = %+v, want hidden", f)
	}
	if f := seek.Flags["topic"]; f.Hidden {
		t.Errorf("group seek --topic in --help-json = %+v, want visible", f)
	}
	if f := tree.Flags["dump-json"]; f.Deprecated == "" {
		t.Errorf("--dump-json in --help-json = %+v, want deprecated", f)
	}
	if _, ok := tree.Flags["help-json"]; ok {
		t.Error("--help-json lists itself")
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

// TestGroupsNameAnUnknownSubcommand pins that every command group answers a
// typo by naming it. Cobra validates arguments before RunE, so a group that
// sets Args of its own reports an argument count instead: "kcl acl zzz" said
// "accepts 0 arg(s), received 1" while the other thirteen groups said
// `unknown command "zzz" for "kcl acl"`.
func TestGroupsNameAnUnknownSubcommand(t *testing.T) {
	root, _ := buildRoot()
	allCommands(root, func(cmd *cobra.Command) {
		if !cmd.HasParent() || !cmd.HasSubCommands() {
			return
		}
		// kcl fake is a cluster you run, not only a group, so a stray
		// argument to it really is an argument error.
		if strings.HasPrefix(cmd.CommandPath(), "kcl fake") {
			return
		}
		if err := cmd.ValidateArgs([]string{"zzz"}); err != nil {
			t.Errorf("%s: %v; a group must let the argument through so the run can name it", cmd.CommandPath(), err)
			return
		}
		err := cmd.RunE(cmd, []string{"zzz"})
		want := `unknown command "zzz" for "` + cmd.CommandPath() + `"`
		var ce *out.ExitCodeError
		if err == nil || !errors.As(err, &ce) || ce.Code != out.ExitUsage || err.Error() != want {
			t.Errorf("%s: err = %v, want exit 2 and %q", cmd.CommandPath(), err, want)
		}
	})
}

// TestExamplesArePasteable pins that every Example line is a command you can
// paste. buildRoot used to rewrite the Example field, replacing the bare
// command name with the full path, which doubled a path that was already
// full ("kcl acl kcl acl delete --topic foo") and mangled any prose that
// happened to contain the word ("kcl logdirs describes all").
func TestExamplesArePasteable(t *testing.T) {
	root, _ := buildRoot()
	var checked int
	var hidden func(*cobra.Command) bool
	hidden = func(cmd *cobra.Command) bool {
		return cmd.Hidden || cmd.HasParent() && hidden(cmd.Parent())
	}
	allCommands(root, func(cmd *cobra.Command) {
		if cmd.Example == "" {
			return
		}
		path := cmd.CommandPath()
		for _, line := range strings.Split(cmd.Example, "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			checked++
			if line != strings.TrimLeft(line, " \t") {
				t.Errorf("%s: example is indented, so it does not paste: %q", path, line)
			}
			if !strings.HasPrefix(line, "kcl ") {
				t.Errorf("%s: example does not start with kcl: %q", path, line)
			}
			// A hidden deprecated mirror carries the primary command's
			// examples on purpose, and its deprecation notice names it.
			if !hidden(cmd) && !strings.HasPrefix(line, path+" ") && line != path {
				t.Errorf("%s: example is for another command: %q", path, line)
			}
		}
	})
	if checked == 0 {
		t.Error("no examples found; the walk is not reaching them")
	}
}

// runChild runs this test binary as kcl with args, the way the walkthrough
// does, and returns what a user sees. A command that exits on its own, as
// --awk-header does, needs a process of its own.
func runChild(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	enc, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "KCL_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, walkthroughEnv+"="+string(enc))
	var outb, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &outb, &errb
	err = cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		code = exit.ExitCode()
	default:
		t.Fatalf("unable to run kcl %s: %v", strings.Join(args, " "), err)
	}
	return outb.String(), errb.String(), code
}

// TestAwkHeader pins that --awk-header prints the registered header row and
// exits 0 before anything is dialed: localhost:1 refuses connections, so a
// command that reached the cluster would exit 1. A command with no registered
// table prints nothing, and cobra's argument count does not get in the way.
func TestAwkHeader(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{"registered", []string{"profile", "list", "--awk-header"}, "NAME\tCURRENT\n"},
		{"registered, flag first", []string{"--awk-header", "profile", "list"}, "NAME\tCURRENT\n"},
		{"unregistered leaf", []string{"topic", "list", "--awk-header"}, ""},
		{"missing arguments", []string{"topic", "create", "--awk-header"}, ""},
		{"group", []string{"topic", "--awk-header"}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			stdout, stderr, code := runChild(t, append([]string{"--no-config-file", "-B", "localhost:1"}, test.args...)...)
			if code != 0 || stdout != test.want || stderr != "" {
				t.Errorf("exit %d stdout %q stderr %q, want exit 0 stdout %q and no stderr", code, stdout, stderr, test.want)
			}
		})
	}
}
