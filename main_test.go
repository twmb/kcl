package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"runtime"
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
		"kcl topic list-offsets":        "list-offsets [TOPICS...]",
		"kcl misc list-offsets":         "list-offsets [TOPICS...]",
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

// TestExamplesArePasteable pins that every line of every EXAMPLES: block is
// a command you can paste: it starts with kcl, and its flags and arguments
// parse against the tree. A block names the command whose help it is in, or
// one under it for a group, at least once; a line may name a related command
// as the step before or after. Examples live in the long help rather than
// cobra's Example field, so the walk reads the block the way --help-json
// does. A pipeline or a redirection is checked at each kcl segment.
func TestExamplesArePasteable(t *testing.T) {
	root, _ := buildRoot()
	var checked int
	var hidden func(*cobra.Command) bool
	hidden = func(cmd *cobra.Command) bool {
		return cmd.Hidden || cmd.HasParent() && hidden(cmd.Parent())
	}
	allCommands(root, func(cmd *cobra.Command) {
		path := cmd.CommandPath()
		lines := examples(cmd)
		var own int
		for _, line := range lines {
			checked++
			var found bool
			for _, words := range shellSegments(line) {
				if len(words) == 0 || words[0] != "kcl" {
					continue
				}
				found = true
				// A fresh tree per example: parsing sets flag variables,
				// and a slice flag appends on every parse.
				fresh, _ := buildRoot()
				sub, rest, err := fresh.Find(words[1:])
				if err != nil {
					t.Errorf("%s: example %q: %v", path, line, err)
					continue
				}
				if err := sub.ParseFlags(rest); err != nil {
					t.Errorf("%s: example %q: %v", path, line, err)
					continue
				}
				if err := sub.ValidateArgs(sub.Flags().Args()); err != nil {
					t.Errorf("%s: example %q: %v", path, line, err)
					continue
				}
				if sub.CommandPath() == path || strings.HasPrefix(sub.CommandPath(), path+" ") {
					own++
				}
			}
			if !found {
				t.Errorf("%s: example does not start with kcl: %q", path, line)
			}
		}
		// A hidden deprecated mirror carries the primary command's
		// examples on purpose, and its deprecation notice names it.
		if len(lines) > 0 && own == 0 && !hidden(cmd) {
			t.Errorf("%s: no example is for this command: %q", path, lines)
		}
	})
	if checked == 0 {
		t.Error("no examples found; the walk is not reaching them")
	}
}

// shellSegments splits an example line the way a shell would read it: words
// broken on spaces outside quotes, with a comment dropped, and the line cut
// into segments at an unquoted |, &, or ;. A redirection and its target are
// dropped from the segment they are in.
func shellSegments(line string) [][]string {
	var (
		segments [][]string
		words    []string
		word     strings.Builder
		inWord   bool
		quote    rune
		redirect bool
	)
	flush := func() {
		if inWord {
			if redirect {
				redirect = false
			} else {
				words = append(words, word.String())
			}
			word.Reset()
			inWord = false
		}
	}
	cut := func() {
		flush()
		if len(words) > 0 {
			segments = append(segments, words)
		}
		words = nil
	}
	for _, r := range line {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				word.WriteRune(r)
			}
		case r == '\'' || r == '"':
			quote = r
			inWord = true
		case r == '#' && !inWord:
			cut()
			return segments
		case r == ' ' || r == '\t':
			flush()
		case r == '|' || r == '&' || r == ';':
			cut()
		case r == '<' || r == '>':
			flush()
			redirect = true
		default:
			word.WriteRune(r)
			inWord = true
		}
	}
	cut()
	return segments
}

func TestShellSegments(t *testing.T) {
	for _, test := range []struct {
		line string
		want [][]string
	}{
		{"kcl topic list", [][]string{{"kcl", "topic", "list"}}},
		{`kcl topic list -r 'logs\.'   # comment`, [][]string{{"kcl", "topic", "list", "-r", `logs\.`}}},
		{"cat x | kcl produce foo", [][]string{{"cat", "x"}, {"kcl", "produce", "foo"}}},
		{"kcl produce foo < lines.txt", [][]string{{"kcl", "produce", "foo"}}},
		{"kcl fake --control &", [][]string{{"kcl", "fake", "--control"}}},
		{"kcl x --format json | jq '.a[] | select(.b == 0)'", [][]string{{"kcl", "x", "--format", "json"}, {"jq", ".a[] | select(.b == 0)"}}},
		{`kcl fault add --rule '{"topic":"foo"}'`, [][]string{{"kcl", "fault", "add", "--rule", `{"topic":"foo"}`}}},
	} {
		t.Run(test.line, func(t *testing.T) {
			got := shellSegments(test.line)
			if !slices.EqualFunc(got, test.want, slices.Equal) {
				t.Errorf("shellSegments(%q) = %q, want %q", test.line, got, test.want)
			}
		})
	}
}

// TestHelpJSONExamples pins that --help-json carries the examples the long
// help does, since nothing sets cobra's Example field any more.
func TestHelpJSONExamples(t *testing.T) {
	root, _ := buildRoot()
	tree := buildCommandJSON(root, false)
	list := tree.Commands["topic"].Commands["list"]
	if len(list.Examples) == 0 || !strings.HasPrefix(list.Examples[0], "kcl topic list") {
		t.Errorf("topic list examples = %q", list.Examples)
	}
	for _, e := range list.Examples {
		if e != strings.TrimSpace(e) {
			t.Errorf("example %q is not trimmed", e)
		}
	}
}

// runChild runs this test binary as kcl with args, the way the walkthrough
// does, and returns what a user sees. A command that exits on its own, as
// --format awk-header does, needs a process of its own.
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

// TestAwkHeader pins that --format awk-header prints the registered header
// row and exits 0 before anything is dialed: localhost:1 refuses connections,
// so a command that reached the cluster would exit 1. A command with no
// registered table prints nothing, consume's own --format answers it too, and
// cobra's argument count does not get in the way.
func TestAwkHeader(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{"registered", []string{"profile", "list", "--format", "awk-header"}, "NAME\tCURRENT\n"},
		{"registered, flag first", []string{"--format=awk-header", "profile", "list"}, "NAME\tCURRENT\n"},
		{"unregistered leaf", []string{"consume", "foo", "--format", "awk-header"}, ""},
		{"unregistered leaf, no arguments", []string{"consume", "-f", "awk-header"}, ""},
		{"missing arguments", []string{"topic", "create", "--format", "awk-header"}, "TOPIC\tTOPIC-ID\tERROR\tMESSAGE\n"},
		{"section selects the table", []string{"group", "describe", "--section", "summary", "--format", "awk-header"}, "GROUP\tCOORDINATOR\tSTATE\tBALANCER\tMEMBERS\tTOTAL-LAG\tERROR\tMESSAGE\n"},
		{"group", []string{"topic", "--format", "awk-header"}, ""},
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

// TestErrorDocumentNamesTheCommand pins the _command and the format of an
// error a failed Execute reports: the new path under a hidden alias, since
// the command records it before it fails; the hint for a boolean flag given a
// value; and text, not a JSON document, when "--format json" belongs to a
// leaf's own --format, the record format of consume and produce.
func TestErrorDocumentNamesTheCommand(t *testing.T) {
	for _, test := range []struct {
		name     string
		args     []string
		command  string
		contains string
		text     bool
	}{
		{"hidden alias", []string{"misc", "list-offsets", "foo", "--at", "bogus", "--format", "json"}, "topic.list-offsets", "invalid --at", false},
		{"bool flag given a value", []string{"topic", "list", "--regex=foo", "--format", "json"}, "topic.list", "flag --regex takes no value; pass the pattern as an argument", false},
		{"bool flag given a value, no hint", []string{"group", "delete", "-d=foo", "--format", "json"}, "group.delete", "flag --dry-run takes no value", false},
		{"consume owns --format", []string{"consume", "--format", "json"}, "", "at least one topic", true},
		{"consume owns --format, flag first", []string{"--format", "json", "consume"}, "", "at least one topic", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			stdout, stderr, code := runChild(t, append([]string{"--no-config-file", "-B", "localhost:1"}, test.args...)...)
			if code != out.ExitUsage {
				t.Errorf("exit %d, want %d; stdout %q stderr %q", code, out.ExitUsage, stdout, stderr)
			}
			if test.text {
				if stdout != "" || !strings.Contains(stderr, test.contains) {
					t.Errorf("stdout %q stderr %q, want text on stderr containing %q", stdout, stderr, test.contains)
				}
				return
			}
			var doc map[string]any
			if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
				t.Fatalf("stdout %q is not JSON: %v; stderr %q", stdout, err, stderr)
			}
			if doc["_command"] != test.command || doc["code"] != float64(2) || !strings.Contains(doc["error"].(string), test.contains) {
				t.Errorf("doc = %v, want _command %q, code 2, error containing %q", doc, test.command, test.contains)
			}
		})
	}
}

// TestEmptyBootstrapIsUsageError pins that an empty -B exits 2 and names the
// flag, rather than connecting to whatever the config named.
func TestEmptyBootstrapIsUsageError(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{"text", []string{"--no-config-file", "-B", "", "topic", "list"}},
		{"json", []string{"--no-config-file", "-B", "", "topic", "list", "--format", "json"}},
		{"empty address", []string{"--no-config-file", "-B", "localhost:1,", "topic", "list"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			stdout, stderr, code := runChild(t, test.args...)
			if code != out.ExitUsage {
				t.Errorf("exit %d, want %d; stdout %q stderr %q", code, out.ExitUsage, stdout, stderr)
			}
			if test.name == "json" {
				var doc map[string]any
				if err := json.Unmarshal([]byte(stdout), &doc); err != nil || doc["code"] != float64(2) || doc["_command"] != "topic.list" || !strings.Contains(doc["error"].(string), "-B") {
					t.Errorf("stdout = %q, want a usage error document naming -B", stdout)
				}
			} else if !strings.Contains(stderr, "-B") {
				t.Errorf("stderr = %q, want it to name -B", stderr)
			}
		})
	}
}

// TestVersionCommand pins the version document: five keys in JSON, one
// KEY<tab>value row each in awk, and aligned lines with no header in text,
// with the details a build does not carry printed as unknown rather than
// dropped. A test binary carries no VCS stamp, so git_ref and build_date are
// the unknown case here.
func TestVersionCommand(t *testing.T) {
	keys := []string{"version", "git_ref", "build_date", "go_version", "os_arch"}
	for _, test := range []struct {
		format string
		check  func(t *testing.T, stdout string)
	}{
		{"json", func(t *testing.T, stdout string) {
			var doc map[string]any
			if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
				t.Fatalf("not JSON: %v: %s", err, stdout)
			}
			if doc["_command"] != "version" || doc["_version"] != float64(1) || len(doc) != 2+len(keys) {
				t.Errorf("doc = %v", doc)
			}
			for _, k := range keys {
				if _, ok := doc[k]; !ok {
					t.Errorf("doc lacks %q: %v", k, doc)
				}
			}
			if v, _ := doc["version"].(string); v == "" {
				t.Errorf("version = %v, want a string", doc["version"])
			}
			if v, _ := doc["go_version"].(string); !strings.HasPrefix(v, "go") {
				t.Errorf("go_version = %v", doc["go_version"])
			}
			if v, _ := doc["os_arch"].(string); v != runtime.GOOS+"/"+runtime.GOARCH {
				t.Errorf("os_arch = %v", doc["os_arch"])
			}
		}},
		{"awk", func(t *testing.T, stdout string) {
			rows := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
			if len(rows) != len(keys) {
				t.Fatalf("got %d rows, want %d: %q", len(rows), len(keys), stdout)
			}
			for i, row := range rows {
				fields := strings.Split(row, "\t")
				if len(fields) != 2 || fields[0] != keys[i] || fields[1] == "" {
					t.Errorf("row %d = %q, want %s<tab>value", i, row, keys[i])
				}
			}
		}},
		{"text", func(t *testing.T, stdout string) {
			lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
			if len(lines) != len(keys) || !strings.HasPrefix(lines[0], "version ") || !strings.HasPrefix(lines[4], "os/arch ") {
				t.Errorf("text = %q", stdout)
			}
			for _, line := range lines {
				if strings.ToUpper(line) == line {
					t.Errorf("line %q reads as a header", line)
				}
			}
		}},
	} {
		t.Run(test.format, func(t *testing.T) {
			root, _ := buildRoot()
			root.SetArgs([]string{"--no-config-file", "version", "--format", test.format})
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			old := os.Stdout
			os.Stdout = w
			execErr := root.Execute()
			w.Close()
			os.Stdout = old
			b, _ := io.ReadAll(r)
			if execErr != nil {
				t.Fatal(execErr)
			}
			test.check(t, string(b))
		})
	}
}

// TestHelpShape pins the help conventions CLAUDE.md names: every Short is a
// sentence ending in a period, every Long opens by repeating it on its own
// line, and the EXAMPLES: and SEE ALSO: headings are spelled that way, with
// their lines indented two spaces.
func TestHelpShape(t *testing.T) {
	root, _ := buildRoot()
	allCommands(root, func(cmd *cobra.Command) {
		if cmd == root {
			// The root's Short is the one line that is not a sentence,
			// and its Long is the tool's introduction.
			return
		}
		path := cmd.CommandPath()
		if cmd.Short == "" {
			t.Errorf("%s: no Short", path)
			return
		}
		if !strings.HasSuffix(cmd.Short, ".") {
			t.Errorf("%s: Short %q does not end in a period", path, cmd.Short)
		}
		if cmd.Long == "" {
			return
		}
		lines := strings.Split(cmd.Long, "\n")
		if lines[0] != cmd.Short {
			t.Errorf("%s: Long opens %q, want the Short %q", path, lines[0], cmd.Short)
		}
		var in string
		for i, line := range lines {
			switch {
			case strings.EqualFold(line, "examples:") || strings.EqualFold(line, "see also:"):
				if line != strings.ToUpper(line) {
					t.Errorf("%s: heading %q on line %d, want it in capitals", path, line, i+1)
				}
				in = line
			case isHelpHeading(line):
				in = ""
			case in != "" && line != "" && !strings.HasPrefix(line, "  "):
				t.Errorf("%s: line %d under %s is not indented two spaces: %q", path, i+1, in, line)
			}
		}
	})
}
