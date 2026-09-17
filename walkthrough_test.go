package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/twmb/kcl/out"
)

// The walkthrough runs every read-only command against one kfake cluster and
// checks what a user sees: the exit code, and the shape of stdout in each of
// the three formats. It runs each command in a child process because a
// command may exit on its own, and because the exit code is half of what we
// are checking.

// walkthroughEnv carries the arguments of the kcl a child runs, as JSON. A
// test binary with this set in its environment is a kcl, not a test.
const walkthroughEnv = "KCL_TEST_WALKTHROUGH_ARGS"

func TestMain(m *testing.M) {
	if s, ok := os.LookupEnv(walkthroughEnv); ok {
		var args []string
		if err := json.Unmarshal([]byte(s), &args); err != nil {
			fmt.Fprintf(os.Stderr, "unable to read %s: %v\n", walkthroughEnv, err)
			os.Exit(99)
		}
		os.Args = append([]string{"kcl"}, args...)
		main() // exits on its own on failure
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// Names the walkthrough seeds and then reads back.
const (
	walkTopic   = "walk-topic"
	walkOther   = "walk-other"
	walkGroup   = "walk-group"
	walkSubject = walkTopic + "-value"
	walkProfile = "walkthrough"

	// walkRecords is how many records each seeded topic carries.
	walkRecords = 5
)

// schemaFileArg stands in for the schema file a command reads. We write the
// file per run, so the table cannot name its path.
const schemaFileArg = "<schema-file>"

// walkthroughLeaves are the leaf commands the walkthrough runs, by the dotted
// path out.CommandName gives them. Args are what follows the command name.
// Exit is what a user sees: zero, unless kfake cannot answer the request, and
// then why says what it answers instead.
var walkthroughLeaves = []struct {
	path  string
	args  []string
	stdin string
	exit  int
	why   string
}{
	{path: "acl.list"},
	{path: "client-metrics.describe", args: []string{"walk-metrics"}, exit: 1, why: "kfake answers INVALID_REQUEST for a client metrics config resource"},
	{path: "client-metrics.list"},
	{path: "cluster.describe"},
	{path: "cluster.describe-quorum", exit: 1, why: "kfake does not implement DescribeQuorum"},
	{path: "cluster.features.describe"},
	{path: "cluster.metadata"},
	{path: "config.describe", args: []string{walkTopic}},
	{path: "dtoken.describe", exit: 1, why: "kfake does not implement DescribeDelegationToken"},
	{path: "group.describe", args: []string{walkGroup}},
	{path: "group.list"},
	{path: "logdirs.describe"},
	{path: "misc.api-versions"},
	{path: "misc.errcode", args: []string{"3"}},
	{path: "misc.errtext", args: []string{"UNKNOWN_TOPIC_OR_PARTITION"}},
	{path: "misc.offset-for-leader-epoch", args: []string{walkTopic, "-e", "0"}},
	{path: "misc.probe-version"},
	{path: "misc.raw-req", args: []string{"-k", "18"}, stdin: "{}"},
	{path: "profile.current"},
	{path: "profile.dump"},
	{path: "profile.list"},
	{path: "quota.describe"},
	{path: "reassign.list"},
	{path: "registry.compatibility.get", args: []string{walkSubject}},
	{path: "registry.context.list"},
	{path: "registry.mode.get"},
	{path: "registry.schema.check-compatibility", args: []string{walkSubject, "-s", schemaFileArg}},
	{path: "registry.schema.get", args: []string{"-S", walkSubject}},
	{path: "registry.schema.list"},
	{path: "registry.schema.references", args: []string{walkSubject}},
	{path: "registry.subject.list"},
	{path: "share-group.describe", args: []string{"walk-share"}, exit: 1, why: "nothing has joined a share group, so the broker answers GROUP_ID_NOT_FOUND"},
	{path: "share-group.list"},
	{path: "topic.describe", args: []string{walkTopic}},
	{path: "topic.list"},
	{path: "topic.list-offsets", args: []string{walkTopic}},
	{path: "txn.describe", args: []string{"walk-txn"}, exit: 1, why: "nothing has produced transactionally, so the broker answers TRANSACTIONAL_ID_NOT_FOUND"},
	{path: "txn.describe-producers", args: []string{walkTopic}},
	{path: "txn.list"},
	{path: "user.list"},
	{path: "version"},
}

// walkthroughSkips are the leaves the walkthrough does not run, and why. A
// leaf in neither list fails the run, so a new command cannot arrive without
// someone deciding which of the two it is.
var walkthroughSkips = []struct{ path, why string }{
	{"client-metrics.alter", "alters a metrics subscription"},
	{"client-metrics.delete", "deletes a metrics subscription"},
	{"cluster.add-controller", "changes the quorum"},
	{"cluster.features.update", "changes finalized feature versions"},
	{"cluster.remove-controller", "changes the quorum"},
	{"consume", "runs until it is interrupted; the consume package tests it"},
	{"dtoken.create", "creates a delegation token"},
	{"dtoken.expire", "expires a delegation token"},
	{"dtoken.renew", "renews a delegation token"},
	{"fake.control.call", "drives a running kcl fake cluster"},
	{"fake.control.fault.add", "drives a running kcl fake cluster"},
	{"fake.control.fault.list", "needs a kcl fake control endpoint, which a cluster in this process does not serve"},
	{"fake.control.fault.rm", "drives a running kcl fake cluster"},
	{"fake.control.fault.wait", "blocks until a fault is hit"},
	{"fake.control.group.wait", "blocks until a group reaches a state"},
	{"fake.control.methods", "needs a kcl fake control endpoint, which a cluster in this process does not serve"},
	{"group.offset-delete", "deletes committed offsets"},
	{"logdirs.alter", "moves partitions between log dirs"},
	{"misc.gen-autocomplete", "writes a shell script for you to source, so there is no document for --format to shape"},
	{"produce", "reads records from stdin and writes them to the cluster"},
	{"reassign.alter", "reassigns partitions"},
	{"reassign.cancel", "cancels a reassignment"},
	{"registry.compatibility.set", "sets a compatibility level"},
	{"registry.context.delete", "deletes a context"},
	{"registry.schema.delete", "deletes a schema version, and has no dry run"},
	{"registry.subject.delete", "deletes a subject, and has no dry run"},
	{"registry.mode.set", "sets a mode"},
	{"registry.schema.create", "registers a schema"},
	{"share-group.offset-delete", "deletes committed offsets"},
	{"topic.add-partitions", "adds partitions"},
	{"user.alter", "alters SCRAM credentials"},
}

// walkthroughMutations are the mutating leaves the walkthrough runs against
// the cluster in the way that changes nothing: a dry run, or a [y/N] prompt
// whose stdin is not a terminal, which prints the plan. Each prints one
// document in every format, marked as a dry run, and exits 0, so that a
// script can preview what a command would do.
var walkthroughMutations = []struct {
	path string
	args []string
}{
	{path: "acl.create", args: []string{"--topic", walkTopic, "--allow-principal", "User:alice", "--operation", "read", "--dry-run"}},
	{path: "acl.delete", args: []string{"--topic", walkTopic, "--dry-run"}},
	{path: "cluster.elect-leaders", args: []string{walkTopic + ":0", "--dry-run"}},
	{path: "config.alter", args: []string{walkTopic, "-s", "retention.ms=1000", "--dry-run"}},
	{path: "group.delete", args: []string{walkGroup, "--dry-run"}},
	{path: "group.seek", args: []string{walkGroup, "--to", "start", "--dry-run"}},
	{path: "quota.alter", args: []string{"--name", "user=alice", "--add", "producer_byte_rate=1048576", "--dry-run"}},
	{path: "share-group.delete", args: []string{"walk-share", "--dry-run"}},
	{path: "share-group.seek", args: []string{"walk-share", "--to", "start", "-t", walkTopic, "--dry-run"}},
	{path: "topic.create", args: []string{"walk-new", "--dry-run"}},
	{path: "topic.delete", args: []string{walkOther, "--dry-run"}},
	{path: "topic.trim-prefix", args: []string{walkTopic, "-o", "1"}},
}

// walkthroughProfileSteps are the profile mutators, run in this order against
// a config file of their own, since each depends on what the one before
// wrote. Each prints a {profile, path, current} document and exits 0.
var walkthroughProfileSteps = []struct {
	path string
	args []string
}{
	{path: "profile.create", args: []string{"walk2", "-B", "localhost:9"}},
	{path: "profile.use", args: []string{"walk2"}},
	{path: "profile.set", args: []string{"-X", "dial_timeout=2s"}},
	{path: "profile.rename", args: []string{"walk2", "walk3"}},
	{path: "profile.delete", args: []string{"walk3"}},
}

func TestWalkthrough(t *testing.T) {
	t.Run("every leaf is listed or skipped", testEveryLeafClassified)

	w := newWalkthrough(t)
	for _, leaf := range walkthroughLeaves {
		t.Run(leaf.path, func(t *testing.T) {
			t.Parallel()
			args := slices.Concat(strings.Split(leaf.path, "."), w.fillArgs(leaf.args))

			text := w.run(t, leaf.stdin, slices.Concat(args, []string{"--format", "text"}))
			w.check(t, leaf.path, "text", leaf.exit, leaf.why, text)
			if text.code == out.ExitOK && strings.TrimSpace(text.stdout) == "" {
				w.errf(t, leaf.path, "text", text, "exit 0 with nothing on stdout")
			}

			js := w.run(t, leaf.stdin, slices.Concat(args, []string{"--format", "json"}))
			if w.check(t, leaf.path, "json", leaf.exit, leaf.why, js) {
				w.checkJSON(t, leaf.path, js, false)
			}

			awk := w.run(t, leaf.stdin, slices.Concat(args, []string{"--format", "awk"}))
			if w.check(t, leaf.path, "awk", leaf.exit, leaf.why, awk) {
				w.checkAWK(t, leaf.path, text.stdout, awk, w.awkHeader(t, leaf.path, args))
			}
		})
	}
	for _, m := range walkthroughMutations {
		t.Run(m.path, func(t *testing.T) {
			t.Parallel()
			args := slices.Concat(strings.Split(m.path, "."), m.args)

			text := w.run(t, "", slices.Concat(args, []string{"--format", "text"}))
			if w.check(t, m.path, "text", out.ExitOK, "", text) && strings.TrimSpace(text.stdout) == "" {
				w.errf(t, m.path, "text", text, "exit 0 with nothing on stdout")
			}

			js := w.run(t, "", slices.Concat(args, []string{"--format", "json"}))
			if w.check(t, m.path, "json", out.ExitOK, "", js) {
				w.checkJSON(t, m.path, js, true)
			}

			awk := w.run(t, "", slices.Concat(args, []string{"--format", "awk"}))
			if w.check(t, m.path, "awk", out.ExitOK, "", awk) {
				w.checkAWK(t, m.path, text.stdout, awk, w.awkHeader(t, m.path, args))
			}
		})
	}
	for _, format := range []string{"text", "json", "awk"} {
		t.Run("profile."+format, func(t *testing.T) {
			t.Parallel()
			cfgPath := filepath.Join(t.TempDir(), "config.toml")
			for _, step := range walkthroughProfileSteps {
				args := slices.Concat([]string{"--config-path", cfgPath}, strings.Split(step.path, "."), step.args, []string{"--format", format})
				r := w.runArgs(t, "", args)
				if !w.check(t, step.path, format, out.ExitOK, "", r) {
					return
				}
				switch format {
				case "json":
					w.checkJSON(t, step.path, r, false)
					var doc map[string]any
					json.Unmarshal([]byte(r.stdout), &doc)
					for _, key := range []string{"profile", "path", "current"} {
						if _, ok := doc[key]; !ok {
							w.errf(t, step.path, format, r, "document has no %q", key)
						}
					}
				case "awk":
					w.checkAWK(t, step.path, "", r, w.awkHeader(t, step.path, args))
				}
			}
		})
	}
}

// testEveryLeafClassified names any command the two lists above miss. Hidden
// and deprecated commands are skipped by rule: they exist so that an old
// script keeps working, and the command they forward to is walked.
func testEveryLeafClassified(t *testing.T) {
	classified := make(map[string]bool)
	for _, leaf := range walkthroughLeaves {
		classified[leaf.path] = true
	}
	for _, m := range walkthroughMutations {
		if classified[m.path] {
			t.Errorf("%s is both walked and mutated", m.path)
		}
		classified[m.path] = true
	}
	for _, step := range walkthroughProfileSteps {
		classified[step.path] = true
	}
	for _, skip := range walkthroughSkips {
		if skip.why == "" {
			t.Errorf("%s is skipped with no reason", skip.path)
		}
		if classified[skip.path] {
			t.Errorf("%s is both walked and skipped", skip.path)
		}
		classified[skip.path] = true
	}

	var hidden func(*cobra.Command) bool
	hidden = func(cmd *cobra.Command) bool {
		return cmd.Hidden || cmd.Deprecated != "" || cmd.HasParent() && hidden(cmd.Parent())
	}

	root, _ := buildRoot()
	var leaves int
	allCommands(root, func(cmd *cobra.Command) {
		if cmd.HasSubCommands() || hidden(cmd) {
			return
		}
		leaves++
		path := out.CommandName(cmd.CommandPath())
		if !classified[path] {
			t.Errorf("%s is in neither list; add it to walkthroughLeaves, or to walkthroughSkips with a reason", path)
		}
		delete(classified, path)
	})
	for path := range classified {
		t.Errorf("%s is listed but is not a leaf of the tree", path)
	}
	if leaves == 0 {
		t.Error("no leaves found; the walk is not reaching them")
	}
}

type walkthrough struct {
	exe        string
	cfgPath    string
	schemaFile string
}

// newWalkthrough seeds one kfake cluster and one srfake registry with enough
// to read back: topics carrying records, a group with committed offsets, and
// a registered schema. It writes a config file naming both, so that every
// command reaches them the way a profile does.
func newWalkthrough(t *testing.T) *walkthrough {
	t.Helper()

	c, err := kfake.NewCluster()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	reg := srfake.New()
	t.Cleanup(reg.Close)

	kcl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kcl.Close)

	ctx := t.Context()
	adm := kadm.NewClient(kcl)
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, walkTopic, walkOther); err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{walkTopic, walkOther} {
		for i := range walkRecords {
			r := &kgo.Record{
				Topic: topic,
				Key:   fmt.Appendf(nil, "id-%d", i),
				Value: fmt.Appendf(nil, `{"id":"id-%d","count":%d}`, i, i),
			}
			if res := kcl.ProduceSync(ctx, r); res.FirstErr() != nil {
				t.Fatal(res.FirstErr())
			}
		}
	}

	var offsets kadm.Offsets
	offsets.Add(kadm.Offset{Topic: walkTopic, Partition: 0, At: walkRecords, LeaderEpoch: -1})
	resp, err := adm.CommitOffsets(ctx, walkGroup, offsets)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Error(); err != nil {
		t.Fatal(err)
	}

	const schema = `{"type":"record","name":"Demo","fields":[{"name":"id","type":"string"},{"name":"count","type":"int"}]}`
	scl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scl.CreateSchema(ctx, walkSubject, sr.Schema{Schema: schema, Type: sr.TypeAvro}); err != nil {
		t.Fatal(err)
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	w := &walkthrough{
		exe:        exe,
		cfgPath:    filepath.Join(dir, "config.toml"),
		schemaFile: filepath.Join(dir, "schema.avsc"),
	}
	cfg := fmt.Sprintf(`current_profile = %q

[profiles.%s]
seed_brokers = [%q]

[profiles.%s.registry]
urls = [%q]
`, walkProfile, walkProfile, c.ListenAddrs()[0], walkProfile, reg.URL())
	if err := os.WriteFile(w.cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(w.schemaFile, []byte(schema), 0o600); err != nil {
		t.Fatal(err)
	}
	return w
}

// fillArgs replaces the placeholders a table cannot know at run time.
func (w *walkthrough) fillArgs(args []string) []string {
	out := slices.Clone(args)
	for i, a := range out {
		if a == schemaFileArg {
			out[i] = w.schemaFile
		}
	}
	return out
}

type runResult struct {
	stdout string
	stderr string
	code   int
}

// run runs one kcl in a child process, pointed at the seeded cluster.
func (w *walkthrough) run(t *testing.T, stdin string, args []string) runResult {
	t.Helper()
	return w.runArgs(t, stdin, slices.Concat([]string{"--config-path", w.cfgPath}, args))
}

// runArgs runs one kcl in a child process with exactly these arguments.
func (w *walkthrough) runArgs(t *testing.T, stdin string, full []string) runResult {
	t.Helper()

	enc, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, w.exe)
	// A KCL_ variable in the environment this test was run from would
	// override the config we just wrote, so the child gets none of them.
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "KCL_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, walkthroughEnv+"="+string(enc))
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	r := runResult{}
	err = cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		r.code = exit.ExitCode()
	default:
		t.Fatalf("unable to run kcl %s: %v", strings.Join(full, " "), err)
	}
	r.stdout, r.stderr = stdout.String(), stderr.String()
	return r
}

// awkHeader is the row --format awk-header prints for the command args
// name, as its fields, or nil for a command that registered no table. It
// runs in a child like everything else, so it is what a script would see.
func (w *walkthrough) awkHeader(t *testing.T, path string, args []string) []string {
	t.Helper()
	r := w.runArgs(t, "", slices.Concat(args, []string{"--format", "awk-header"}))
	if r.code != out.ExitOK || r.stderr != "" {
		w.errf(t, path, "awk", r, "--format awk-header: exit %d, want 0 and nothing on stderr", r.code)
		return nil
	}
	if r.stdout == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(r.stdout, "\n"), "\t")
}

// check reports the exit code, returning whether it is the one we expect.
func (w *walkthrough) check(t *testing.T, path, format string, want int, why string, r runResult) bool {
	t.Helper()
	if r.code == want {
		return true
	}
	if why != "" {
		w.errf(t, path, format, r, "exit %d, want %d (%s)", r.code, want, why)
	} else {
		w.errf(t, path, format, r, "exit %d, want %d", r.code, want)
	}
	return false
}

// checkJSON pins the contract every JSON document carries: one line, the
// command that printed it, and "dry_run":true when the run was one.
func (w *walkthrough) checkJSON(t *testing.T, path string, r runResult, dryRun bool) {
	t.Helper()
	body := strings.TrimSuffix(r.stdout, "\n")
	if body == "" {
		w.errf(t, path, "json", r, "nothing on stdout")
		return
	}
	if strings.Contains(body, "\n") {
		w.errf(t, path, "json", r, "stdout is %d lines, want one", strings.Count(body, "\n")+1)
		return
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		w.errf(t, path, "json", r, "stdout is not JSON: %v", err)
		return
	}
	if _, ok := doc["_version"]; !ok {
		w.errf(t, path, "json", r, "document has no _version")
	}
	if got, _ := doc["_command"].(string); got != path {
		w.errf(t, path, "json", r, "_command is %q, want %q", got, path)
	}
	if dryRun && doc["dry_run"] != true {
		w.errf(t, path, "json", r, "dry_run is %v, want true", doc["dry_run"])
	}
}

// checkAWK pins the scripting contract: every row carries the same fields,
// as many as the header --format awk-header prints for the command, and the header
// the text format prints is not one of them. A command that prints rows must
// have registered its columns, so that a script can learn them.
func (w *walkthrough) checkAWK(t *testing.T, path, text string, r runResult, header []string) {
	t.Helper()
	body := strings.TrimSuffix(r.stdout, "\n")
	if body == "" {
		return
	}
	rows := strings.Split(body, "\n")
	want := strings.Count(rows[0], "\t") + 1
	if header == nil && strings.HasPrefix(body, "{") {
		// misc raw-req has no table: a raw response is a JSON document in
		// every format.
		return
	}
	if header == nil {
		w.errf(t, path, "awk", r, "prints rows but --format awk-header prints nothing; register the columns with out.Columns")
	} else if len(header) != want {
		w.errf(t, path, "awk", r, "--format awk-header has %d fields, row 0 has %d: %q vs %q", len(header), want, header, rows[0])
	}
	headers := headerLines(text)
	for i, row := range rows {
		if got := strings.Count(row, "\t") + 1; got != want {
			w.errf(t, path, "awk", r, "row %d has %d fields, row 0 has %d: %q", i, got, want, row)
		}
		if slices.Contains(headers, strings.Join(strings.Fields(row), " ")) {
			w.errf(t, path, "awk", r, "row %d is a header the text format prints: %q", i, row)
		}
	}
}

// headerLines are the table headers the text format printed, as their words:
// the lines with every letter capitalized, which is what a header is and what
// a row of data is not. A command that prints sections has one per section.
func headerLines(text string) []string {
	var headers []string
	for line := range strings.SplitSeq(text, "\n") {
		if line == "" || line != strings.ToUpper(line) || !strings.ContainsFunc(line, unicode.IsLetter) {
			continue
		}
		headers = append(headers, strings.Join(strings.Fields(line), " "))
	}
	return headers
}

// errf reports one failure and keeps going, so that one run lists everything
// that drifted rather than the first thing.
func (w *walkthrough) errf(t *testing.T, path, format string, r runResult, msg string, args ...any) {
	t.Helper()
	stdout, _, _ := strings.Cut(strings.TrimSuffix(r.stdout, "\n"), "\n")
	stderr, _, _ := strings.Cut(strings.TrimSuffix(r.stderr, "\n"), "\n")
	t.Errorf("kcl %s --format %s: %s\n  stdout: %s\n  stderr: %s",
		strings.ReplaceAll(path, ".", " "), format, fmt.Sprintf(msg, args...), stdout, stderr)
}
