package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"unicode"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/commands/admin"
	"github.com/twmb/kcl/commands/admin/acl"
	"github.com/twmb/kcl/commands/admin/clientmetrics"
	"github.com/twmb/kcl/commands/admin/clientquotas"
	"github.com/twmb/kcl/commands/admin/configs"
	"github.com/twmb/kcl/commands/admin/dtoken"
	"github.com/twmb/kcl/commands/admin/group"
	"github.com/twmb/kcl/commands/admin/logdirs"
	"github.com/twmb/kcl/commands/admin/partas"
	"github.com/twmb/kcl/commands/admin/sharegroup"
	"github.com/twmb/kcl/commands/admin/topic"
	"github.com/twmb/kcl/commands/admin/txn"
	"github.com/twmb/kcl/commands/admin/userscram"
	"github.com/twmb/kcl/commands/cluster"
	"github.com/twmb/kcl/commands/consume"
	"github.com/twmb/kcl/commands/fake"
	"github.com/twmb/kcl/commands/metadata"
	"github.com/twmb/kcl/commands/misc"
	"github.com/twmb/kcl/commands/myconfig"
	"github.com/twmb/kcl/commands/produce"
	"github.com/twmb/kcl/commands/registry"
	"github.com/twmb/kcl/out"
)

// version is set via ldflags at build time:
//
//	go build -ldflags "-X main.version=v1.0.0"
//
// When unset (typical for `go install github.com/twmb/kcl@vX.Y.Z`),
// we fall back to runtime/debug.ReadBuildInfo so the module version
// recorded in the binary is used.
var version string

func resolveVersion() string {
	if version != "" {
		return version
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	// Tagged install: "v1.2.3". Use as-is.
	if v := bi.Main.Version; v != "" && v != "(devel)" && !isPseudoVersion(v) {
		return v
	}
	// Dirty or pseudo-version build: try to surface the VCS short sha
	// as "dev+abc1234" (and "+dirty" if the tree was dirty).
	var rev string
	var dirty bool
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if len(rev) >= 7 {
		v := "dev+" + rev[:7]
		if dirty {
			v += "-dirty"
		}
		return v
	}
	return "dev"
}

// isPseudoVersion returns true for Go module pseudo-versions
// (v0.0.0-20231231120000-abcdef123456, v1.2.3-0.20231231120000-abcdef123456,
// or any of the above with a "+dirty" suffix).
func isPseudoVersion(v string) bool {
	if strings.HasSuffix(v, "+dirty") {
		return true
	}
	// The fingerprint of a pseudo-version is an embedded 14-digit
	// UTC timestamp preceded by "-" or ".0." and followed by "-".
	for i := 0; i+14 < len(v); i++ {
		if (v[i] == '-' || v[i] == '.') && v[i+15] == '-' {
			allDigits := true
			for j := 1; j <= 14; j++ {
				if v[i+j] < '0' || v[i+j] > '9' {
					allDigits = false
					break
				}
			}
			if allDigits {
				return true
			}
		}
	}
	return false
}

// versionCommand prints what this kcl is: the version main resolves, and the
// build details the Go toolchain stamps into the binary. It touches no
// network.
func versionCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print the kcl version and build details.",
		Long: `Print the kcl version and build details.

The version is the release this binary was built as, or dev plus the commit
for a build from source. The git ref and build date come from the VCS stamp
the Go toolchain adds to a build, and are "-" when a build carries none,
such as a go install from the module proxy.

EXAMPLES:
  kcl version                      # aligned key and value lines
  kcl version --format json        # {version, git_ref, build_date, go_version, os_arch}
  kcl version --format awk         # one KEY<tab>value row per line
`,
		Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			details := buildDetails()
			switch cl.Format() {
			case out.FormatJSON:
				fields := make(map[string]any, len(details))
				for _, d := range details {
					fields[d.key] = d.value
				}
				out.MarshalJSON(cl.Command(), 1, fields)
			case out.FormatAWK:
				for _, d := range details {
					out.AwkRow(d.key, d.value)
				}
			default:
				tw := out.BeginTabWrite()
				for _, d := range details {
					fmt.Fprintf(tw, "%s\t%v\n", d.label, d.value)
				}
				tw.Flush()
			}
			return nil
		},
	}
	out.Columns(cmd, "KEY", "VALUE")
	return cmd
}

// buildDetail is one line of kcl version: the label text prints, the key JSON
// and awk print, and the value, which is out.Unknown for a detail the build
// does not carry.
type buildDetail struct {
	label string
	key   string
	value any
}

func buildDetails() []buildDetail {
	var ref, date any = out.Unknown, out.Unknown
	goVersion := runtime.Version()
	if bi, ok := debug.ReadBuildInfo(); ok {
		goVersion = bi.GoVersion
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				ref = s.Value
			case "vcs.time":
				date = s.Value
			}
		}
	}
	return []buildDetail{
		{"version", "version", resolveVersion()},
		{"git ref", "git_ref", ref},
		{"build date", "build_date", date},
		{"go version", "go_version", goVersion},
		{"os/arch", "os_arch", runtime.GOOS + "/" + runtime.GOARCH},
	}
}

// buildRoot builds the whole command tree with the client it shares.
func buildRoot() (*cobra.Command, *client.Client) {
	v := resolveVersion()
	client.SetVersion(v)
	if version == "" && v == "dev" {
		v = "kcl (development)"
	}

	root := &cobra.Command{
		Use:     "kcl",
		Short:   "Kafka Command Line command for commanding Kafka on the command line",
		Version: v,
		// Runtime errors (broker failures, protocol errors) should not
		// trigger the full cobra usage dump. Argument/flag parse errors
		// still print usage because they surface before RunE.
		SilenceUsage: true,
		Long: `A Kafka command line interface.

kcl is a Kafka swiss army knife that aims to enable Kafka administration,
message producing, and message consuming. If Kafka supports it, kcl aims
to provide it.

For help about configuration, run:
  kcl profile -h

To create a profile for a cluster:
  kcl profile create NAME -B host:9092

Command completion is available at:
  kcl misc gen-autocomplete
`,

		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}

	cl := client.New(root)

	// Keep metadata as hidden deprecated alias for cluster info.
	metadataCmd := metadata.Command(cl)
	metadataCmd.Deprecated = "use 'kcl cluster metadata' instead"
	metadataCmd.Hidden = true

	// Add hidden consume/produce aliases under topic.
	topicCmd := topic.Command(cl)
	topicConsume := consume.Command(cl)
	topicConsume.Deprecated = "use 'kcl consume' instead"
	topicConsume.Hidden = true
	topicProduce := produce.Command(cl)
	topicProduce.Deprecated = "use 'kcl produce' instead"
	topicProduce.Hidden = true
	topicCmd.AddCommand(topicConsume, topicProduce)

	root.AddCommand(
		consume.Command(cl),
		produce.Command(cl),
		registry.Command(cl),
		metadataCmd,
		misc.Command(cl),
		admin.Command(cl),
		myconfig.Command(cl),           // "profile" (primary)
		myconfig.DeprecatedCommand(cl), // "myconfig" (deprecated alias)

		// Resource commands (promoted from admin).
		topicCmd,
		group.Command(cl),
		sharegroup.Command(cl),
		cluster.Command(cl),
		acl.Command(cl),
		clientmetrics.Command(cl),
		configs.Command(cl),
		clientquotas.Command(cl),
		dtoken.Command(cl),
		logdirs.Command(cl),
		partas.Command(cl),
		userscram.Command(cl),
		txn.Command(cl),
		fake.Command(),
		versionCommand(cl),
	)

	allCommands(root, func(cmd *cobra.Command) {
		// Since we print usage on error, there is no reason to also
		// print an error on error.
		cmd.SilenceErrors = true
		// We do not want extra [flags] on every command.
		cmd.DisableFlagsInUseLine = true
	})

	root.SetUsageTemplate(usageTmpl)

	root.PersistentFlags().Bool("help-json", false, "dump the full command tree as JSON")

	// --help-json has to be answered before Execute, because cobra answers a
	// non-runnable command like the bare root with help before any hook runs.
	// We look for the flag in the arguments rather than parsing them: Execute
	// parses again, and slice flags such as -B append on every parse, so
	// parsing twice doubled every seed broker.
	usageErrors(root, func() error { _, err := cl.FlagCfg(); return err })

	// client.New registers the persistent pre-run that records the running
	// command and answers -X help and -X list.
	root.RegisterFlagCompletionFunc("config-opt", func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		return client.XCompletions(), cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
	})

	return root, cl
}

func main() {
	root, cl := buildRoot()

	if wantsHelpJSON(os.Args[1:]) {
		tree := helpJSON{Version: 1, commandJSON: buildCommandJSON(root, false)}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(tree)
		os.Exit(0)
	}

	if cmd, err := root.ExecuteC(); err != nil {
		out.HandleError(asUsageError(err), errFormat(root, cmd, cl), errCommand(cmd, cl))
	}
}

// errCommand is the _command an error document from a failed Execute
// carries: the name the client recorded, which a command reached through a
// hidden alias sets to its new path, or else cobra's path to the command,
// which is all we have when the arguments failed to validate before any hook
// ran. It is "" for a failure at the bare root.
func errCommand(cmd *cobra.Command, cl *client.Client) string {
	if name := cl.Command(); name != "" {
		return name
	}
	if cmd != nil {
		return out.CommandName(cmd.CommandPath())
	}
	return ""
}

// errFormat is the format to report a failed Execute in. Flag parsing stops
// at the first bad flag, so "kcl topic list --nosuchflag --format json" never
// reaches --format and cl still holds the default. When that happens we scan
// the arguments for the format you asked for, the same way wantsHelpJSON
// scans for --help-json.
//
// consume and produce own a --format of their own, the record format, which
// shadows the root's; a "--format json" in their arguments asks for JSON
// records, not a JSON error document, so for a leaf with a local format flag
// we do not scan.
func errFormat(root *cobra.Command, leaf *cobra.Command, cl *client.Client) string {
	if !root.PersistentFlags().Changed("format") && !ownsFormat(leaf) {
		if format := formatFromArgs(os.Args[1:]); format != "" {
			return format
		}
	}
	return cl.Format()
}

// ownsFormat reports whether cmd declares a --format flag of its own, which
// shadows the root's persistent one.
func ownsFormat(cmd *cobra.Command) bool {
	return cmd != nil && cmd.LocalNonPersistentFlags().Lookup("format") != nil
}

// usageErrors wraps every command's argument validator and the flag error
// handler so that cobra's own errors, a bad argument count or an unknown
// flag, exit 2 like every other usage error. A group with no Run of its own
// gets one that treats a stray argument as an unknown subcommand; cobra
// itself only reports those at the root, and answered "kcl profile nope"
// with the help text and exit 0. A bare group also runs checkFlags, so that
// "kcl -X hlep" reports the bad key rather than printing the help.
//
// The validator is skipped under --format awk-header: the header row does
// not depend on the arguments, and cobra validates them before the
// persistent pre-run that answers the format.
func usageErrors(root *cobra.Command, checkFlags func() error) {
	allCommands(root, func(cmd *cobra.Command) {
		if cmd.HasSubCommands() && !cmd.Runnable() {
			cmd.RunE = func(c *cobra.Command, args []string) error {
				if len(args) > 0 {
					return out.Errf(out.ExitUsage, "unknown command %q for %q", args[0], c.CommandPath())
				}
				if checkFlags != nil {
					if err := checkFlags(); err != nil {
						return out.Errf(out.ExitUsage, "%v", err)
					}
				}
				return c.Help()
			}
		}
		validate := cmd.Args
		if validate == nil {
			return
		}
		cmd.Args = func(c *cobra.Command, args []string) error {
			if format, _ := c.Flags().GetString("format"); format == out.FormatAwkHeader {
				return nil
			}
			if err := validate(c, args); err != nil {
				return out.Errf(out.ExitUsage, "%v", err)
			}
			return nil
		}
	})
	root.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		if hint := boolFlagValueHint(cmd, err); hint != "" {
			return out.Errf(out.ExitUsage, "%s", hint)
		}
		return out.Errf(out.ExitUsage, "%v", err)
	})
}

// boolFlagValueHint rewrites pflag's error for a boolean flag given a value,
// "--regex=PATTERN" on topic list, which pflag reports as a strconv.ParseBool
// failure. A flag that used to take the pattern and now marks the arguments
// as patterns is the case this is for, so the hint says where the value goes.
// It returns "" for any other error.
func boolFlagValueHint(cmd *cobra.Command, err error) string {
	msg := err.Error()
	if !strings.Contains(msg, "strconv.ParseBool") {
		return ""
	}
	// pflag: invalid argument "PATTERN" for "-r, --regex" flag: strconv.ParseBool: ...
	_, rest, ok := strings.Cut(msg, ` for "`)
	if !ok {
		return ""
	}
	names, _, ok := strings.Cut(rest, `" flag`)
	if !ok {
		return ""
	}
	name := names
	if _, long, ok := strings.Cut(names, ", "); ok {
		name = long
	}
	f := cmd.Flags().Lookup(strings.TrimPrefix(name, "--"))
	if f == nil || f.Value.Type() != "bool" {
		return ""
	}
	hint := fmt.Sprintf("flag %s takes no value", name)
	if f.Name == "regex" {
		hint += "; pass the pattern as an argument"
	}
	return hint
}

// asUsageError marks cobra's unknown command error, which no hook of ours
// can produce, as a usage error.
func asUsageError(err error) error {
	if err != nil && strings.HasPrefix(err.Error(), "unknown command ") {
		return out.Errf(out.ExitUsage, "%v", err)
	}
	return err
}

func allCommands(root *cobra.Command, fn func(*cobra.Command)) {
	for _, cmd := range root.Commands() {
		allCommands(cmd, fn)
	}
	fn(root)
}

// helpJSON is the --help-json document: the command tree, with _version so
// that a consumer can tell this shape from the next one.
type helpJSON struct {
	Version int `json:"_version"`
	commandJSON
}

type commandJSON struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Usage       string   `json:"usage,omitempty"`
	Aliases     []string `json:"aliases,omitempty"`
	Deprecated  string   `json:"deprecated,omitempty"`
	// Hidden is true for a command excluded from --help, and for every
	// command under one. Cobra hides a parent without marking its
	// children, so propagating here lets a consumer filter at any depth
	// rather than having to reason about whole subtrees.
	Hidden   bool                   `json:"hidden,omitempty"`
	Examples []string               `json:"examples,omitempty"`
	Flags    map[string]flagJSON    `json:"flags,omitempty"`
	Commands map[string]commandJSON `json:"commands,omitempty"`
}

type flagJSON struct {
	Short       string `json:"short,omitempty"`
	Type        string `json:"type"`
	Default     string `json:"default,omitempty"`
	Description string `json:"description"`
	// Hidden and Deprecated mark the old name of a renamed flag, kept so a
	// script keeps working, so that tooling can leave it out.
	Hidden     bool   `json:"hidden,omitempty"`
	Deprecated string `json:"deprecated,omitempty"`
}

func buildCommandJSON(cmd *cobra.Command, parentHidden bool) commandJSON {
	hidden := parentHidden || cmd.Hidden
	c := commandJSON{
		Name:        cmd.Name(),
		Description: cmd.Short,
		Deprecated:  cmd.Deprecated,
		Hidden:      hidden,
	}
	if cmd.Runnable() {
		c.Usage = cmd.UseLine()
	}
	if len(cmd.Aliases) > 0 {
		c.Aliases = cmd.Aliases
	}
	c.Examples = examples(cmd)

	// Flags.
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
		if f.Name == "help" || f.Name == "help-json" {
			return
		}
		fj := flagJSON{
			Type:        f.Value.Type(),
			Default:     f.DefValue,
			Description: f.Usage,
			Hidden:      f.Hidden,
			Deprecated:  f.Deprecated,
		}
		if f.Shorthand != "" {
			fj.Short = f.Shorthand
		}
		if c.Flags == nil {
			c.Flags = make(map[string]flagJSON)
		}
		c.Flags[f.Name] = fj
	})

	// Subcommands.
	for _, sub := range cmd.Commands() {
		if sub.Name() == "help" {
			continue
		}
		if c.Commands == nil {
			c.Commands = make(map[string]commandJSON)
		}
		c.Commands[sub.Name()] = buildCommandJSON(sub, hidden)
	}
	return c
}

// examples returns the command lines of the EXAMPLES: block in cmd's long
// help, the ones a user can paste, trimmed of their indent and with the
// comment that follows a command kept. The block runs to the next heading
// (SEE ALSO:) or the end of the help; its indented lines are the examples, a
// line that is only a comment is skipped, and a line ending in a backslash
// continues on the next. Every command writes its examples there rather than
// in cobra's Example field, so that the help reads in one order.
func examples(cmd *cobra.Command) []string {
	var lines []string
	var in, cont bool
	for line := range strings.SplitSeq(cmd.Long, "\n") {
		switch {
		case line == "EXAMPLES:":
			in = true
		case !in:
		case isHelpHeading(line):
			in = false
		case cont:
			// The rest of a command that ended in a backslash.
			cont = strings.HasSuffix(line, "\\")
			lines[len(lines)-1] += " " + strings.TrimSpace(strings.TrimSuffix(line, "\\"))
		case strings.HasPrefix(line, "  ") && !strings.HasPrefix(strings.TrimSpace(line), "#"):
			cont = strings.HasSuffix(line, "\\")
			lines = append(lines, strings.TrimSpace(strings.TrimSuffix(line, "\\")))
		}
	}
	return lines
}

// isHelpHeading reports whether line is a heading of the long help, such as
// EXAMPLES: or SEE ALSO:, capitals at the margin ending in a colon.
func isHelpHeading(line string) bool {
	if !strings.HasSuffix(line, ":") || line != strings.ToUpper(line) {
		return false
	}
	return strings.ContainsFunc(line, unicode.IsLetter)
}

const usageTmpl = `USAGE:{{if and .Runnable (not .HasAvailableSubCommands)}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

ALIASES:
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

EXAMPLES:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

SUBCOMMANDS:{{range .Commands}}{{if .IsAvailableCommand}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

FLAGS:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

GLOBAL FLAGS:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`

// wantsHelpJSON reports whether args ask for --help-json, following pflag's
// syntax for a boolean long flag: bare, or --help-json=VALUE with a boolean
// VALUE, last one wins. Arguments after a bare "--" are positional.
func wantsHelpJSON(args []string) bool {
	var want bool
	for _, a := range args {
		switch {
		case a == "--":
			return want
		case a == "--help-json":
			want = true
		case strings.HasPrefix(a, "--help-json="):
			b, err := strconv.ParseBool(strings.TrimPrefix(a, "--help-json="))
			want = err == nil && b
		}
	}
	return want
}

// formatFromArgs returns the format args ask for with --format, following
// pflag's syntax for a string long flag: --format VALUE or --format=VALUE,
// last one wins, and arguments after a bare "--" are positional. The flag has
// no shorthand, so there is no short form to look for. We return "" if
// --format is absent, has no value, or names a format we do not know, and the
// caller then keeps whatever format it already had.
func formatFromArgs(args []string) string {
	var format string
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--":
			return format
		case a == "--format":
			format = ""
			if i+1 < len(args) {
				i++
				format = knownFormat(args[i])
			}
		case strings.HasPrefix(a, "--format="):
			format = knownFormat(strings.TrimPrefix(a, "--format="))
		}
	}
	return format
}

// knownFormat returns v if it is a format out can print, otherwise "".
// awk-header counts: an error under it is reported as text, not as a JSON
// document.
func knownFormat(v string) string {
	switch v {
	case out.FormatText, out.FormatJSON, out.FormatAWK, out.FormatAwkHeader:
		return v
	}
	return ""
}
