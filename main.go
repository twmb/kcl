package main

import (
	"encoding/json"
	"os"
	"runtime/debug"
	"strings"

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
	"github.com/twmb/kcl/commands/metadata"
	"github.com/twmb/kcl/commands/misc"
	"github.com/twmb/kcl/commands/myconfig"
	"github.com/twmb/kcl/commands/produce"
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

func main() {
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

If this is your first time running kcl, you can create a configuration with:
  kcl profile create

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
	metadataCmd.Deprecated = "use 'kcl cluster info' instead"
	metadataCmd.Hidden = true

	// Add hidden consume/produce aliases under topic.
	topicCmd := topic.Command(cl)
	topicConsume := consume.Command(cl)
	topicConsume.Hidden = true
	topicProduce := produce.Command(cl)
	topicProduce.Hidden = true
	topicCmd.AddCommand(topicConsume, topicProduce)

	root.AddCommand(
		consume.Command(cl),
		produce.Command(cl),
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
	)

	allCommands(root, func(cmd *cobra.Command) {
		// Since we print usage on error, there is no reason to also
		// print an error on error.
		cmd.SilenceErrors = true
		// We do not want extra [flags] on every command.
		cmd.DisableFlagsInUseLine = true

		if cmd.HasParent() {
			name := strings.Split(cmd.Use, " ")[0]
			cmd.Example = strings.ReplaceAll(cmd.Example, name, cmd.Parent().CommandPath()+" "+name)
		}
	})

	root.SetUsageTemplate(usageTmpl)

	var helpJSON bool
	root.PersistentFlags().BoolVar(&helpJSON, "help-json", false, "dump the full command tree as JSON")

	// Handle --help-json: parse flags early and check before Execute.
	// This is needed because cobra processes help before PersistentPreRun.
	root.ParseFlags(os.Args[1:])
	if helpJSON {
		tree := buildCommandJSON(root)
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(tree)
		os.Exit(0)
	}

	if err := root.Execute(); err != nil {
		out.HandleError(err, cl.Format())
	}
}

func allCommands(root *cobra.Command, fn func(*cobra.Command)) {
	for _, cmd := range root.Commands() {
		allCommands(cmd, fn)
	}
	fn(root)
}

type commandJSON struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Usage       string                 `json:"usage,omitempty"`
	Aliases     []string               `json:"aliases,omitempty"`
	Deprecated  string                 `json:"deprecated,omitempty"`
	Examples    []string               `json:"examples,omitempty"`
	Flags       map[string]flagJSON    `json:"flags,omitempty"`
	Commands    map[string]commandJSON `json:"commands,omitempty"`
}

type flagJSON struct {
	Short       string `json:"short,omitempty"`
	Type        string `json:"type"`
	Default     string `json:"default,omitempty"`
	Description string `json:"description"`
}

func buildCommandJSON(cmd *cobra.Command) commandJSON {
	c := commandJSON{
		Name:        cmd.Name(),
		Description: cmd.Short,
		Deprecated:  cmd.Deprecated,
	}
	if cmd.Runnable() {
		c.Usage = cmd.UseLine()
	}
	if len(cmd.Aliases) > 0 {
		c.Aliases = cmd.Aliases
	}
	if cmd.Example != "" {
		for _, line := range strings.Split(strings.TrimSpace(cmd.Example), "\n") {
			line = strings.TrimSpace(line)
			if line != "" {
				c.Examples = append(c.Examples, line)
			}
		}
	}

	// Flags.
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
		if f.Name == "help" || f.Name == "help-json" {
			return
		}
		fj := flagJSON{
			Type:        f.Value.Type(),
			Default:     f.DefValue,
			Description: f.Usage,
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
		c.Commands[sub.Name()] = buildCommandJSON(sub)
	}
	return c
}

const usageTmpl = `USAGE:{{if .Runnable}}
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
