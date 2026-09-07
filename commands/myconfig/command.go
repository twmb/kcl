// Package myconfig contains kcl config/profile related subcommands.
package myconfig

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

// Command returns the "profile" command (the primary config interface).
func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage connection profiles.",
		Long:  configHelpText(cl),
	}

	cmd.AddCommand(
		useCommand(cl),
		listCommand(cl),
		currentCommand(cl),
		createCommand(cl),
		setupCommand(cl),
		setCommand(cl),
		keysCommand(cl),
		dumpCommand(cl),
		renameCommand(cl),
		deleteCommand(cl),
	)

	return cmd
}

// DeprecatedCommand returns a hidden "myconfig" alias for backward compat.
func DeprecatedCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:        "myconfig",
		Short:      "kcl configuration commands",
		Deprecated: "use 'kcl profile' instead",
		Hidden:     true,
	}

	cmd.AddCommand(
		useCommand(cl),
		listCommand(cl),
		currentCommand(cl),
		createCommand(cl),
		setupCommand(cl),
		setCommand(cl),
		keysCommand(cl),
		dumpCommand(cl),
		renameCommand(cl),
		deleteCommand(cl),
		linkCommand(cl),
		unlinkCommand(cl),
	)

	return cmd
}

func useCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "use NAME",
		Short: "Switch the active profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			cfgPath := cl.CfgFilePath()

			var cfgFile client.CfgFile
			if _, err := toml.DecodeFile(cfgPath, &cfgFile); err != nil {
				return fmt.Errorf("unable to read config: %v", err)
			}

			if len(cfgFile.Profiles) == 0 {
				return fmt.Errorf("config file has no profiles; add [profiles.NAME] sections to your config first")
			}
			if _, ok := cfgFile.Profiles[name]; !ok {
				return fmt.Errorf("profile %q not found; available: %v", name, profileNames(cfgFile))
			}

			cfgFile.CurrentProfile = name
			if err := writeCfgFile(cfgPath, cfgFile); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "Switched to profile %q\n", name)
			return nil
		},
	}
}

func listCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all profiles",
		Args:    cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			cfgPath := cl.CfgFilePath()

			var cfgFile client.CfgFile
			if _, err := toml.DecodeFile(cfgPath, &cfgFile); err != nil {
				return fmt.Errorf("unable to read config: %v", err)
			}

			if len(cfgFile.Profiles) == 0 {
				fmt.Fprintln(os.Stderr, "No profiles configured. Config uses flat format.")
				return nil
			}

			for _, n := range profileNames(cfgFile) {
				if n == cfgFile.CurrentProfile {
					fmt.Println("* " + n)
				} else {
					fmt.Println("  " + n)
				}
			}
			return nil
		},
	}
}

func currentCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "current",
		Short: "Print the active profile name",
		Args:  cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			cfgPath := cl.CfgFilePath()

			var cfgFile client.CfgFile
			if _, err := toml.DecodeFile(cfgPath, &cfgFile); err != nil {
				return fmt.Errorf("unable to read config: %v", err)
			}

			if cfgFile.CurrentProfile == "" {
				fmt.Fprintln(os.Stderr, "(no profile set)")
			} else {
				fmt.Println(cfgFile.CurrentProfile)
			}
			return nil
		},
	}
}

func createCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "create NAME",
		Short: "Create a profile from the -B, -X, and -R flags.",
		Long: `Create a profile from the -B, -X, and -R flags.

The profile is written as a [profiles.NAME] table in the config file,
creating the file if needed. Any -X key can be saved. If no profile is
current, the new one becomes current. Only flags are saved; KCL_*
environment variables are not read.

EXAMPLES:
  kcl profile create local -B localhost:9092
  kcl profile create prod -B k1:9093,k2:9093 -X tls.ca_cert_path=/etc/kafka/ca.pem -X sasl.method=scram-sha-256 -X sasl.user=me -X sasl.pass=secret
  kcl profile create sr -B localhost:9092 -R http://localhost:8081

SEE ALSO:
  kcl profile use      switch the current profile
  kcl profile list     list profiles
  kcl profile dump     show the configuration kcl is running with
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			cfg, err := cl.FlagCfg()
			if err != nil {
				return out.Errf(out.ExitUsage, "%v", err)
			}
			name, cfgPath := args[0], cl.CfgFilePath()
			current, err := createProfile(cfgPath, name, cfg)
			if err != nil {
				return err
			}
			if current {
				fmt.Fprintf(os.Stderr, "Created profile %q in %s; it is now current\n", name, cfgPath)
			} else {
				fmt.Fprintf(os.Stderr, "Created profile %q in %s; switch with: kcl profile use %s\n", name, cfgPath, name)
			}
			return nil
		},
	}
}

// setupCommand is the old name of create, kept working but out of the help.
func setupCommand(cl *client.Client) *cobra.Command {
	cmd := createCommand(cl)
	cmd.Use = "setup NAME"
	cmd.Hidden = true
	return cmd
}

func setCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "set",
		Short: "Set keys in a profile from the -B, -X, and -R flags.",
		Long: `Set keys in a profile from the -B, -X, and -R flags.

The current profile is changed unless -C names another; a config without
profiles is edited at the top level. The flags are the ones create builds a
profile from, so anything that works as a one-off override can be saved.
Nothing is written unless every flag parses.

EXAMPLES:
  kcl profile set -B k1:9092,k2:9092
  kcl -C prod profile set -X sasl.method=scram-sha-256 -X sasl.user=me -X sasl.pass=secret

SEE ALSO:
  kcl profile create   create a profile from the same flags
  kcl profile dump     show the configuration kcl is running with
`,
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			var keys []string
			where, err := setProfile(cl.CfgFilePath(), cl.ProfileName(), func(cfg *client.Cfg) error {
				var err error
				if keys, err = cl.ApplyFlags(cfg); err != nil {
					return err
				}
				if len(keys) == 0 {
					return errors.New("nothing to set; pass -X key=value, -B, or -R")
				}
				return nil
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "Set %s in %s\n", strings.Join(keys, ", "), where)
			return nil
		},
	}
}

// setProfile calls apply on the profile named name in the config file at
// path, on the current profile when name is empty, or on the top level of a
// config without profiles, and writes the result. An error from apply is a
// usage error and nothing is written. It returns what was edited.
func setProfile(path, name string, apply func(*client.Cfg) error) (string, error) {
	var cfgFile client.CfgFile
	md, err := toml.DecodeFile(path, &cfgFile)
	if os.IsNotExist(err) {
		return "", fmt.Errorf("no config file at %s; create a profile first with kcl profile create", path)
	}
	if err != nil {
		return "", fmt.Errorf("unable to read config: %v", err)
	}

	if len(cfgFile.Profiles) == 0 {
		if name != "" {
			return "", fmt.Errorf("profile %q not found; config file has no profiles", name)
		}
		if !isFlat(md, cfgFile) {
			return "", fmt.Errorf("config at %s has no profiles; create one first with kcl profile create", path)
		}
		if err := apply(&cfgFile.Cfg); err != nil {
			return "", out.Errf(out.ExitUsage, "%v", err)
		}
		if err := writeCfgFile(path, cfgFile); err != nil {
			return "", err
		}
		return path, nil
	}

	if name == "" {
		name = cfgFile.CurrentProfile
	}
	if name == "" {
		return "", fmt.Errorf("no current profile; pass -C NAME or run kcl profile use NAME")
	}
	p, ok := cfgFile.Profiles[name]
	if !ok {
		return "", fmt.Errorf("profile %q not found; available: %v", name, profileNames(cfgFile))
	}
	if err := apply(&p); err != nil {
		return "", out.Errf(out.ExitUsage, "%v", err)
	}
	cfgFile.Profiles[name] = p
	if err := writeCfgFile(path, cfgFile); err != nil {
		return "", err
	}
	return fmt.Sprintf("profile %q", name), nil
}

// isFlat reports whether a decoded config uses the flat single cluster
// layout: no profiles, and some key other than current_profile.
func isFlat(md toml.MetaData, cfgFile client.CfgFile) bool {
	if len(cfgFile.Profiles) > 0 {
		return false
	}
	for _, k := range md.Keys() {
		if k[0] != "current_profile" {
			return true
		}
	}
	return false
}

// createProfile adds cfg to the config file at path as [profiles.name],
// creating the file and its directory if needed. It reports whether the new
// profile became current, which happens when the file's current_profile is
// unset or names a profile that does not exist.
func createProfile(path, name string, cfg client.Cfg) (bool, error) {
	if name == "" {
		return false, out.Errf(out.ExitUsage, "profile name cannot be empty")
	}

	var cfgFile client.CfgFile
	md, err := toml.DecodeFile(path, &cfgFile)
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("unable to read config: %v", err)
	}

	// Adding a profile to the flat layout would silently stop its keys being
	// read, so leave the conversion to the user.
	if isFlat(md, cfgFile) {
		return false, fmt.Errorf("config at %s is a flat single-cluster config; move its keys under a [profiles.NAME] table and set current_profile, then retry", path)
	}
	if _, exists := cfgFile.Profiles[name]; exists {
		return false, fmt.Errorf("profile %q already exists", name)
	}

	if cfgFile.Profiles == nil {
		cfgFile.Profiles = make(map[string]client.Cfg)
	}
	cfgFile.Profiles[name] = cfg
	var current bool
	if _, ok := cfgFile.Profiles[cfgFile.CurrentProfile]; !ok {
		cfgFile.CurrentProfile = name
		current = true
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, fmt.Errorf("unable to create config directory: %v", err)
	}
	if err := writeCfgFile(path, cfgFile); err != nil {
		return false, err
	}
	return current, nil
}

func keysCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "keys",
		Short: "List every config key with its meaning.",
		Long: `List every config key with its meaning.

These are the keys that -X, KCL_<KEY> environment variables (dots become
underscores: KCL_SASL_USER), and the config file all share. A bool may be
given bare, -X tls.insecure, and an empty value unsets any key, -X sasl.pass=.
The table keys tls, sasl, registry, and registry.tls take only the empty
value and remove the whole table. A value may reference an environment
variable as ${NAME}; $${NAME} is a literal.

TIMEOUTS

  dial_timeout    caller side, per TCP dial attempt.
  retry_timeout   client side; gates whether to START a retry, not a wall
                  clock budget, so an in-flight attempt is not cancelled
                  when it elapses.
  broker_timeout  sent to the broker and enforced there, only on requests
                  that carry a TimeoutMs field.

  Keep dial_timeout <= broker_timeout <= retry_timeout. retry_timeout is
  only consulted when an attempt errors, so a slow but successful reply
  still succeeds and one retry can run to about twice broker_timeout in
  total. If dial_timeout is at or above retry_timeout, one failed dial uses
  the whole retry budget and nothing is retried.

EXAMPLES:
  kcl profile keys                         # table of keys
  kcl profile keys --format json           # for scripts

SEE ALSO:
  kcl profile set      set keys in a profile
  kcl profile dump     show the configuration kcl is running with
`,
		Args: cobra.NoArgs,
		Run: func(*cobra.Command, []string) {
			table := out.NewFormattedTable(cl.Format(), "profile.keys", 1, "keys", "KEY", "TYPE", "DESCRIPTION")
			for _, k := range client.CfgKeys() {
				table.Row(k.Name, k.Type, k.Desc)
			}
			table.Flush()
		},
	}
}

func dumpCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "dump",
		Short: "Dump the loaded configuration",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			toml.NewEncoder(os.Stdout).Encode(cl.DiskCfg())
		},
	}
}

func renameCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "rename OLD NEW",
		Short: "Rename a profile",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			oldName, newName := args[0], args[1]
			cfgPath := cl.CfgFilePath()

			var cfgFile client.CfgFile
			if _, err := toml.DecodeFile(cfgPath, &cfgFile); err != nil {
				return fmt.Errorf("unable to read config: %v", err)
			}

			cfg, ok := cfgFile.Profiles[oldName]
			if !ok {
				return fmt.Errorf("profile %q not found", oldName)
			}
			if _, exists := cfgFile.Profiles[newName]; exists {
				return fmt.Errorf("profile %q already exists", newName)
			}

			delete(cfgFile.Profiles, oldName)
			cfgFile.Profiles[newName] = cfg
			if cfgFile.CurrentProfile == oldName {
				cfgFile.CurrentProfile = newName
			}

			if err := writeCfgFile(cfgPath, cfgFile); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "Renamed profile %q to %q\n", oldName, newName)
			return nil
		},
	}
}

func deleteCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "delete NAME",
		Short: "Delete a profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			cfgPath := cl.CfgFilePath()

			var cfgFile client.CfgFile
			if _, err := toml.DecodeFile(cfgPath, &cfgFile); err != nil {
				return fmt.Errorf("unable to read config: %v", err)
			}

			if _, ok := cfgFile.Profiles[name]; !ok {
				return fmt.Errorf("profile %q not found", name)
			}

			delete(cfgFile.Profiles, name)
			if cfgFile.CurrentProfile == name {
				cfgFile.CurrentProfile = ""
			}

			if err := writeCfgFile(cfgPath, cfgFile); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "Deleted profile %q\n", name)
			return nil
		},
	}
}

func configHelpText(cl *client.Client) string {
	return `Manage connection profiles.

Profiles are [profiles.NAME] tables in the config file; current_profile names
the one in use, and -C picks another for one command. The file is read from:

  ` + cl.DefaultCfgPath() + `

--config-path or KCL_CONFIG_PATH move it, --no-config-file or a non-empty
KCL_NO_CONFIG_FILE skip it, and KCL_CONFIG_DIR and KCL_CONFIG_FILE set the
directory and file name separately.

PRIORITY (highest wins)

  1. -B, -X, and -R flags
  2. KCL_<KEY> environment variables, e.g. KCL_SASL_USER
  3. The profile, or the top level keys of a file without profiles
  4. Built-in defaults

Only keys that are set take effect at each level; a key written as zero is
zero. "kcl profile keys" lists every key, and "kcl profile dump" shows what
kcl is running with.

EXAMPLES:
  kcl profile create prod -B k1:9092,k2:9092 -X sasl.method=scram-sha-256 -X sasl.user=me -X sasl.pass='${KAFKA_PASS}'
  kcl -C prod topic list                   # one command against prod
  kcl profile use prod                     # every command against prod
  kcl profile set -X dial_timeout=2s       # change the current profile
`
}

func profileNames(cfgFile client.CfgFile) []string {
	names := make([]string, 0, len(cfgFile.Profiles))
	for n := range cfgFile.Profiles {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func writeCfgFile(path string, cfgFile client.CfgFile) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("unable to write config: %v", err)
	}
	defer f.Close()
	if err := toml.NewEncoder(f).Encode(cfgFile); err != nil {
		return fmt.Errorf("unable to encode config: %v", err)
	}
	return nil
}

// linkCommand is the legacy symlink-based context switching.
func linkCommand(cl *client.Client) *cobra.Command {
	dir := filepath.Dir(cl.DefaultCfgPath())
	return &cobra.Command{
		Use:        "link NAME",
		Short:      "Link a config file (deprecated: use 'profile use')",
		Deprecated: "use 'kcl profile use' instead",
		Hidden:     true,
		Args:       cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if cl.DefaultCfgPath() == "" {
				return fmt.Errorf("cannot link config; unable to determine home dir")
			}
			existing, err := os.Lstat(cl.DefaultCfgPath())
			if err != nil {
				if !os.IsNotExist(err) {
					return fmt.Errorf("stat err for existing config path %q: %v", cl.DefaultCfgPath(), err)
				}
			} else if existing.Mode()&os.ModeSymlink == 0 {
				return fmt.Errorf("existing config at %q is not a symlink", cl.DefaultCfgPath())
			}
			dirents, err := os.ReadDir(dir)
			if err != nil {
				return fmt.Errorf("unable to read config dir %q: %v", dir, err)
			}
			use := args[0]
			exact := strings.HasSuffix(use, ".toml")
			found := false
			for _, d := range dirents {
				if exact && d.Name() == use {
					found = true
					break
				}
				if strings.TrimSuffix(d.Name(), ".toml") == use {
					found = true
					if len(d.Name()) > len(use) {
						use = d.Name()
					}
				}
			}
			if !found {
				return fmt.Errorf("could not find requested config %q", args[0])
			}
			if existing != nil {
				os.Remove(cl.DefaultCfgPath())
			}
			src := filepath.Join(dir, use)
			if err := os.Symlink(src, cl.DefaultCfgPath()); err != nil {
				return fmt.Errorf("unable to symlink: %v", err)
			}
			fmt.Fprintf(os.Stderr, "linked %q to %q\n", src, cl.DefaultCfgPath())
			return nil
		},
	}
}

func unlinkCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:        "unlink",
		Short:      "Remove config symlink (deprecated: use 'profile use')",
		Deprecated: "use 'kcl profile use' instead",
		Hidden:     true,
		Args:       cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			existing, err := os.Lstat(cl.DefaultCfgPath())
			if err != nil {
				if os.IsNotExist(err) {
					fmt.Fprintf(os.Stderr, "no symlink found at %q\n", cl.DefaultCfgPath())
					return nil
				}
				return fmt.Errorf("stat err: %v", err)
			}
			if existing.Mode()&os.ModeSymlink == 0 {
				return fmt.Errorf("existing config at %q is not a symlink", cl.DefaultCfgPath())
			}
			if err := os.Remove(cl.DefaultCfgPath()); err != nil {
				return fmt.Errorf("unable to remove symlink: %v", err)
			}
			fmt.Fprintf(os.Stderr, "unlinked config symlink %q\n", cl.DefaultCfgPath())
			return nil
		},
	}
}
