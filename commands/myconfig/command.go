// Package myconfig contains kcl config/profile related subcommands.
package myconfig

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
)

// Command returns the "profile" command (the primary config interface).
func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage connection profiles (use, list, create, dump, rename, delete).",
	}

	cmd.AddCommand(
		useCommand(cl),
		listCommand(cl),
		currentCommand(cl),
		createCommand(cl),
		dumpCommand(cl),
		renameCommand(cl),
		deleteCommand(cl),
		configHelpCommand(cl),
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
		dumpCommand(cl),
		renameCommand(cl),
		deleteCommand(cl),
		configHelpCommand(cl),
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
			fmt.Printf("Switched to profile %q\n", name)
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
				fmt.Println("No profiles configured. Config uses flat format.")
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
				fmt.Println("(no profile set)")
			} else {
				fmt.Println(cfgFile.CurrentProfile)
			}
			return nil
		},
	}
}

func createCommand(cl *client.Client) *cobra.Command {
	var noHelp bool
	cmd := &cobra.Command{
		Use:     "create",
		Aliases: []string{"setup", "wizard"},
		Short:   "Interactive configuration setup",
		Args:    cobra.MaximumNArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			client.Wizard(noHelp)
		},
	}
	cmd.Flags().BoolVar(&noHelp, "no-help", false, "disable help text (only prompts will print)")
	return cmd
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
			fmt.Printf("Renamed profile %q to %q\n", oldName, newName)
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
			fmt.Printf("Deleted profile %q\n", name)
			return nil
		},
	}
}

func configHelpCommand(cl *client.Client) *cobra.Command {
	configHelp := `On your machine, kcl takes configuration options by default from:

  ` + cl.DefaultCfgPath() + `

The config path can be set with --config-path, while --no-config-file disables
loading a config file entirely (as well as KCL_NO_CONFIG_FILE being non-empty).
To show the configuration that kcl is running with, use the dump command.

Three environment variables can be used to override default file path
semantics: KCL_CONFIG_DIR, KCL_CONFIG_FILE, and KCL_CONFIG_PATH. The first
changes the directory searched, the middle changes the file name used, and the
last overrides the former two (as a shortcut for setting both).

The repeatable -X flag allows for specifying config options directly. Any flag
set option has higher precedence over config file options.

OPTIONS

  seed_brokers=["localhost", "127.0.0.1:9092"]
  timeout_ms=1000

The [tls] section: ca_cert_path, client_cert_path, client_key_path,
server_name, min_version, cipher_suites, curve_preferences, insecure.

The [sasl] section: method (plain, scram-sha-256, scram-sha-512,
aws_msk_iam), zid, user, pass, is_token.

See kcl documentation for full details on each option.
`
	return &cobra.Command{
		Use:   "config-help",
		Short: "Describe config file options",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println(configHelp)
		},
	}
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
			fmt.Printf("linked %q to %q\n", src, cl.DefaultCfgPath())
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
					fmt.Printf("no symlink found at %q\n", cl.DefaultCfgPath())
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
			fmt.Printf("unlinked config symlink %q\n", cl.DefaultCfgPath())
			return nil
		},
	}
}
