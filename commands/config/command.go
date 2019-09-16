// Package config contains config file related subcommands.
package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "kcl configuration commands",
	}

	cmd.AddCommand(cfgDumpCommand(cl))
	cmd.AddCommand(cfgHelpCommand(cl))
	cmd.AddCommand(cfgUseCommand(cl))
	cmd.AddCommand(cfgClearCommand(cl))
	cmd.AddCommand(cfgListCommand(cl))

	return cmd
}

func cfgDumpCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "dump",
		Short: "dump the loaded configuration",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			toml.NewEncoder(os.Stdout).Encode(cl.DiskCfg())
		},
	}
}

func cfgHelpCommand(cl *client.Client) *cobra.Command {
	var configHelp = `kcl takes configuration options by default from ` + cl.DefaultCfgPath() + `.
The config path can be set with --config-path, and --no-config (-Z) can be used
to disable loading a config file entirely.

To show the configuration that kcl is running with, run 'kcl misc dump-config'.

The repeatable -X flag allows for specifying config options directly. Any flag
set option has higher precedence over config file options.

Options are described below, with examples being how they would look in a
config.toml. Overrides generally look the same, but quotes can be dropped and
arrays do not use brackets (-X foo=bar,baz).

OPTIONS

  seed_brokers=["localhost", "127.0.0.1:9092"]
     An inital set of brokers to use for connecting to your Kafka cluster.

  timeout_ms=1000
     Timeout to use for any command that takes a timeout.

  tls_ca_cert_path="/path/to/my/ca.cert"
     Path to a CA cert to load and use for connecting to brokers over TLS.

  tls_client_cert_path="/path/to/my/ca.cert"
     Path to a client cert to load and use for connecting to brokers over TLS.
     This must be paired with tls_client_key_path.

  tls_client_key_path="/path/to/my/ca.cert"
     Path to a client key to load and use for connecting to brokers over TLS.
     This must be paired with tls_client_cert_path.

  tls_server_name="127.0.0.1"
     Server name to use for connecting to brokers over TLS.

`
	return &cobra.Command{
		Use:   "help",
		Short: "describe kcl config semantics",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println(configHelp)
		},
	}
}

func cfgUseCommand(cl *client.Client) *cobra.Command {
	dir := filepath.Dir(cl.DefaultCfgPath())

	return &cobra.Command{
		Use:   "use NAME",
		Short: "Link a config in " + dir + " to " + cl.DefaultCfgPath(),
		Long: `Link a config in ` + dir + ` to ` + cl.DefaultCfgPath() + `.

This command allows you to easily swap kcl configs. It will look for
any file NAME or NAME.toml (favoring .toml) in the config directory
and link it to the default config path.

This dies if NAME does not yet exist or on any other os errors.

If asked to use config "none", this simply remove an existing symlink.
`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if cl.DefaultCfgPath() == "" {
				out.Die("cannot use a config; unable to determine home dir")
			}

			existing, err := os.Lstat(cl.DefaultCfgPath())
			if err != nil {
				if !os.IsNotExist(err) {
					out.Die("stat err for existing config path %q: %v", cl.DefaultCfgPath(), err)
				}
				// not exists: we can create symlink
			} else {
				if existing.Mode()&os.ModeSymlink == 0 {
					out.Die("stat shows that existing config at %q is not a symlink", cl.DefaultCfgPath())
				}
			}

			dirents, err := ioutil.ReadDir(dir)
			out.MaybeDie(err, "unable to read config dir %q: %v", dir, err)

			use := args[0]
			exact := strings.HasSuffix(use, ".toml")
			found := false
			for _, dirent := range dirents {
				if exact && dirent.Name() == use {
					found = true
					break
				}

				// With inexact matching, we favor
				// foo.toml over foo if both exist.
				noExt := strings.TrimSuffix(dirent.Name(), ".toml")
				if noExt == use {
					found = true
					if len(dirent.Name()) > len(use) {
						use = dirent.Name()
					}
				}
			}

			if !found {
				out.Die("could not find requested config %q", args[0])
			}

			if existing != nil {
				if err := os.Remove(cl.DefaultCfgPath()); err != nil {
					out.Die("unable to remove old symlink at %q: %v", cl.DefaultCfgPath(), err)
				}
			}

			src := filepath.Join(dir, use)
			if err := os.Symlink(src, cl.DefaultCfgPath()); err != nil {
				out.Die("unable to create symlink from %q to %q: %v", src, cl.DefaultCfgPath(), err)
			}

			fmt.Printf("linked %q to %q\n", src, cl.DefaultCfgPath())
		},
	}
}

func cfgClearCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "clear",
		Short: "Remove " + cl.DefaultCfgPath() + " if it is a symlink",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			if cl.DefaultCfgPath() == "" {
				out.Die("cannot use a config; unable to determine config dir")
			}

			existing, err := os.Lstat(cl.DefaultCfgPath())
			if err != nil {
				if !os.IsNotExist(err) {
					out.Die("stat err for existing config path %q: %v", cl.DefaultCfgPath(), err)
				}
				fmt.Printf("no symlink found at %q\n", cl.DefaultCfgPath())
				return
			}

			if existing.Mode()&os.ModeSymlink == 0 {
				out.Die("stat shows that existing config at %q is not a symlink", cl.DefaultCfgPath())
			}

			if err := os.Remove(cl.DefaultCfgPath()); err != nil {
				out.Die("unable to remove symlink at %q: %v", cl.DefaultCfgPath(), err)
			}
			fmt.Printf("cleared config symlink %q\n", cl.DefaultCfgPath())
		},
	}
}

func cfgListCommand(cl *client.Client) *cobra.Command {
	dir := filepath.Dir(cl.DefaultCfgPath())

	return &cobra.Command{
		Use:   "lsdir",
		Short: "List all files in configuration directory" + cl.DefaultCfgPath(),
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			dirents, err := ioutil.ReadDir(dir)
			out.MaybeDie(err, "unable to read config dir %q: %v", dir, err)
			for _, dirent := range dirents {
				fmt.Println(dirent.Name())
			}
		},
	}
}
