// Package myconfig contains kcl config file related subcommands.
package myconfig

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
		Use:   "myconfig",
		Short: "kcl configuration commands",
	}

	cmd.AddCommand(linkCommand(cl))
	cmd.AddCommand(unlinkCommand(cl))
	cmd.AddCommand(dumpCommand(cl))
	cmd.AddCommand(helpCommand(cl))
	cmd.AddCommand(listCommand(cl))

	return cmd
}

func dumpCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "dump",
		Short: "dump the loaded configuration",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			toml.NewEncoder(os.Stdout).Encode(cl.DiskCfg())
		},
	}
}

func helpCommand(cl *client.Client) *cobra.Command {
	var configHelp = `On your machine, kcl takes configuration options by default from:

  ` + cl.DefaultCfgPath() + `

The config path can be set with --config-path, while --no-config disables
loading a config file entirely. To show the configuration that kcl is running
with, use the dump command.

The repeatable -X flag allows for specifying config options directly. Any flag
set option has higher precedence over config file options.

Options are described below, with examples being how they would look in a
config.toml. Overrides generally look the same, but quotes can be dropped and
arrays do not use brackets (-X foo=bar,baz).

For overrides of an option in a nested section, lowercase and underscore-suffix
any option you want to override. For example, tls_ca_cert_path overrides
ca_cert_path in the tls section.

Environment variables can be set to override the config as well. Environment
variables take middle priority (higher than the config file, lower than config
file overrides from the -X flag). The default environment variable prefix is
KCL_ but can be overridden with the --config-env-prefix flag.  Environment
variable overrides operate similarly to the -X flag, but are all uppercase. For
example, KCL_TLS_CA_CERT_PATH overrides ca_cert_path in the tls section.

For tls specifically, one additional option exists for flags/environment
variables: use_tls=true or KCL_USE_TLS with any value. This will opt in to
using TLS without any additional options (ca cert, client certs, server name).
This would be used if you are connecting to brokers that are using certs from
well known CAs.

OPTIONS

  seed_brokers=["localhost", "127.0.0.1:9092"]
     An inital set of brokers to use for connecting to your Kafka cluster.

  timeout_ms=1000
     Timeout to use for any command that takes a timeout.

The [tls] section

  ca_cert_path="/path/to/my/ca.cert"
     Path to a CA cert to load and use for connecting to brokers over TLS.

  client_cert_path="/path/to/my/ca.cert"
     Path to a client cert to load and use for connecting to brokers over TLS.
     This must be paired with tls_client_key_path.

  client_key_path="/path/to/my/ca.cert"
     Path to a client key to load and use for connecting to brokers over TLS.
     This must be paired with tls_client_cert_path.

  server_name="127.0.0.1"
     Server name to use for connecting to brokers over TLS.

The [sasl] section

  method="scram_sha_256"
     SASL method to use. Must be paired with sasl_user and sasl_pass.
     Possible values are "plaintext", "scram-sha-256", or "scram-sha-512".
     Dashes and underscores are stripped.

  zid="zid"
  user="user"
  pass="pass"
     SASL authzid (always optional), user, and pass.

  is_token=false
     Specifies that the sasl user and pass came from a delegation token.
     This is only relevant for scram methods.
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

func linkCommand(cl *client.Client) *cobra.Command {
	dir := filepath.Dir(cl.DefaultCfgPath())

	return &cobra.Command{
		Use:     "link NAME",
		Aliases: []string{"use"},

		Short: "Link a config in " + dir + " to " + cl.DefaultCfgPath(),
		Long: `Link a config in ` + dir + ` to ` + cl.DefaultCfgPath() + `.

This command allows you to easily swap kcl configs. It will look for any file
NAME or NAME.toml (favoring .toml) in the config directory and link it to the
default config path.
`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if cl.DefaultCfgPath() == "" {
				out.Die("cannot link config; unable to determine home dir")
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

func unlinkCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "unlink",
		Aliases: []string{"clear"},
		Short:   "Remove " + cl.DefaultCfgPath() + " if it is a symlink",
		Args:    cobra.ExactArgs(0),
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
			fmt.Printf("unlinked config symlink %q\n", cl.DefaultCfgPath())
		},
	}
}

func listCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all files in configuration directory" + cl.DefaultCfgPath(),
		Args:    cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			dir := filepath.Dir(cl.DefaultCfgPath())
			dirents, err := ioutil.ReadDir(dir)
			out.MaybeDie(err, "unable to read config dir %q: %v", dir, err)
			for _, dirent := range dirents {
				fmt.Println(dirent.Name())
			}
		},
	}
}
