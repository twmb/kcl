package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

func init() {
	root.AddCommand(miscCmd())
}

func miscCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "misc",
		Short: "Miscellaneous utilities",
	}

	cmd.AddCommand(code2errCmd())
	cmd.AddCommand(metadataCmd())
	cmd.AddCommand(genAutocompleteCmd())
	cmd.AddCommand(dumpCfgCmd())
	cmd.AddCommand(helpCfgCmd())
	cmd.AddCommand(useCfgCmd())

	return cmd
}

func code2errCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "code2err CODE",
		Short: "Print the name for an error code",
		Args:  cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			code, err := strconv.Atoi(args[0])
			if err != nil {
				die("unable to parse error code: %v", err)
			}
			if code == 0 {
				fmt.Println("NONE")
				return
			}
			kerr := kerr.ErrorForCode(int16(code)).(*kerr.Error)
			fmt.Printf("%s\n%s\n", kerr.Message, kerr.Description)
		},
	}
}

func metadataCmd() *cobra.Command {
	var req kmsg.MetadataRequest
	var noTopics bool

	cmd := &cobra.Command{
		Use:   "metadata",
		Short: "Issue a metadata command and dump the results",
		ValidArgs: []string{
			"--no-topics",
			"--topics",
		},
		Run: func(_ *cobra.Command, _ []string) {
			if noTopics { // nil means everything, empty means nothing
				req.Topics = []string{}
			}

			kresp, err := client.Request(&req)
			maybeDie(err, "unable to get metadata: %v", err)
			if asJSON {
				dumpJSON(kresp)
			}
			resp := kresp.(*kmsg.MetadataResponse)
			if resp.ClusterID != nil {
				fmt.Printf("CLUSTER\n=======\n%s\n\n", *resp.ClusterID)
			}

			fmt.Printf("BROKERS\n=======\n")
			printBrokers(resp.ControllerID, resp.Brokers)

			fmt.Printf("\nTOPICS\n======\n")
			printTopics(resp.TopicMetadata, true) // detailed does not use tw so no need to reinit

		},
	}

	// TODO --broker to query a specific broker?
	cmd.Flags().BoolVar(&noTopics, "no-topics", false, "fetch only broker metadata, no topics")
	cmd.Flags().StringSliceVarP(&req.Topics, "topics", "t", nil, "list of topics to fetch (comma separated or repeated flag)")

	return cmd
}

func genAutocompleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "gen-autocomplete",
		Short: "Generates bash completion scripts",
		Long: `To load completion run

. <(kcl misc gen-autocomplete)

To configure your bash shell to load completions for each session add to your bashrc

# ~/.bashrc or ~/.profile
. <(kcl misc gen-autocomplete)
`,
		Run: func(_ *cobra.Command, _ []string) { root.GenBashCompletion(os.Stdout) },
	}
}

func dumpCfgCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "dump-config",
		Short: "dump the loaded configuration",
		Run: func(_ *cobra.Command, _ []string) {
			toml.NewEncoder(os.Stdout).Encode(cfg)
		},
	}
}

func helpCfgCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "help-config",
		Short: "describe kcl config semantics",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println(configHelp)
		},
	}
}

func useCfgCmd() *cobra.Command {
	dir := filepath.Dir(defaultCfgPath)

	return &cobra.Command{
		Use:   "use-config NAME",
		Short: "link a config in " + dir + " to " + defaultCfgPath,
		Long: `Link a config in ` + dir + ` to ` + defaultCfgPath + `.

This command allows you to easily swap kcl configs. It will look for
any file NAME or NAME.toml (favoring .toml) in the config directory
and link it to the default config path.

This dies if NAME does not yet exist or on any other os errors.

If asked to use config "clear", this simply remove an existing symlink.
`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if defaultCfgPath == "" {
				die("cannot use a config; unable to determine home dir: %v", defaultCfgPathErr)
			}

			existing, err := os.Lstat(defaultCfgPath)
			if err != nil {
				if !os.IsNotExist(err) {
					die("stat err for existing config path %q: %v", defaultCfgPath, err)
				}
				// not exists: we can create symlink
			} else {
				if existing.Mode()&os.ModeSymlink == 0 {
					die("stat shows that existing config at %q is not a symlink", defaultCfgPath)
				}

				if args[0] == "clear" {
					if err := os.Remove(defaultCfgPath); err != nil {
						die("unable to remove old symlink at %q: %v", defaultCfgPath, err)
					}
					fmt.Printf("cleared old config symlink %q", defaultCfgPath)
					return
				}
			}

			dirents, err := ioutil.ReadDir(dir)
			maybeDie(err, "unable to read config dir %q: %v", dir, err)

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
				die("could not find requested config %q", args[0])
			}

			if existing != nil {
				if err := os.Remove(defaultCfgPath); err != nil {
					die("unable to remove old symlink at %q: %v", defaultCfgPath, err)
				}
			}

			src := filepath.Join(dir, use)
			if err := os.Symlink(src, defaultCfgPath); err != nil {
				die("unable to create symlink from %q to %q: %v", src, defaultCfgPath, err)
			}

			fmt.Printf("linked %q to %q\n", src, defaultCfgPath)
		},
	}
}
