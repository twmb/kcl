package myconfig

import (
	"fmt"
	"os"
	"sort"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func profileCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage config profiles (use, list, current, rename, delete).",
	}

	cmd.AddCommand(
		profileUseCommand(cl),
		profileListCommand(cl),
		profileCurrentCommand(cl),
		profileRenameCommand(cl),
		profileDeleteCommand(cl),
	)

	return cmd
}

func profileUseCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "use NAME",
		Short: "Switch the active profile",
		Args:  cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			name := args[0]
			cfgPath := cl.CfgFilePath()

			var cfgFile client.CfgFile
			if _, err := toml.DecodeFile(cfgPath, &cfgFile); err != nil {
				out.Die("unable to read config: %v", err)
			}

			if len(cfgFile.Profiles) == 0 {
				out.Die("config file has no profiles; use 'myconfig profile' after adding [profiles.NAME] sections to your config")
			}

			if _, ok := cfgFile.Profiles[name]; !ok {
				out.Die("profile %q not found; available profiles: %v", name, profileNames(cfgFile))
			}

			cfgFile.CurrentProfile = name
			writeCfgFile(cfgPath, cfgFile)
			fmt.Printf("Switched to profile %q\n", name)
		},
	}
}

func profileListCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all profiles",
		Args:    cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			cfgPath := cl.CfgFilePath()

			var cfgFile client.CfgFile
			if _, err := toml.DecodeFile(cfgPath, &cfgFile); err != nil {
				out.Die("unable to read config: %v", err)
			}

			if len(cfgFile.Profiles) == 0 {
				fmt.Println("No profiles configured. Config uses flat format.")
				return
			}

			names := profileNames(cfgFile)
			for _, n := range names {
				if n == cfgFile.CurrentProfile {
					fmt.Println("* " + n)
				} else {
					fmt.Println("  " + n)
				}
			}
		},
	}
}

func profileCurrentCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "current",
		Short: "Print the active profile name",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			cfgPath := cl.CfgFilePath()

			var cfgFile client.CfgFile
			if _, err := toml.DecodeFile(cfgPath, &cfgFile); err != nil {
				out.Die("unable to read config: %v", err)
			}

			if cfgFile.CurrentProfile == "" {
				fmt.Println("(no profile set)")
			} else {
				fmt.Println(cfgFile.CurrentProfile)
			}
		},
	}
}

func profileRenameCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "rename OLD NEW",
		Short: "Rename a profile",
		Args:  cobra.ExactArgs(2),
		Run: func(_ *cobra.Command, args []string) {
			oldName, newName := args[0], args[1]
			cfgPath := cl.CfgFilePath()

			var cfgFile client.CfgFile
			if _, err := toml.DecodeFile(cfgPath, &cfgFile); err != nil {
				out.Die("unable to read config: %v", err)
			}

			cfg, ok := cfgFile.Profiles[oldName]
			if !ok {
				out.Die("profile %q not found", oldName)
			}
			if _, exists := cfgFile.Profiles[newName]; exists {
				out.Die("profile %q already exists", newName)
			}

			delete(cfgFile.Profiles, oldName)
			cfgFile.Profiles[newName] = cfg
			if cfgFile.CurrentProfile == oldName {
				cfgFile.CurrentProfile = newName
			}

			writeCfgFile(cfgPath, cfgFile)
			fmt.Printf("Renamed profile %q to %q\n", oldName, newName)
		},
	}
}

func profileDeleteCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "delete NAME",
		Short: "Delete a profile",
		Args:  cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			name := args[0]
			cfgPath := cl.CfgFilePath()

			var cfgFile client.CfgFile
			if _, err := toml.DecodeFile(cfgPath, &cfgFile); err != nil {
				out.Die("unable to read config: %v", err)
			}

			if _, ok := cfgFile.Profiles[name]; !ok {
				out.Die("profile %q not found", name)
			}

			delete(cfgFile.Profiles, name)
			if cfgFile.CurrentProfile == name {
				cfgFile.CurrentProfile = ""
			}

			writeCfgFile(cfgPath, cfgFile)
			fmt.Printf("Deleted profile %q\n", name)
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

func writeCfgFile(path string, cfgFile client.CfgFile) {
	f, err := os.Create(path)
	out.MaybeDie(err, "unable to write config: %v", err)
	defer f.Close()
	if err := toml.NewEncoder(f).Encode(cfgFile); err != nil {
		out.Die("unable to encode config: %v", err)
	}
}
