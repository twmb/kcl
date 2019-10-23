package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/commands/configs"
	"github.com/twmb/kcl/commands/consume"
	"github.com/twmb/kcl/commands/group"
	"github.com/twmb/kcl/commands/misc"
	"github.com/twmb/kcl/commands/produce"
	"github.com/twmb/kcl/commands/topic"
)

// TODO remove cobra to remove ridiculous implicit "help" command from everything.

func main() {
	root := &cobra.Command{
		Use:   "kcl",
		Short: "Kafka Command Line command for commanding Kafka on the command line",
		Long: `A Kafka command line interface.

kcl is a Kafka swiss army knife that aims to enable Kafka administration,
message producing, and message consuming. If Kafka supports it, kcl aims
to provide it.

For help about configuration, run:
  kcl myconfig -h

Command completion is available at:
  kcl misc gen-autocomplete
`,
	}

	cl := client.New(root)

	root.AddCommand(
		consume.Command(cl),
		produce.Command(cl),
		configs.Command(cl),
		// TODO transact
		misc.Command(cl),
		// TODO admin
		// TODO dtoken
		// TODO acl
		group.Command(cl),
		topic.Command(cl),
		// TODO configs
		// TODO metadata
		// TODO myconfig
	)

	allCommands(root, func(cmd *cobra.Command) {
		// Since we print usage on error, there is no reason to also
		// print an error on error.
		cmd.SilenceErrors = true
		// We do not want extra [flags] on every command.
		cmd.DisableFlagsInUseLine = true

		if cmd.HasParent() {
			name := strings.Split(cmd.Use, " ")[0]
			cmd.Example = strings.Replace(cmd.Example, name, cmd.Parent().CommandPath()+" "+name, -1)
		}
	})

	if err := root.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func allCommands(root *cobra.Command, fn func(*cobra.Command)) {
	for _, cmd := range root.Commands() {
		allCommands(cmd, fn)
	}
	fn(root)
}
