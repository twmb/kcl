package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/commands/broker"
	"github.com/twmb/kcl/commands/config"
	"github.com/twmb/kcl/commands/consume"
	"github.com/twmb/kcl/commands/group"
	"github.com/twmb/kcl/commands/logdirs"
	"github.com/twmb/kcl/commands/misc"
	"github.com/twmb/kcl/commands/produce"
	"github.com/twmb/kcl/commands/topic"
)

func main() {
	root := &cobra.Command{
		Use:   "kcl",
		Short: "Kafka Command Line command for commanding Kafka on the command line",
		Long: `Kafka Command Line command for commanding Kafka on the command line.

kcl is a Kafka swiss army knife that aims to enable Kafka administration,
message producing, and message consuming.

For help about configuration, run 'kcl config help'.

To enable bash autocompletion, add '. <(kcl misc gen-autocomplete)'
to your bash profile.
`,
	}

	cl := client.New(root)

	root.AddCommand(broker.Command(cl))
	root.AddCommand(config.Command(cl))
	root.AddCommand(consume.Command(cl))
	root.AddCommand(group.Command(cl))
	root.AddCommand(logdirs.Command(cl))
	root.AddCommand(misc.Command(cl))
	root.AddCommand(produce.Command(cl))
	root.AddCommand(topic.Command(cl))

	if err := root.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
