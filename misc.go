package main

import (
	"fmt"
	"os"
	"strconv"

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
			fmt.Println(kerr.ErrorForCode(int16(code)))
		},
	}
}

func metadataCmd() *cobra.Command {
	var req kmsg.MetadataRequest
	var noTopics bool

	cmd := &cobra.Command{
		Use:   "metadata",
		Short: "Issue a metadata command and JSON dump the results",
		ValidArgs: []string{
			"--no-topics",
			"--topics",
		},
		Run: func(_ *cobra.Command, _ []string) {
			if noTopics { // nil means everything, empty means nothing
				req.Topics = []string{}
			}

			resp, err := client.Request(&req)
			maybeDie(err, "unable to get metadata: %v", err)
			dumpJSON(resp)
		},
	}

	cmd.Flags().BoolVar(&noTopics, "no-topics", false, "fetch only broker metadata, no topics")
	cmd.Flags().StringSliceVarP(&req.Topics, "topics", "t", nil, "comma separated list of topics to fetch")

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
