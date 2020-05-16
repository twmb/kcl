// Package misc contains miscellaneous, unspecific commands.
package misc

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kgo"
	"github.com/twmb/kafka-go/pkg/kmsg"
	"github.com/twmb/kafka-go/pkg/kversion"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func apiVersionsRequest() *kmsg.ApiVersionsRequest {
	return &kmsg.ApiVersionsRequest{
		ClientSoftwareName:    "kcl",
		ClientSoftwareVersion: "v0.0.0",
	}
}

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "misc",
		Short: "Miscellaneous utilities",
	}

	cmd.AddCommand(errcodeCommand())
	cmd.AddCommand(genAutocompleteCommand())
	cmd.AddCommand(apiVersionsCommand(cl))
	cmd.AddCommand(probeVersionCommand(cl))
	cmd.AddCommand(rawCommand(cl))

	return cmd
}

func errcodeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "errcode CODE",
		Short: "Print the name and message for an error code",
		Args:  cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			code, err := strconv.Atoi(args[0])
			if err != nil {
				out.Die("unable to parse error code: %v", err)
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

func genAutocompleteCommand() *cobra.Command {
	var kind string

	cmd := &cobra.Command{
		Use:   "gen-autocomplete",
		Short: "Generates bash completion scripts",
		Long: `To load completion run

. <(kcl misc gen-autocomplete -kbash)

To configure your shell to load completions for each session add to your bashrc
(or equivalent, for your shell depending on support):

# ~/.bashrc or ~/.profile
if [ -f /etc/bash_completion ] && ! shopt -oq posix; then
    . /etc/bash_completion
    . <(kcl misc gen-autocomplete -kbash)
fi

This command supports completion for bash, zsh, and powershell.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			switch kind {
			case "bash":
				cmd.Root().GenBashCompletion(os.Stdout)
			case "zsh":
				cmd.Root().GenZshCompletion(os.Stdout)
			case "powershell":
				cmd.Root().GenPowerShellCompletion(os.Stdout)
			default:
				out.Die("unrecognized autocomplete kind %q", kind)
			}
		},
	}

	cmd.Flags().StringVarP(&kind, "kind", "k", "bash", "autocomplete kind (bash, zsh, powershell)")

	return cmd
}

func apiVersionsCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "api-versions",
		Short: "Print broker API versions for each Kafka request type",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			kresp, err := cl.Client().Request(context.Background(), apiVersionsRequest())
			out.MaybeDie(err, "unable to request API versions: %v", err)

			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

			resp := kresp.(*kmsg.ApiVersionsResponse)

			fmt.Fprintf(tw, "NAME\tMAX\n")
			for _, k := range resp.ApiKeys {
				kind := kmsg.NameForKey(k.ApiKey)
				if kind == "" {
					kind = "Unknown"
				}
				fmt.Fprintf(tw, "%s\t%d\n", kind, k.MaxVersion)
			}
		},
	}
}

func probeVersionCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "probe-version",
		Short: "Probe and print the version of Kafka running",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			probeVersion(cl)
		},
	}
}

// probeVersion prints what version of Kafka the client is interacting with.
func probeVersion(cl *client.Client) {
	// If we request against a Kafka older than ApiVersions,
	// Kafka will close the connection. ErrConnDead is
	// retried automatically, so we must stop that.
	cl.AddOpt(kgo.RequestRetries(0))
	kresp, err := cl.Client().Request(context.Background(), apiVersionsRequest())
	if err != nil { // pre 0.10.0 had no api versions
		// 0.9.0 has list groups
		if _, err = cl.Client().Broker(math.MinInt32).Request(context.Background(), new(kmsg.ListGroupsRequest)); err == nil {
			fmt.Println("Kafka 0.9.0")
			return
		}
		// 0.8.2 has find coordinator
		if _, err = cl.Client().Request(context.Background(), new(kmsg.FindCoordinatorRequest)); err == nil {
			fmt.Println("Kafka 0.8.2")
			return
		}
		// 0.8.1 has offset fetch
		// OffsetFetch triggers FindCoordinator, which does not
		// exist and will fail. We need to directly ask a broker.
		// kgo's seed brokers start at MinInt32, so we know we are
		// getting a broker with this number.
		// TODO maybe not make specific to impl...
		if _, err = cl.Client().Broker(math.MinInt32).Request(context.Background(), new(kmsg.OffsetFetchRequest)); err == nil {
			fmt.Println("Kafka 0.8.1")
			return
		}
		fmt.Println("Kafka 0.8.0")
		return
	}

	resp := kresp.(*kmsg.ApiVersionsResponse)
	got := make(kversion.Versions, len(resp.ApiKeys))
	for i, key := range resp.ApiKeys {
		got[i] = key.MaxVersion
	}

	eq := func(test kversion.Versions) bool {
		if len(test) != len(got) {
			return false
		}
		for i, v := range test {
			if got[i] != v {
				return false
			}
		}
		return true
	}

	switch {
	case eq(kversion.V0_10_0()):
		fmt.Println("Kafka 0.10.0")
	case eq(kversion.V0_10_1()):
		fmt.Println("Kafka 0.10.1")
	case eq(kversion.V0_10_2()):
		fmt.Println("Kafka 0.10.2")
	case eq(kversion.V0_11_0()):
		fmt.Println("Kafka 0.11.0")
	case eq(kversion.V1_0_0()):
		fmt.Println("Kafka 1.0.0")
	case eq(kversion.V1_1_0()):
		fmt.Println("Kafka 1.1.0")
	case eq(kversion.V2_0_0()):
		fmt.Println("Kafka 2.0.0")
	case eq(kversion.V2_1_0()):
		fmt.Println("Kafka 2.1.0")
	case eq(kversion.V2_2_0()):
		fmt.Println("Kafka 2.2.0")
	case eq(kversion.V2_3_0()):
		fmt.Println("Kafka 2.3.0")
	case eq(kversion.V2_4_0()):
		fmt.Println("Kafka 2.4.0")
	case eq(kversion.V2_5_0()):
		fmt.Println("Kafka 2.5.0")
	default:
		fmt.Println("Unknown version: either tip or between releases")
	}
}

func rawCommand(cl *client.Client) *cobra.Command {
	var key int16
	cmd := &cobra.Command{
		Use:   "raw-req",
		Short: "Issue an arbitrary request parsed from JSON read from STDIN.",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			req := kmsg.RequestForKey(key)
			if req == nil {
				out.Die("request key %d unknown", key)
			}
			raw, err := ioutil.ReadAll(os.Stdin)
			out.MaybeDie(err, "unable to read stdin: %v", err)
			err = json.Unmarshal(raw, req)
			out.MaybeDie(err, "unable to unmarshal stdin: %v", err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			kresp, err := cl.Client().Request(ctx, req)
			out.MaybeDie(err, "response error: %v", err)
			out.ExitJSON(kresp)
		},
	}
	cmd.Flags().Int16VarP(&key, "key", "k", -1, "request key")
	return cmd
}
