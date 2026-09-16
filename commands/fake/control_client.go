package fake

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/out"
)

// controlCommand returns `kcl fake control`.
func controlCommand() *cobra.Command {
	addr := defaultControlAddr

	cmd := &cobra.Command{
		Use:   "control",
		Short: "Drive a running kcl fake cluster.",
		Long: `Drive a running kcl fake cluster.

These talk to the endpoint that ` + "`kcl fake --control`" + ` serves. The
cluster must have been started with --control; without it there is nothing
listening.

EXAMPLES:
  kcl fake --control &                            # start a cluster serving one
  kcl fake control methods                        # what we can call
  kcl fake control call MoveTopicPartition foo 0 2
  kcl fake control fault add --rule '{"topic":"foo","error":"NOT_LEADER_OR_FOLLOWER"}'

SEE ALSO:
  kcl fake               start the cluster
  kcl fake control call  call a cluster method
  kcl fake control fault install and inspect faults
`,
	}
	cmd.PersistentFlags().StringVar(&addr, "addr", addr, "control endpoint address")
	cmd.AddCommand(
		controlMethodsCommand(&addr),
		controlCallCommand(&addr),
		faultCommand(&addr),
	)
	return cmd
}

func controlMethodsCommand(addr *string) *cobra.Command {
	return &cobra.Command{
		Use:   "methods",
		Short: "List the cluster methods that control can call.",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, _ []string) error {
			var resp struct {
				Methods []controlMethod `json:"methods"`
			}
			if err := controlDo(http.MethodGet, *addr, "/methods", nil, &resp); err != nil {
				return err
			}
			tw := out.NewFormattedTable(controlFormat(cmd), "fake.control.methods", 1, "methods", "NAME", "SIGNATURE")
			for _, m := range resp.Methods {
				tw.Row(m.Name, m.Signature)
			}
			tw.Flush()
			return nil
		},
	}
}

func controlCallCommand(addr *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "call METHOD [ARGS...]",
		Short: "Call a kfake Cluster method on a running fake cluster.",
		Long: `Call a kfake Cluster method on a running fake cluster.

Methods are named and documented as kfake names them, so kfake's godoc is
the reference for what each one does. Run "kcl fake control methods" for
what this cluster can call.

Arguments are positional and match the method's parameters. A string is
taken as written, a topic ID is a uuid in any usual form, and anything else
is JSON. An argument that starts with a dash, a -1 offset, goes after --,
which is where flag parsing stops.

Results print as one line of JSON, so pipe to jq if you want it wide. This
is the one control output --format does not touch: what comes back is the
method's own return value rather than kcl's output.

EXAMPLES:
  kcl fake control call ShufflePartitionLeaders
  kcl fake control call MoveTopicPartition foo 0 2
  kcl fake control call SetFollowers foo 0 [1,2]
  kcl fake control call -- DeleteRecords foo 0 -1   # -1 is an argument
  kcl fake control call TopicInfo foo | jq -r .TopicID

SEE ALSO:
  kcl fake control methods  what this cluster can call
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			// Keep the result as raw json so we print it in the order the
			// cluster gave it rather than in map order. We print it on one
			// line: pipe to jq if you want it wide.
			var resp struct {
				Result jsontext.Value `json:"result"`
			}
			body := map[string]any{"args": args[1:]}
			if err := controlDo(http.MethodPost, *addr, "/call/"+args[0], body, &resp); err != nil {
				return err
			}
			if len(resp.Result) == 0 || string(resp.Result) == "null" {
				return nil
			}
			if err := resp.Result.Compact(); err != nil {
				return err
			}
			fmt.Println(resp.Result.String())
			return nil
		},
	}
	// An argument like the -1 offset DeleteRecords takes is a flag as far
	// as pflag is concerned, and "unknown shorthand flag: '1' in -1" does
	// not tell you what to do about it. We add that, then hand the error to
	// whoever handles ours, which is what gives it exit code 2.
	cmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		err = dashArgHint(err)
		if p := c.Parent(); p != nil {
			return p.FlagErrorFunc()(c, err)
		}
		return err
	})
	return cmd
}

// dashArgHint says how to pass an argument that starts with a dash, when the
// flag pflag could not find is a digit: only a number written as an argument
// looks like that. pflag builds the error with fmt.Errorf, so its text is all
// we have to go on.
func dashArgHint(err error) error {
	rest, ok := strings.CutPrefix(err.Error(), "unknown shorthand flag: '")
	if !ok || len(rest) == 0 || rest[0] < '0' || rest[0] > '9' {
		return err
	}
	return fmt.Errorf("%v; put -- before METHOD to pass an argument that starts with -", err)
}

// controlFormat is --format, which kcl registers as a persistent flag on the
// root command. kcl fake holds no *client.Client, so we read the flag off the
// command rather than through the client.
func controlFormat(cmd *cobra.Command) string {
	format, err := cmd.Flags().GetString("format")
	if err != nil {
		return out.FormatText
	}
	return format
}

// controlDo sends one request to a control endpoint and decodes the response
// into into. A non-200 carries the reason as an error message, which we
// return as the error. The endpoint says whether the call itself was wrong
// (an unknown method, an argument we cannot build, a rule that does not
// parse); those exit 2, and a cluster that ran the call and refused it
// exits 1.
func controlDo(method, addr, path string, body, into any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, controlURL(addr, path), rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("unable to reach a control endpoint at %s: %v (is the cluster running with --control?)", addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
			Usage bool   `json:"usage"`
		}
		if json.UnmarshalRead(resp.Body, &e) == nil && e.Error != "" {
			code := out.ExitError
			if e.Usage {
				code = out.ExitUsage
			}
			return out.Errf(code, "%s", e.Error)
		}
		return fmt.Errorf("control endpoint returned %s", resp.Status)
	}
	return json.UnmarshalRead(resp.Body, into)
}

func controlURL(addr, path string) string {
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	return strings.TrimSuffix(addr, "/") + path
}
