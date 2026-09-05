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

  kcl fake --control &
  kcl fake control methods
  kcl fake control call MoveTopicPartition foo 0 2
  kcl fake control fault add --rule '{"topic":"foo","error":"NOT_LEADER_OR_FOLLOWER"}'
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
		RunE: func(_ *cobra.Command, _ []string) error {
			var resp struct {
				Methods []controlMethod `json:"methods"`
			}
			if err := controlDo(http.MethodGet, *addr, "/methods", nil, &resp); err != nil {
				return err
			}
			tw := out.BeginTabWrite()
			defer tw.Flush()
			for _, m := range resp.Methods {
				fmt.Fprintf(tw, "%s\t%s\n", m.Name, m.Signature)
			}
			return nil
		},
	}
}

func controlCallCommand(addr *string) *cobra.Command {
	return &cobra.Command{
		Use:   "call METHOD [ARGS...]",
		Short: "Call a kfake Cluster method on a running fake cluster.",
		Long: `Call a kfake Cluster method on a running fake cluster.

Methods are named and documented as kfake names them, so kfake's godoc is
the reference for what each one does. Run "kcl fake control methods" for
what this cluster can call.

Arguments are positional and match the method's parameters. A string is
taken as written, a topic ID is a uuid in any usual form, and anything else
is JSON.

  kcl fake control call ShufflePartitionLeaders
  kcl fake control call MoveTopicPartition foo 0 2
  kcl fake control call SetFollowers foo 0 [1,2]
  kcl fake control call TopicInfo foo
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			// Keep the result as raw json so we print it in the order the
			// cluster gave it rather than in map order.
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
			if err := resp.Result.Indent(jsontext.WithIndent("  ")); err != nil {
				return err
			}
			fmt.Println(resp.Result.String())
			return nil
		},
	}
}

// controlDo sends one request to a control endpoint and decodes the response
// into into. A non-200 carries the reason as an error message, which we
// return as the error.
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
		}
		if json.UnmarshalRead(resp.Body, &e) == nil && e.Error != "" {
			return out.Errf(out.ExitUsage, "%s", e.Error)
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
