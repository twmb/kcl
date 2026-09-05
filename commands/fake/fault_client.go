package fake

import (
	"bytes"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/out"
)

func faultCommand(addr *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fault",
		Short: "Install and inspect faults on a running fake cluster.",
		Long: `Install and inspect faults on a running fake cluster.

A fault fails matching requests with an error in place of the real answer.
Every selector is an AND filter, and a selector you leave out matches
everything, so a rule with only an error faults everything that can carry
one. A faulted entity is rejected before the cluster acts on it, so a
faulted produce does not append.

Rules are JSON, matching kfake's Fault type:

  keys        request names or numbers, e.g. ["fetch","produce"]
  nodes       broker IDs the request arrived at
  topic       topic name
  topic_id    topic uuid, e.g. a stale ID a client still uses; matches
              only requests that carry a topic ID
  partitions  partition numbers
  group       group ID
  txn_id      transactional ID
  resource    a config resource, quota entity, SCRAM user, log dir,
              feature, member, or ACL name
  top_level   fault the response's top-level error code rather than
              its entities
  error       error name or code, default UNKNOWN_SERVER_ERROR
  count       requests to fault, default 1, -1 until removed
`,
	}
	cmd.AddCommand(
		faultAddCommand(addr),
		faultListCommand(addr),
		faultRmCommand(addr),
		faultWaitCommand(addr),
	)
	return cmd
}

func faultAddCommand(addr *string) *cobra.Command {
	var rules []string
	cmd := &cobra.Command{
		Use:   "add",
		Short: "Install one or more faults, returning their ID.",
		Long: `Install one or more faults, returning their ID.

Rules installed together share an ID, and removing that ID removes all of
them. Each --rule is a JSON object, or @FILE to read one from a file (@- for
stdin) holding either an object or an array of them.

  kcl fake control fault add --rule '{"topic_id":"4286fc61...","error":"UNKNOWN_TOPIC_ID","count":3}'
  kcl fake control fault add --rule '{"keys":["fetch"],"nodes":[1],"topic":"foo","error":"NOT_LEADER_OR_FOLLOWER","count":-1}'
  kcl fake control fault add --rule @faults.json
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			if len(rules) == 0 {
				return out.Errf(out.ExitUsage, "at least one --rule is required")
			}
			var all []Rule
			for _, raw := range rules {
				parsed, err := parseRules(raw)
				if err != nil {
					return out.Errf(out.ExitUsage, "--rule %s: %v", elide(raw), err)
				}
				all = append(all, parsed...)
			}
			var resp faultSet
			if err := controlDo(http.MethodPost, *addr, "/faults", map[string]any{"rules": all}, &resp); err != nil {
				return err
			}
			fmt.Println(resp.ID)
			return nil
		},
	}
	cmd.Flags().StringArrayVar(&rules, "rule", nil, "fault rule as JSON, or @FILE (repeatable; rules added together share an ID)")
	return cmd
}

func faultListCommand(addr *string) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List installed faults and how many requests each has answered.",
		Args:    cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			var resp struct {
				Faults []faultSet `json:"faults"`
			}
			if err := controlDo(http.MethodGet, *addr, "/faults", nil, &resp); err != nil {
				return err
			}
			tw := out.BeginTabWrite()
			defer tw.Flush()
			fmt.Fprintf(tw, "ID\tHITS\tRULES\n")
			for _, f := range resp.Faults {
				b, err := json.Marshal(f.Rules)
				if err != nil {
					return err
				}
				fmt.Fprintf(tw, "%d\t%d\t%s\n", f.ID, f.Hits, b)
			}
			return nil
		},
	}
}

func faultRmCommand(addr *string) *cobra.Command {
	var all bool
	cmd := &cobra.Command{
		Use:   "rm ID...",
		Short: "Remove faults by ID, or all of them.",
		RunE: func(_ *cobra.Command, args []string) error {
			if all == (len(args) > 0) {
				return out.Errf(out.ExitUsage, "give either fault IDs or --all")
			}
			if all {
				var resp struct {
					Removed int `json:"removed"`
				}
				if err := controlDo(http.MethodDelete, *addr, "/faults", nil, &resp); err != nil {
					return err
				}
				fmt.Printf("removed %d\n", resp.Removed)
				return nil
			}
			for _, arg := range args {
				if _, err := strconv.Atoi(arg); err != nil {
					return out.Errf(out.ExitUsage, "fault ID %q is not a number", arg)
				}
				var resp struct{}
				if err := controlDo(http.MethodDelete, *addr, "/faults/"+arg, nil, &resp); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "remove every installed fault")
	return cmd
}

func faultWaitCommand(addr *string) *cobra.Command {
	var hits int
	var timeout string
	cmd := &cobra.Command{
		Use:   "wait ID",
		Short: "Block until a fault has answered enough requests.",
		Long: `Block until a fault has answered enough requests.

This is how a caller out of process paces itself against a fault rather than
sleeping and hoping. Exits non-zero if the timeout passes first, saying how
many requests the fault had answered by then.

  kcl fake control fault wait 1 --hits 3
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if _, err := strconv.Atoi(args[0]); err != nil {
				return out.Errf(out.ExitUsage, "fault ID %q is not a number", args[0])
			}
			var resp struct {
				Hits int `json:"hits"`
			}
			body := map[string]any{"hits": hits, "timeout": timeout}
			if err := controlDo(http.MethodPost, *addr, "/faults/"+args[0]+"/wait", body, &resp); err != nil {
				return err
			}
			fmt.Println(resp.Hits)
			return nil
		},
	}
	cmd.Flags().IntVar(&hits, "hits", 1, "requests the fault must have answered")
	cmd.Flags().StringVar(&timeout, "timeout", "30s", "how long to wait before giving up")
	return cmd
}

// parseRules reads one --rule value: JSON as written, or @FILE, or @- for
// stdin. A file may hold one rule or an array of them.
func parseRules(raw string) ([]Rule, error) {
	b := []byte(raw)
	if s, ok := strings.CutPrefix(raw, "@"); ok {
		var err error
		if s == "-" {
			b, err = io.ReadAll(os.Stdin)
		} else {
			b, err = os.ReadFile(s)
		}
		if err != nil {
			return nil, err
		}
	}
	if t := bytes.TrimLeft(b, " \t\r\n"); len(t) > 0 && t[0] == '[' {
		var rs []Rule
		err := json.Unmarshal(b, &rs, json.RejectUnknownMembers(true))
		return rs, err
	}
	var r Rule
	if err := json.Unmarshal(b, &r, json.RejectUnknownMembers(true)); err != nil {
		return nil, err
	}
	return []Rule{r}, nil
}

// elide shortens a rule for an error message.
func elide(s string) string {
	if len(s) <= 60 {
		return s
	}
	return s[:57] + "..."
}
