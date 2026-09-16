package fake

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
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

A rule is comma separated key=value pairs, or JSON, or @FILE holding JSON
(@- for stdin). Both forms take the same fields, matching kfake's Fault
type:

  keys        request names or numbers, e.g. fetch or ["fetch","produce"]
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
  observe     count matching requests rather than faulting them, so a
              wait blocks on requests that succeed; not with error

In pairs, keys, nodes, and partitions append when repeated and every other
field given twice is an error; a bare observe or top_level means true.
fault list prints rules as JSON with the defaults filled in, which pastes
back into fault add --rule.

EXAMPLES:
  kcl fake control fault add --rule topic=foo,error=NOT_LEADER_OR_FOLLOWER,count=3
  kcl fake control fault add --rule keys=fetch,keys=produce,nodes=1,observe,count=-1
  kcl fake control fault add --rule '{"topic":"foo","error":"NOT_LEADER_OR_FOLLOWER"}'
  kcl fake control fault list                   # the fault and what it has hit

SEE ALSO:
  kcl fake control fault add   install a fault
  kcl fake control fault list  what is installed and what it has hit
  kcl fake control fault wait  block until a fault fires
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
them. Each --rule is key=value pairs, or a JSON object, or @FILE to read one
from a file (@- for stdin) holding either an object or an array of them.
Run "kcl fake control fault --help" for the fields a rule takes.

EXAMPLES:
  kcl fake control fault add --rule topic=foo,error=NOT_LEADER_OR_FOLLOWER,count=3
  kcl fake control fault add --rule '{"topic_id":"4286fc61-8d3e-4b4a-9d3e-1a2b3c4d5e6f","error":"UNKNOWN_TOPIC_ID","count":3}'
  kcl fake control fault add --rule '{"keys":["fetch"],"nodes":[1],"topic":"foo","error":"NOT_LEADER_OR_FOLLOWER","count":-1}'
  kcl fake control fault add --rule @faults.json
  kcl fake control fault add --rule '{"keys":["fetch"],"topic":"foo","observe":true,"count":-1}'
  kcl fake control fault wait ID --hits 5           # blocks until the consumer has fetched five times

SEE ALSO:
  kcl fake control fault list  what is installed
  kcl fake control fault rm    remove one or all
`,
		Args: cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if len(rules) == 0 {
				return out.Errf(out.ExitUsage, "at least one --rule is required")
			}
			var all []Rule
			for _, raw := range rules {
				if strings.TrimSpace(raw) == "" {
					return out.Errf(out.ExitUsage, "--rule: empty rule")
				}
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
			if controlFormat(cmd) == out.FormatJSON {
				out.MarshalJSON("fake.control.fault.add", 1, map[string]any{"id": resp.ID})
				return nil
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
		Short:   "List installed faults, their hits, and what is left of their budget.",
		Long: `List installed faults, their hits, and what is left of their budget.

LEFT is the requests the fault can still answer, 0 once it is spent and -1
when a rule in it is unlimited. That tells a spent count:3 apart from a live
count:-1 that happens to have fired three times.

Rules print with kfake's defaults filled in, the rule the cluster enforces
rather than the rule you typed, so a rule from --format json pastes back
into fault add --rule unchanged.

EXAMPLES:
  kcl fake control fault list
  kcl fake control fault list --format json | jq '.faults[] | select(.left == 0)'

SEE ALSO:
  kcl fake control fault add  install a fault
`,
		Args: cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, _ []string) error {
			var resp struct {
				Faults []faultSet `json:"faults"`
			}
			if err := controlDo(http.MethodGet, *addr, "/faults", nil, &resp); err != nil {
				return err
			}
			tw := out.NewFormattedTable(controlFormat(cmd), "fake.control.fault.list", 1, "faults", "ID", "HITS", "LEFT", "RULES")
			for _, f := range resp.Faults {
				b, err := json.Marshal(f.Rules)
				if err != nil {
					return err
				}
				// As a jsontext.Value the rules print as JSON in text and awk
				// and nest as JSON rather than as a quoted string in json.
				tw.Row(f.ID, f.Hits, f.Left, jsontext.Value(b))
			}
			tw.Flush()
			return nil
		},
	}
}

func faultRmCommand(addr *string) *cobra.Command {
	var all bool
	cmd := &cobra.Command{
		Use:   "rm ID...",
		Short: "Remove faults by ID, or all of them.",
		Long: `Remove faults by ID, or all of them.

Removing an ID removes every rule installed under it, and prints how many
faults went away.

EXAMPLES:
  kcl fake control fault rm 1
  kcl fake control fault rm --all

SEE ALSO:
  kcl fake control fault list  what is installed
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if all == (len(args) > 0) {
				return out.Errf(out.ExitUsage, "give either fault IDs or --all")
			}
			var removed int
			if all {
				var resp struct {
					Removed int `json:"removed"`
				}
				if err := controlDo(http.MethodDelete, *addr, "/faults", nil, &resp); err != nil {
					return err
				}
				removed = resp.Removed
			}
			for _, arg := range args {
				if _, err := strconv.Atoi(arg); err != nil {
					return out.Errf(out.ExitUsage, "fault ID %q is not a number", arg)
				}
				var resp struct {
					Removed int `json:"removed"`
				}
				if err := controlDo(http.MethodDelete, *addr, "/faults/"+arg, nil, &resp); err != nil {
					return err
				}
				removed += resp.Removed
			}
			printRemoved(controlFormat(cmd), removed)
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

EXAMPLES:
  kcl fake control fault wait 1            # until it fires once
  kcl fake control fault wait 1 --hits 3

SEE ALSO:
  kcl fake control fault list  hits and budget without blocking
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
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
			if controlFormat(cmd) == out.FormatJSON {
				out.MarshalJSON("fake.control.fault.wait", 1, map[string]any{"hits": resp.Hits})
				return nil
			}
			fmt.Println(resp.Hits)
			return nil
		},
	}
	cmd.Flags().IntVar(&hits, "hits", 1, "requests the fault must have answered")
	cmd.Flags().StringVar(&timeout, "timeout", "30s", "how long to wait before giving up")
	return cmd
}

// printRemoved says what rm removed, in whichever format was asked for. Both
// forms of rm print it: naming an ID that was there printed nothing at all
// before, and --all printed prose whatever --format said.
func printRemoved(format string, n int) {
	switch format {
	case out.FormatJSON:
		out.MarshalJSON("fake.control.fault.rm", 1, map[string]any{"removed": n})
	case out.FormatAWK:
		fmt.Println(n)
	default:
		fmt.Printf("removed %d\n", n)
	}
}

// parseRules reads one --rule value. The first character decides: { or [ is
// JSON, @ is a file (@- for stdin) holding JSON, one rule or an array of
// them, and anything else is key=value pairs.
func parseRules(raw string) ([]Rule, error) {
	if s, ok := strings.CutPrefix(raw, "@"); ok {
		var b []byte
		var err error
		if s == "-" {
			b, err = io.ReadAll(os.Stdin)
		} else {
			b, err = os.ReadFile(s)
		}
		if err != nil {
			return nil, err
		}
		return parseRulesJSON(b)
	}
	if t := strings.TrimLeft(raw, " \t\r\n"); len(t) > 0 && (t[0] == '{' || t[0] == '[') {
		return parseRulesJSON([]byte(raw))
	}
	r, err := parseRulePairs(raw)
	if err != nil {
		return nil, err
	}
	return []Rule{r}, nil
}

// parseRulesJSON reads one rule or an array of them.
func parseRulesJSON(b []byte) ([]Rule, error) {
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

// ruleFields are the fields a rule takes, in the order fault --help lists
// them. The pairs form and the JSON form take the same ones.
var ruleFields = []string{"keys", "nodes", "topic", "topic_id", "partitions", "group", "txn_id", "resource", "top_level", "error", "count", "observe"}

// ruleAppends are the fields that take a list, which a repeat appends to.
// Every other field given twice is an error: one of the two was meant to
// win and we cannot tell which.
var ruleAppends = map[string]bool{"keys": true, "nodes": true, "partitions": true}

// parseRulePairs reads a rule written as comma separated key=value pairs,
// the shape -c and --synthetic-batch take. Fields and values are the JSON
// rule's, and a bare observe or top_level means true.
func parseRulePairs(s string) (Rule, error) {
	var r Rule
	seen := make(map[string]bool)
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			return r, fmt.Errorf("empty pair; want %s", strings.Join(ruleFields, ", "))
		}
		k, v, given := strings.Cut(pair, "=")
		if !given {
			if k != "observe" && k != "top_level" {
				if slices.Contains(ruleFields, k) {
					return r, fmt.Errorf("%s: want %s=VALUE", k, k)
				}
				return r, fmt.Errorf("unknown key %q: want %s", k, strings.Join(ruleFields, ", "))
			}
			v = "true"
		}
		if v == "" {
			return r, fmt.Errorf("%s: empty value", k)
		}
		if seen[k] && !ruleAppends[k] {
			return r, fmt.Errorf("%s given twice", k)
		}
		seen[k] = true

		switch k {
		case "keys":
			r.Keys = append(r.Keys, v)
		case "nodes":
			n, err := ruleInt32(k, v)
			if err != nil {
				return r, err
			}
			r.Nodes = append(r.Nodes, n)
		case "partitions":
			n, err := ruleInt32(k, v)
			if err != nil {
				return r, err
			}
			r.Partitions = append(r.Partitions, n)
		case "topic":
			r.Topic = v
		case "topic_id":
			r.TopicID = v
		case "group":
			r.Group = v
		case "txn_id":
			r.TxnID = v
		case "resource":
			r.Resource = v
		case "error":
			r.Error = v
		case "count":
			n, err := strconv.Atoi(v)
			if err != nil {
				return r, fmt.Errorf("%s: %q is not a number", k, v)
			}
			r.Count = n
		case "top_level":
			b, err := ruleBool(k, v)
			if err != nil {
				return r, err
			}
			r.TopLevel = b
		case "observe":
			b, err := ruleBool(k, v)
			if err != nil {
				return r, err
			}
			r.Observe = b
		default:
			return r, fmt.Errorf("unknown key %q: want %s", k, strings.Join(ruleFields, ", "))
		}
	}
	return r, nil
}

func ruleInt32(k, v string) (int32, error) {
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a number", k, v)
	}
	return int32(n), nil
}

func ruleBool(k, v string) (bool, error) {
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: %q is not true or false", k, v)
	}
	return b, nil
}

// elide shortens a rule for an error message.
func elide(s string) string {
	if len(s) <= 60 {
		return s
	}
	return s[:57] + "..."
}
