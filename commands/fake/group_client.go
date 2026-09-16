package fake

import (
	"net/http"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/twmb/kcl/out"
)

func groupCommand(addr *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "group",
		Short: "Wait on groups in a running fake cluster.",
		Long: `Wait on groups in a running fake cluster.

A group forms, rebalances, and empties on its own schedule, so a script
driving consumers waits on the shape it wants rather than on a clock.

EXAMPLES:
  kcl fake control group wait g1 --state Stable --members 2

SEE ALSO:
  kcl fake control group wait      block until a group looks the way you want
  kcl fake control call GroupInfo  the group as it is right now
`,
	}
	cmd.AddCommand(groupWaitCommand(addr))
	return cmd
}

func groupWaitCommand(addr *string) *cobra.Command {
	var (
		members  int
		state    string
		assigned int
		timeout  string
	)
	cmd := &cobra.Command{
		Use:   "wait GROUP",
		Short: "Block until a group has the members, state, or assignment you want.",
		Long: `Block until a group has the members, state, or assignment you want.

This is how a caller out of process waits out a rebalance rather than
sleeping and hoping. At least one of --members, --state, and --assigned is
required, and every one you give must hold at once. A group that does not
exist yet satisfies nothing, so this also waits for one to appear. Exits
non-zero if the timeout passes first, saying what the group looked like by
then.

EXAMPLES:
  kcl fake control group wait g1 --state Stable --members 2
  kcl fake control group wait g1 --members 0   # everyone left

SEE ALSO:
  kcl fake control fault wait      block until a fault fires
  kcl fake control call GroupInfo  the group as it is right now
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req := groupWait{Group: args[0], State: state, Timeout: timeout}
			if cmd.Flags().Changed("members") {
				req.Members = &members
			}
			if cmd.Flags().Changed("assigned") {
				req.Assigned = &assigned
			}
			if req.Members == nil && req.State == "" && req.Assigned == nil {
				return out.Errf(out.ExitUsage, "give at least one of --members, --state, --assigned")
			}
			var resp struct {
				Group *kfake.GroupInfo `json:"group"`
			}
			if err := controlDo(http.MethodPost, *addr, "/groups/wait", req, &resp); err != nil {
				return err
			}
			if resp.Group == nil {
				return out.Errf(out.ExitError, "group %s: not found", args[0])
			}
			g := resp.Group
			// The group prints Go cased, as kcl fake control call
			// GroupInfo prints it, since it is kfake's own type: what
			// comes back from one pastes into a script reading the other.
			if controlFormat(cmd) == out.FormatJSON {
				out.MarshalJSON("fake.control.group.wait", 1, map[string]any{"group": g})
				return nil
			}
			tw := out.NewFormattedTable(controlFormat(cmd), "fake.control.group.wait", 1, "groups", "GROUP", "STATE", "EPOCH", "MEMBERS", "ASSIGNED")
			tw.Row(g.Group, g.State, g.Epoch, len(g.Members), g.NumAssigned())
			tw.Flush()
			return nil
		},
	}
	cmd.Flags().IntVar(&members, "members", 0, "members the group must have")
	cmd.Flags().StringVar(&state, "state", "", "state the group must be in (Empty, Stable, ...)")
	cmd.Flags().IntVar(&assigned, "assigned", 0, "partitions that must be assigned across all members")
	cmd.Flags().StringVar(&timeout, "timeout", "30s", "how long to wait before giving up")
	return cmd
}
