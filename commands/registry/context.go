package registry

import (
	"context"
	"slices"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func contextCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "context",
		Aliases: []string{"ctx"},
		Short:   "List or delete schema registry contexts (namespaces).",
		Long: `List or delete schema registry contexts (namespaces).

A context is an independent namespace within a registry: subjects and schema
ids are unique per context. Scope any registry command to a context with the
--context flag, or anything that talks to the registry -- including
schema-aware produce/consume -- with the registry.context config key
(-X registry.context=myctx).

EXAMPLES:
  kcl registry context list                      # every context
  kcl registry --context myctx subject list      # the subjects in one context
  kcl registry context delete myctx              # delete an empty context

SEE ALSO:
  kcl registry subject list   the subjects in the selected context
`,
	}
	cmd.AddCommand(contextListCommand(cl), contextDeleteCommand(cl))
	return cmd
}

func contextListCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all contexts in the registry.",
		Long: `List all contexts in the registry.

The default context is ".". Contexts are listed sorted by name.

EXAMPLES:
  kcl registry context list

SEE ALSO:
  kcl registry context delete   delete an empty context
`,
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			contexts, err := scl.Contexts(context.Background())
			if err != nil {
				return dieErr("list contexts", err)
			}
			slices.Sort(contexts)
			tw := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "contexts", "CONTEXT")
			for _, c := range contexts {
				tw.Row(c)
			}
			return tw.Flush()
		},
	}
	out.Columns(cmd, "CONTEXT")
	return cmd
}

func contextDeleteCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete CONTEXT",
		Short: "Delete an empty context from the registry.",
		Long: `Delete an empty context from the registry.

The registry refuses to delete a context that still has subjects. The row is
CONTEXT ERROR MESSAGE; ERROR names the registry's error and the command exits
1 when the delete failed.

EXAMPLES:
  kcl registry context delete myctx

SEE ALSO:
  kcl registry context list   the contexts there are
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			err = scl.DeleteContext(context.Background(), args[0])
			errName, message, ok := resultCells(err)
			if !ok {
				return dieErr("delete context", err)
			}
			tw := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "deleted", "CONTEXT", "ERROR", "MESSAGE").ResultColumns()
			tw.Row(args[0], errName, message)
			return tw.Flush()
		},
	}
	out.Columns(cmd, "CONTEXT", "ERROR", "MESSAGE")
	return cmd
}
