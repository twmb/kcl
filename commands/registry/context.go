package registry

import (
	"context"

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

List or delete schema registry contexts.

A context is an independent namespace within a registry: subjects and schema
ids are unique per context. Scope any registry command to a context with the
--context flag, or anything that talks to the registry -- including
schema-aware produce/consume -- with the registry.context config key
(-X registry.context=myctx).`,
	}
	cmd.AddCommand(contextListCommand(cl), contextDeleteCommand(cl))
	return cmd
}

func contextListCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all contexts in the registry.",
		Args:    cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			contexts, err := scl.Contexts(context.Background())
			if err != nil {
				return dieErr("list contexts", err)
			}
			tw := out.NewFormattedTable(cl.Format(), "registry.context.list", 1, "contexts", "CONTEXT")
			for _, c := range contexts {
				tw.Row(c)
			}
			tw.Flush()
			return nil
		},
	}
}

func contextDeleteCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "delete CONTEXT",
		Short: "Delete an (empty) context from the registry.",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			if err := scl.DeleteContext(context.Background(), args[0]); err != nil {
				return dieErr("delete context", err)
			}
			tw := out.NewFormattedTable(cl.Format(), "registry.context.delete", 1, "deleted", "CONTEXT")
			tw.Row(args[0])
			tw.Flush()
			return nil
		},
	}
}
