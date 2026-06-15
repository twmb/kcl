package registry

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func subjectsCommand(cl *client.Client) *cobra.Command {
	var showDeleted bool
	cmd := &cobra.Command{
		Use:     "subjects",
		Aliases: []string{"ls", "list"},
		Short:   "List all subjects in the registry.",
		Args:    cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			ctx := context.Background()
			if showDeleted {
				ctx = sr.WithParams(ctx, sr.ShowDeleted)
			}
			subjects, err := scl.Subjects(ctx)
			if err != nil {
				return dieErr("list subjects", err)
			}
			tw := out.NewFormattedTable(cl.Format(), "registry.subjects", 1, "subjects", "SUBJECT")
			for _, s := range subjects {
				tw.Row(s)
			}
			tw.Flush()
			return nil
		},
	}
	cmd.Flags().BoolVar(&showDeleted, "show-deleted", false, "include soft-deleted subjects")
	return cmd
}

func versionsCommand(cl *client.Client) *cobra.Command {
	var showDeleted bool
	cmd := &cobra.Command{
		Use:     "versions SUBJECT",
		Aliases: []string{"vs"},
		Short:   "List all versions registered under a subject.",
		Args:    cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			ctx := context.Background()
			if showDeleted {
				ctx = sr.WithParams(ctx, sr.ShowDeleted)
			}
			versions, err := scl.SubjectVersions(ctx, args[0])
			if err != nil {
				return dieErr("list versions", err)
			}
			tw := out.NewFormattedTable(cl.Format(), "registry.versions", 1, "versions", "VERSION")
			for _, v := range versions {
				tw.Row(v)
			}
			tw.Flush()
			return nil
		},
	}
	cmd.Flags().BoolVar(&showDeleted, "show-deleted", false, "include soft-deleted versions")
	return cmd
}
