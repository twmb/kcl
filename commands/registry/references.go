package registry

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func referencesCommand(cl *client.Client) *cobra.Command {
	var versionStr string
	cmd := &cobra.Command{
		Use:     "references SUBJECT",
		Aliases: []string{"refs", "referenced-by"},
		Short:   "List schemas that reference a subject version.",
		Long: `List schemas that reference a subject version.

List the schemas that reference a given subject version.

This is the reverse of a schema's own references: it answers "who depends on
this schema?", which is useful before deleting or evolving it.`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			version, err := parseVersion(versionStr)
			if err != nil {
				return err
			}
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			refs, err := scl.SchemaReferences(context.Background(), args[0], version)
			if err != nil {
				return dieErr("list references", err)
			}
			tw := out.NewFormattedTable(cl.Format(), "registry.references", 1, "references", "SUBJECT", "VERSION", "ID")
			for _, r := range refs {
				tw.Row(r.Subject, r.Version, r.ID)
			}
			tw.Flush()
			return nil
		},
	}
	cmd.Flags().StringVarP(&versionStr, "version", "v", "latest", "subject version to find references to (number or 'latest')")
	return cmd
}
