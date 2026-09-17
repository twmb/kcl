package registry

import (
	"cmp"
	"context"
	"slices"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func schemaReferencesCommand(cl *client.Client) *cobra.Command {
	var versionStr string
	cmd := &cobra.Command{
		Use:     "references SUBJECT",
		Aliases: []string{"refs", "referenced-by"},
		Short:   "List the schemas that reference a subject version.",
		Long: `List the schemas that reference a subject version.

This is the reverse of a schema's own references: it answers "who depends on
this schema?", which is useful before deleting or evolving it. Each row is a
referencing schema: its subject, version, and id, sorted by subject and
version.

EXAMPLES:
  kcl registry schema references common-value        # who references the latest version
  kcl registry schema references common-value -v 2   # who references version 2

SEE ALSO:
  kcl registry schema get      a schema and the references it declares
  kcl registry schema delete   delete a version nothing references any more
`,
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
			slices.SortFunc(refs, func(a, b sr.SubjectSchema) int {
				return cmp.Or(cmp.Compare(a.Subject, b.Subject), cmp.Compare(a.Version, b.Version))
			})
			tw := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "references", "SUBJECT", "VERSION", "ID")
			for _, r := range refs {
				tw.Row(r.Subject, r.Version, r.ID)
			}
			return tw.Flush()
		},
	}
	cmd.Flags().StringVarP(&versionStr, "version", "v", "latest", "find references to this version (number or 'latest')")
	out.Columns(cmd, "SUBJECT", "VERSION", "ID")
	return cmd
}
