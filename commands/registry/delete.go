package registry

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func deleteCommand(cl *client.Client) *cobra.Command {
	var (
		versionStr string
		permanent  bool
	)
	cmd := &cobra.Command{
		Use:   "delete SUBJECT",
		Short: "Delete an entire subject, or a single version of a subject.",
		Long: `Delete an entire subject, or a single version of a subject.

Without --version, the whole subject (all versions) is deleted. With
--version, only that version is deleted; pass "latest" for the latest.

Deletes are "soft" by default: the schema is hidden but retained, and can be
listed again with --show-deleted. A subject/version must already be
soft-deleted before it can be permanently removed with --permanent, which
irreversibly deletes the data.

  kcl registry delete mytopic-value
  kcl registry delete mytopic-value -v 3
  kcl registry delete mytopic-value --permanent
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			subject := args[0]

			how := sr.SoftDelete
			if permanent {
				how = sr.HardDelete
			}

			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			ctx := context.Background()

			// Delete a single version.
			if versionStr != "" {
				version, verr := parseVersion(versionStr)
				if verr != nil {
					return verr
				}
				if err := scl.DeleteSchema(ctx, subject, version, how); err != nil {
					return dieErr("delete schema version", err)
				}
				tw := out.NewFormattedTable(cl.Format(), "registry.delete", 1, "deleted", "SUBJECT", "VERSION", "PERMANENT")
				tw.Row(subject, versionString(version), permanent)
				tw.Flush()
				return nil
			}

			// Delete the whole subject.
			versions, err := scl.DeleteSubject(ctx, subject, how)
			if err != nil {
				return dieErr("delete subject", err)
			}
			tw := out.NewFormattedTable(cl.Format(), "registry.delete", 1, "deleted", "SUBJECT", "VERSION", "PERMANENT")
			for _, v := range versions {
				tw.Row(subject, v, permanent)
			}
			tw.Flush()
			return nil
		},
	}
	cmd.Flags().StringVarP(&versionStr, "version", "v", "", "a single version to delete (number or 'latest'); if unset, the whole subject is deleted")
	cmd.Flags().BoolVar(&permanent, "permanent", false, "permanently (hard) delete; the target must already be soft-deleted")
	return cmd
}
