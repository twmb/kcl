package registry

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func schemaDeleteCommand(cl *client.Client) *cobra.Command {
	var (
		versionStr string
		permanent  bool
	)
	cmd := &cobra.Command{
		Use:   "delete SUBJECT",
		Short: "Delete one version of a subject.",
		Long: `Delete one version of a subject.

-v names the version; pass "latest" for the newest one, and the row names
the number it resolved to. To delete a subject and every version under it,
use kcl registry subject delete.

The delete is soft by default: the version is hidden but retained, and can be
listed again with --show-deleted. A version must already be soft deleted
before --permanent removes it for good, which cannot be undone.

The row is SUBJECT VERSION ERROR MESSAGE; ERROR names the registry's error
and the command exits 1 when the delete failed.

EXAMPLES:
  kcl registry schema delete mytopic-value -v 3               # soft delete version 3
  kcl registry schema delete mytopic-value -v 3 --permanent   # then remove it for good
  kcl registry schema delete mytopic-value -v latest          # the newest version

SEE ALSO:
  kcl registry subject delete   delete a subject and every version under it
  kcl registry schema list      the versions registered under a subject
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if versionStr == "" {
				return out.Errf(out.ExitUsage, "schema delete needs -v VERSION; to delete the subject and every version under it, use kcl registry subject delete")
			}
			return deleteSchemaVersion(cl, args[0], versionStr, permanent)
		},
	}
	cmd.Flags().StringVarP(&versionStr, "version", "v", "", "delete this version (number or 'latest')")
	cmd.Flags().BoolVar(&permanent, "permanent", false, "permanently (hard) delete; the version must already be soft-deleted")
	out.Columns(cmd, schemaDeleteColumns...)
	return cmd
}

var schemaDeleteColumns = []string{"SUBJECT", "VERSION", "ERROR", "MESSAGE"}

// deleteSchemaVersion deletes one version of subject and prints the result
// row. "latest" is resolved to its number first, so that the row names the
// version that was deleted.
func deleteSchemaVersion(cl *client.Client, subject, versionStr string, permanent bool) error {
	version, err := parseVersion(versionStr)
	if err != nil {
		return err
	}
	scl, err := srClient(cl)
	if err != nil {
		return err
	}
	how := sr.SoftDelete
	if permanent {
		how = sr.HardDelete
	}
	ctx := context.Background()
	tw := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "deleted", schemaDeleteColumns...).ResultColumns()

	// "latest" under --permanent is the newest version counting the soft
	// deleted ones, which is how the registry itself resolves it for a
	// permanent delete: only a soft deleted version can be removed for good.
	resolveCtx := ctx
	if permanent {
		resolveCtx = sr.WithParams(ctx, sr.ShowDeleted)
	}
	resolved, err := resolveVersion(resolveCtx, scl, subject, version)
	if err != nil {
		errName, message, ok := resultCells(err)
		if !ok {
			return dieErr("delete schema version", err)
		}
		tw.Row(subject, out.Unknown, errName, message)
		return tw.Flush()
	}

	err = scl.DeleteSchema(ctx, subject, resolved, how)
	errName, message, ok := resultCells(err)
	if !ok {
		return dieErr("delete schema version", err)
	}
	tw.Row(subject, resolved, errName, message)
	return tw.Flush()
}

// oldDeleteCommand is the old "registry delete", which deleted a subject or,
// with -v, one version. It runs the command that does now, and names it.
func oldDeleteCommand(cl *client.Client) *cobra.Command {
	var (
		versionStr string
		permanent  bool
	)
	cmd := &cobra.Command{
		Use:        "delete SUBJECT",
		Short:      "Delete an entire subject, or a single version of a subject.",
		Hidden:     true,
		Deprecated: "use 'kcl registry subject delete', or 'kcl registry schema delete -v' for one version, instead",
		Args:       cobra.ExactArgs(1),
		PreRun: func(*cobra.Command, []string) {
			if versionStr != "" {
				cl.SetCommand("registry.schema.delete")
			} else {
				cl.SetCommand("registry.subject.delete")
			}
		},
		RunE: func(_ *cobra.Command, args []string) error {
			if versionStr != "" {
				return deleteSchemaVersion(cl, args[0], versionStr, permanent)
			}
			return deleteSubject(cl, args[0], permanent)
		},
	}
	cmd.Flags().StringVarP(&versionStr, "version", "v", "", "a single version to delete (number or 'latest'); if unset, the whole subject is deleted")
	cmd.Flags().BoolVar(&permanent, "permanent", false, "permanently (hard) delete; the target must already be soft-deleted")
	out.ColumnsFunc(cmd, func() []string {
		if versionStr != "" {
			return schemaDeleteColumns
		}
		return subjectDeleteColumns
	})
	return cmd
}
