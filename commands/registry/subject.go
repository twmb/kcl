package registry

import (
	"context"
	"slices"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func subjectCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "subject",
		Short: "List and delete subjects.",
		Long: `List and delete subjects.

A subject is the name a registry files schema versions under, "mytopic-value"
for the values of mytopic under the default TopicNameStrategy.

EXAMPLES:
  kcl registry subject list                    # every subject
  kcl registry subject delete mytopic-value    # soft delete a subject

SEE ALSO:
  kcl registry schema list SUBJECT    the versions registered under a subject
  kcl registry schema delete          delete one version of a subject
`,
	}
	cmd.AddCommand(
		subjectListCommand(cl),
		subjectDeleteCommand(cl),
	)
	return cmd
}

func subjectListCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all subjects in the registry.",
		Long: `List all subjects in the registry.

Subjects are listed sorted by name. A soft deleted subject is left out unless
--show-deleted is given.

EXAMPLES:
  kcl registry subject list                  # every subject
  kcl registry subject list --show-deleted   # soft deleted ones too

SEE ALSO:
  kcl registry schema list      every schema version, across all subjects
  kcl registry subject delete   delete a subject
`,
		Args: cobra.NoArgs,
	}
	listCtx := showDeletedFlag(cmd, "subjects")
	out.Columns(cmd, "SUBJECT")
	cmd.RunE = func(_ *cobra.Command, _ []string) error {
		scl, err := srClient(cl)
		if err != nil {
			return err
		}
		subjects, err := scl.Subjects(listCtx())
		if err != nil {
			return dieErr("list subjects", err)
		}
		slices.Sort(subjects)
		tw := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "subjects", "SUBJECT")
		for _, s := range subjects {
			tw.Row(s)
		}
		return tw.Flush()
	}
	return cmd
}

func subjectDeleteCommand(cl *client.Client) *cobra.Command {
	var permanent bool
	cmd := &cobra.Command{
		Use:   "delete SUBJECT",
		Short: "Delete a subject and every version under it.",
		Long: `Delete a subject and every version under it.

The delete is soft by default: the subject is hidden but retained, and can be
listed again with --show-deleted. A subject must already be soft deleted
before --permanent removes it for good, which cannot be undone.

The row is SUBJECT ERROR MESSAGE; ERROR names the registry's error and the
command exits 1 when the delete failed.

EXAMPLES:
  kcl registry subject delete mytopic-value               # soft delete
  kcl registry subject delete mytopic-value --permanent   # then remove it for good

SEE ALSO:
  kcl registry schema delete    delete one version of a subject
  kcl registry subject list     see what is there
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return deleteSubject(cl, args[0], permanent)
		},
	}
	cmd.Flags().BoolVar(&permanent, "permanent", false, "permanently (hard) delete; the subject must already be soft-deleted")
	out.Columns(cmd, subjectDeleteColumns...)
	return cmd
}

var subjectDeleteColumns = []string{"SUBJECT", "ERROR", "MESSAGE"}

// deleteSubject deletes every version of subject and prints the result row.
func deleteSubject(cl *client.Client, subject string, permanent bool) error {
	scl, err := srClient(cl)
	if err != nil {
		return err
	}
	how := sr.SoftDelete
	if permanent {
		how = sr.HardDelete
	}
	_, err = scl.DeleteSubject(context.Background(), subject, how)
	errName, message, ok := resultCells(err)
	if !ok {
		return dieErr("delete subject", err)
	}
	tw := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "deleted", subjectDeleteColumns...).ResultColumns()
	tw.Row(subject, errName, message)
	return tw.Flush()
}
