package registry

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func modeCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mode",
		Short: "Get or set the registry or per-subject mode.",
		Long: `Get or set the registry or per-subject mode.

Modes are one of:
  IMPORT, READONLY, READWRITE

With no subjects, "get" and "set" operate on the global mode; with subjects,
they operate on each subject's override.

EXAMPLES:
  kcl registry mode get                          # the global mode
  kcl registry mode set READONLY mytopic-value   # freeze one subject

SEE ALSO:
  kcl registry compatibility   the level schemas are checked at
`,
	}
	cmd.AddCommand(
		modeGetCommand(cl),
		modeSetCommand(cl),
	)
	return cmd
}

func modeGetCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get [SUBJECTS...]",
		Short: "Get the global or per-subject mode.",
		Long: `Get the global or per-subject mode.

With no subjects, the global mode is printed under the subject "(global)".
With subjects, each subject's mode is printed: its own override, or the
global mode when it has none.

EXAMPLES:
  kcl registry mode get                    # the global mode
  kcl registry mode get a-value b-value    # two subjects

SEE ALSO:
  kcl registry mode set   change a mode
`,
		RunE: func(_ *cobra.Command, args []string) error {
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			results := scl.Mode(context.Background(), args...)
			return printMode(cl, "get mode", false, results)
		},
	}
	out.Columns(cmd, "SUBJECT", "MODE", "ERROR")
	return cmd
}

func modeSetCommand(cl *client.Client) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "set MODE [SUBJECTS...]",
		Short: "Set the global or per-subject mode.",
		Long: `Set the global or per-subject mode.

With no subjects, the global mode is set. With subjects, each subject's
override is set. One row per subject: SUBJECT MODE ERROR MESSAGE, and the
command exits 1 when the registry refused any of them.

EXAMPLES:
  kcl registry mode set READONLY                     # freeze the registry
  kcl registry mode set IMPORT --force               # allow ids to be chosen on create
  kcl registry mode set READWRITE a-value b-value    # two subjects' overrides

SEE ALSO:
  kcl registry mode get   the mode in force
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			var mode sr.Mode
			if err := mode.UnmarshalText([]byte(args[0])); err != nil {
				return out.Errf(out.ExitUsage, "unknown mode %q (valid: import, readonly, readwrite)", args[0])
			}
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			ctx := context.Background()
			if force {
				ctx = sr.WithParams(ctx, sr.Force)
			}
			results := scl.SetMode(ctx, mode, args[1:]...)
			return printMode(cl, "set mode", true, results)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "force the mode change (e.g. setting IMPORT on a non-empty registry)")
	out.Columns(cmd, "SUBJECT", "MODE", "ERROR", "MESSAGE")
	return cmd
}

func printMode(cl *client.Client, action string, set bool, results []sr.ModeResult) error {
	rows := make([]subjectResult, len(results))
	for i, r := range results {
		rows[i] = subjectResult{r.Subject, r.Mode.String(), r.Err}
	}
	return printSubjectResults(cl, action, "modes", "MODE", set, rows)
}
