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
they operate on each subject's override.`,
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
		RunE: func(_ *cobra.Command, args []string) error {
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			results := scl.Mode(context.Background(), args...)
			return printMode(cl, "registry.mode.get", results)
		},
	}
	return cmd
}

func modeSetCommand(cl *client.Client) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "set MODE [SUBJECTS...]",
		Short: "Set the global or per-subject mode.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			var mode sr.Mode
			if err := mode.UnmarshalText([]byte(args[0])); err != nil {
				return out.Errf(out.ExitUsage, "%v (valid: import, readonly, readwrite)", err)
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
			return printMode(cl, "registry.mode.set", results)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "force the mode change (e.g. setting IMPORT on a non-empty registry)")
	return cmd
}

// printMode prints mode results as a table (or JSON), surfacing any per-subject
// errors. It returns ErrSilent if any result carried an error.
func printMode(cl *client.Client, command string, results []sr.ModeResult) error {
	var anyErr bool
	tw := out.NewFormattedTable(cl.Format(), command, 1, "modes", "SUBJECT", "MODE", "ERROR")
	for _, r := range results {
		subject := r.Subject
		if subject == "" {
			subject = "(global)"
		}
		mode := r.Mode.String()
		errStr := ""
		if r.Err != nil {
			anyErr = true
			mode = ""
			errStr = r.Err.Error()
		}
		tw.Row(subject, mode, errStr)
	}
	tw.Flush()
	if anyErr {
		return out.ErrSilent
	}
	return nil
}
