package registry

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func compatCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "compatibility",
		Aliases: []string{"compat"},
		Short:   "Get, set, or test schema compatibility levels.",
		Long: `Get, set, or test schema compatibility levels.

Compatibility levels are one of:
  NONE, BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
  FULL, FULL_TRANSITIVE

With no subjects, "get" and "set" operate on the global default; with subjects,
they operate on each subject's override.`,
	}
	cmd.AddCommand(
		compatGetCommand(cl),
		compatSetCommand(cl),
		compatTestCommand(cl),
	)
	return cmd
}

func compatGetCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get [SUBJECTS...]",
		Short: "Get the global or per-subject compatibility level.",
		RunE: func(_ *cobra.Command, args []string) error {
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			results := scl.Compatibility(context.Background(), args...)
			return printCompat(cl, results)
		},
	}
	return cmd
}

func compatSetCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set LEVEL [SUBJECTS...]",
		Short: "Set the global or per-subject compatibility level.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			var level sr.CompatibilityLevel
			if err := level.UnmarshalText([]byte(args[0])); err != nil {
				return out.Errf(out.ExitUsage, "%v", err)
			}
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			results := scl.SetCompatibility(context.Background(), sr.SetCompatibility{Level: level}, args[1:]...)
			return printCompat(cl, results)
		},
	}
	return cmd
}

func compatTestCommand(cl *client.Client) *cobra.Command {
	var (
		schemaPath string
		typeStr    string
		versionStr string
		references []string
		normalize  bool
		verbose    bool
	)
	cmd := &cobra.Command{
		Use:   "test SUBJECT",
		Short: "Test whether a schema is compatible with a subject version.",
		Long: `Test whether a schema is compatible with a subject version.

Test whether a candidate schema is compatible with an existing subject version.

The candidate schema is read from -s/--schema or stdin. --version selects which
existing version to check against ("latest" by default, or "all" to check
against every version). Exits non-zero if the schema is not compatible; pass
--verbose to have the registry explain why.

--format awk prints one word, true or false, and nothing else. The exit code
says the same thing, so a script can read either.

EXAMPLES:
  kcl registry compatibility test foo-value -s new.avsc          # check the latest version
  kcl registry compatibility test foo-value -s new.avsc -v all   # check every version
  kcl registry compatibility test foo-value -s new.avsc --format awk

SEE ALSO:
  kcl registry compatibility get    the level a subject is checked at
  kcl registry compatibility set    change that level
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			subject := args[0]
			typ, err := parseSchemaType(typeStr)
			if err != nil {
				return err
			}
			text, err := readSchema(schemaPath)
			if err != nil {
				return err
			}
			refs, err := parseReferences(references)
			if err != nil {
				return err
			}
			version, err := parseCheckVersion(versionStr)
			if err != nil {
				return err
			}

			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			ctx := context.Background()
			var params []sr.Param
			if normalize {
				params = append(params, sr.Normalize)
			}
			if verbose {
				params = append(params, sr.Verbose)
			}
			if len(params) > 0 {
				ctx = sr.WithParams(ctx, params...)
			}
			res, err := scl.CheckCompatibility(ctx, subject, version, sr.Schema{
				Schema:     text,
				Type:       typ,
				References: refs,
			})
			if err != nil {
				return dieErr("test compatibility", err)
			}

			switch cl.Format() {
			case out.FormatJSON:
				out.MarshalJSON(cl.Command(), 1, map[string]any{
					"subject":    subject,
					"version":    versionString(version),
					"compatible": res.Is,
					"messages":   res.Messages,
				})
			case out.FormatAWK:
				// One word, so that "if [ $(kcl ... --format awk) =
				// true ]" reads. The label belongs to text.
				fmt.Printf("%v\n", res.Is)
				for _, m := range res.Messages {
					fmt.Fprintln(os.Stderr, m)
				}
			default:
				fmt.Printf("compatible: %v\n", res.Is)
				for _, m := range res.Messages {
					fmt.Fprintln(os.Stderr, m)
				}
			}
			if !res.Is {
				return out.ErrSilent
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&schemaPath, "schema", "s", "", "path to the candidate schema file, or - for stdin (default stdin)")
	cmd.Flags().StringVarP(&typeStr, "type", "t", "avro", "schema type: avro, protobuf, or json")
	cmd.Flags().StringVarP(&versionStr, "version", "v", "latest", "existing version to check against (number, 'latest', or 'all')")
	cmd.Flags().StringArrayVarP(&references, "reference", "r", nil, "schema reference in name:subject:version form (repeatable)")
	cmd.Flags().BoolVar(&normalize, "normalize", false, "ask the registry to normalize schemas before comparing")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "ask the registry to return the reasons for any incompatibility")
	return cmd
}

// parseCheckVersion is like parseVersion but also accepts "all" (-2), which the
// registry interprets as "check against every version".
func parseCheckVersion(s string) (int, error) {
	if strings.EqualFold(s, "all") {
		return -2, nil
	}
	return parseVersion(s)
}

// printCompat prints compatibility results as a table (or JSON), surfacing any
// per-subject errors. It returns ErrSilent if any result carried an error so
// the process exits non-zero.
func printCompat(cl *client.Client, results []sr.CompatibilityResult) error {
	var anyErr bool
	tw := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "compatibility", "SUBJECT", "LEVEL", "ERROR")
	for _, r := range results {
		subject := r.Subject
		if subject == "" {
			subject = "(global)"
		}
		level := r.Level.String()
		errStr := ""
		if r.Err != nil {
			anyErr = true
			level = ""
			errStr = r.Err.Error()
		}
		tw.Row(subject, level, errStr)
	}
	tw.Flush()
	if anyErr {
		return out.ErrSilent
	}
	return nil
}
