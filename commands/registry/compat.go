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
		Short:   "Get or set schema compatibility levels.",
		Long: `Get or set schema compatibility levels.

Compatibility levels are one of:
  NONE, BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
  FULL, FULL_TRANSITIVE

With no subjects, "get" and "set" operate on the global default; with subjects,
they operate on each subject's override.

EXAMPLES:
  kcl registry compatibility get                       # the global default
  kcl registry compatibility set FULL mytopic-value    # one subject's override

SEE ALSO:
  kcl registry schema check-compatibility   check a schema at the level in force
  kcl registry mode                         the registry's read/write mode
`,
	}
	cmd.AddCommand(
		compatGetCommand(cl),
		compatSetCommand(cl),
		oldName(schemaCheckCompatibilityCommand(cl), "test SUBJECT", "registry schema check-compatibility"),
	)
	return cmd
}

func compatGetCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get [SUBJECTS...]",
		Short: "Get the global or per-subject compatibility level.",
		Long: `Get the global or per-subject compatibility level.

With no subjects, the global default is printed under the subject "(global)".
With subjects, each subject's level is printed: its own override, or the
global default when it has none.

EXAMPLES:
  kcl registry compatibility get                    # the global default
  kcl registry compatibility get a-value b-value    # two subjects

SEE ALSO:
  kcl registry compatibility set   change a level
`,
		RunE: func(_ *cobra.Command, args []string) error {
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			results := scl.Compatibility(context.Background(), args...)
			return printCompat(cl, "get compatibility", false, results)
		},
	}
	out.Columns(cmd, "SUBJECT", "LEVEL", "ERROR")
	return cmd
}

func compatSetCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set LEVEL [SUBJECTS...]",
		Short: "Set the global or per-subject compatibility level.",
		Long: `Set the global or per-subject compatibility level.

With no subjects, the global default is set. With subjects, each subject's
override is set. One row per subject: SUBJECT LEVEL ERROR MESSAGE, and the
command exits 1 when the registry refused any of them.

EXAMPLES:
  kcl registry compatibility set BACKWARD               # the global default
  kcl registry compatibility set FULL a-value b-value   # two subjects' overrides

SEE ALSO:
  kcl registry compatibility get   the level in force
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			var level sr.CompatibilityLevel
			if err := level.UnmarshalText([]byte(args[0])); err != nil {
				return out.Errf(out.ExitUsage, "%v (valid: none, backward, backward_transitive, forward, forward_transitive, full, full_transitive)", err)
			}
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			results := scl.SetCompatibility(context.Background(), sr.SetCompatibility{Level: level}, args[1:]...)
			return printCompat(cl, "set compatibility", true, results)
		},
	}
	out.Columns(cmd, "SUBJECT", "LEVEL", "ERROR", "MESSAGE")
	return cmd
}

func printCompat(cl *client.Client, action string, set bool, results []sr.CompatibilityResult) error {
	rows := make([]subjectResult, len(results))
	for i, r := range results {
		rows[i] = subjectResult{r.Subject, r.Level.String(), r.Err}
	}
	return printSubjectResults(cl, action, "compatibility", "LEVEL", set, rows)
}

// subjectResult is what the registry answered for one subject: the value,
// a compatibility level or a mode, or an error.
type subjectResult struct {
	subject string
	value   string
	err     error
}

// printSubjectResults prints one row per subject: SUBJECT, the value under
// header, ERROR, and MESSAGE too when the command set something. The global
// value is under the subject "(global)". A registry error fills the row's
// ERROR, so the command exits 1; any other error fails the command.
func printSubjectResults(cl *client.Client, action, key, header string, set bool, results []subjectResult) error {
	// A set is a mutation and prints OK; a get describes and prints
	// nothing under ERROR.
	var tw *out.FormattedTable
	if set {
		tw = out.NewFormattedTable(cl.Format(), cl.Command(), 1, key, "SUBJECT", header, "ERROR", "MESSAGE").ResultColumns()
	} else {
		tw = out.NewFormattedTable(cl.Format(), cl.Command(), 1, key, "SUBJECT", header, "ERROR").ErrorColumn()
	}
	for _, r := range results {
		subject := r.subject
		if subject == "" {
			subject = "(global)"
		}
		errName, message, ok := resultCells(r.err)
		if !ok {
			return dieErr(action, r.err)
		}
		var value any = r.value
		if r.err != nil {
			value = out.Unknown
		}
		if set {
			tw.Row(subject, value, errName, message)
		} else {
			tw.Row(subject, value, errName)
		}
	}
	return tw.Flush()
}

func schemaCheckCompatibilityCommand(cl *client.Client) *cobra.Command {
	var (
		schemaPath string
		typeStr    string
		versionStr string
		references []string
		normalize  bool
		verbose    bool
	)
	cmd := &cobra.Command{
		Use:   "check-compatibility SUBJECT",
		Short: "Check whether a schema is compatible with a subject version.",
		Long: `Check whether a schema is compatible with a subject version.

The candidate schema is read from -s/--schema or stdin and checked at the
subject's compatibility level, without registering it. --version selects
which existing version to check against: "latest" by default, a number, or
"all" to have the registry check every version the level calls for.

The row is SUBJECT VERSION COMPATIBLE. "latest" is resolved to its number
first, so that the row names the version checked; under -v all the registry
answers once for every version, so VERSION is a dash (null in JSON). The
command exits 1 when the schema is not compatible, so a script can read
either the row or the exit code; pass --verbose to have the registry say why,
on stderr in text and awk and under "messages" in JSON.

EXAMPLES:
  kcl registry schema check-compatibility foo-value -s new.avsc          # against the latest version
  kcl registry schema check-compatibility foo-value -s new.avsc -v all   # against every version
  kcl registry schema check-compatibility foo-value -s new.avsc --verbose --format json

SEE ALSO:
  kcl registry compatibility get   the level a subject is checked at
  kcl registry compatibility set   change that level
  kcl registry schema create       register the schema once it passes
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
			version, err = resolveVersion(ctx, scl, subject, version)
			if err != nil {
				return dieErr("check compatibility", err)
			}
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
				return dieErr("check compatibility", err)
			}
			if res.Messages == nil {
				res.Messages = []string{}
			}
			var outVersion any = version
			if version == -2 {
				outVersion = out.Unknown
			}

			if cl.Format() == out.FormatJSON {
				out.MarshalJSON(cl.Command(), 1, map[string]any{
					"subject":    subject,
					"version":    outVersion,
					"compatible": res.Is,
					"messages":   res.Messages,
				})
			} else {
				tw := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "compatibility", "SUBJECT", "VERSION", "COMPATIBLE")
				tw.Row(subject, outVersion, res.Is)
				tw.Flush()
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
	out.Columns(cmd, "SUBJECT", "VERSION", "COMPATIBLE")
	return cmd
}

// parseCheckVersion is like parseVersion but also accepts "all" (-2), which the
// registry interprets as "check against every version".
func parseCheckVersion(s string) (int, error) {
	if strings.EqualFold(s, "all") {
		return -2, nil
	}
	v, err := parseVersion(s)
	if err != nil {
		return 0, out.Errf(out.ExitUsage, "invalid version %q: must be a positive integer, 'latest', or 'all'", s)
	}
	return v, nil
}
