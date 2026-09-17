package registry

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func schemaCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "schema",
		Aliases: []string{"s"},
		Short:   "Register, fetch, list, delete, and check schemas.",
		Long: `Register, fetch, list, delete, and check schemas.

A schema is registered under a subject and gets a version within that subject
and an id that is global to the registry (or to its context). The id is what a
record's wire format carries.

EXAMPLES:
  kcl registry schema create mytopic-value -s schema.avsc            # register
  kcl registry schema get mytopic-value                              # the latest version's text
  kcl registry schema list mytopic-value                             # the versions under a subject
  kcl registry schema check-compatibility mytopic-value -s new.avsc  # would this register?

SEE ALSO:
  kcl registry subject         list and delete subjects
  kcl registry compatibility   the level a subject's versions are checked at
`,
	}
	cmd.AddCommand(
		schemaCreateCommand(cl),
		schemaGetCommand(cl),
		schemaListCommand(cl),
		schemaDeleteCommand(cl),
		schemaReferencesCommand(cl),
		schemaCheckCompatibilityCommand(cl),
	)
	return cmd
}

func schemaCreateCommand(cl *client.Client) *cobra.Command {
	var (
		schemaPath string
		typeStr    string
		references []string
		normalize  bool
	)
	cmd := &cobra.Command{
		Use:     "create SUBJECT",
		Aliases: []string{"register", "add"},
		Short:   "Register a schema under a subject.",
		Long: `Register a schema under a subject.

The schema is read from the file given with -s/--schema, or from stdin if the
flag is omitted or set to "-". The subject is given as the sole positional
argument; by the default TopicNameStrategy this is "<topic>-value" for record
values or "<topic>-key" for record keys.

References to other registered schemas can be given with -r/--reference, each
in "name:subject:version" form (repeatable).

The row is SUBJECT VERSION ID ERROR MESSAGE. Registering a schema the subject
already has answers the existing version and id. ERROR names the registry's
error when it refused the schema, and the command exits 1.

EXAMPLES:
  kcl registry schema create mytopic-value -s schema.avsc
  cat schema.avsc | kcl registry schema create mytopic-value
  kcl registry schema create mytopic-value -t protobuf -s msg.proto -r 'other.proto:other-value:1'

SEE ALSO:
  kcl registry schema check-compatibility   check a schema against what is registered, without registering it
  kcl registry schema get                   fetch what was registered
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

			scl, err := srClient(cl)
			if err != nil {
				return err
			}

			ctx := context.Background()
			if normalize {
				ctx = sr.WithParams(ctx, sr.Normalize)
			}
			ss, err := scl.CreateSchema(ctx, subject, sr.Schema{
				Schema:     text,
				Type:       typ,
				References: refs,
			})
			errName, message, ok := resultCells(err)
			if !ok {
				return dieErr("register schema", err)
			}
			tw := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "schemas", "SUBJECT", "VERSION", "ID", "ERROR", "MESSAGE").ResultColumns()
			if err != nil {
				tw.Row(subject, out.Unknown, out.Unknown, errName, message)
			} else {
				tw.Row(ss.Subject, ss.Version, ss.ID, errName, message)
			}
			return tw.Flush()
		},
	}
	cmd.Flags().StringVarP(&schemaPath, "schema", "s", "", "path to the schema file, or - for stdin (default stdin)")
	cmd.Flags().StringVarP(&typeStr, "type", "t", "avro", "schema type: avro, protobuf, or json")
	cmd.Flags().StringArrayVarP(&references, "reference", "r", nil, "schema reference in name:subject:version form (repeatable)")
	cmd.Flags().BoolVar(&normalize, "normalize", false, "ask the registry to normalize the schema before registering")
	out.Columns(cmd, "SUBJECT", "VERSION", "ID", "ERROR", "MESSAGE")
	return cmd
}

func schemaGetCommand(cl *client.Client) *cobra.Command {
	var (
		id         int
		subject    string
		versionStr string
		meta       bool
	)
	cmd := &cobra.Command{
		Use:     "get [SUBJECT]",
		Aliases: []string{"describe", "fetch"},
		Short:   "Fetch a schema by subject and version, or by id.",
		Long: `Fetch a schema by subject and version, or by id.

Give the subject, or --id for a global schema id. With a subject, --version
defaults to "latest"; pass a version number to fetch an older one.

By default only the schema text is printed (so it can be piped). Use --meta to
also print the subject/version/id/type to stderr. In JSON output format the
full structured schema (including references) is always printed.

--format awk prints one tab separated row: subject, version, id, type, and the
schema text. A schema spanning lines, a .proto for instance, has its newlines
and tabs written as \n and \t so that the row stays one line. Subject and
version are a dash (null in JSON) when you fetched by --id, since one id can
be registered under many subjects.

EXAMPLES:
  kcl registry schema get mytopic-value
  kcl registry schema get mytopic-value -v 2 --meta
  kcl registry schema get --id 5

SEE ALSO:
  kcl registry schema list   list schemas, by subject or across all
`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) == 1 {
				if subject != "" {
					return out.Errf(out.ExitUsage, "subject given both as an argument and with --subject")
				}
				subject = args[0]
			}
			if (id > 0) == (subject != "") {
				return out.Errf(out.ExitUsage, "exactly one of a subject or --id must be given")
			}

			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			ctx := context.Background()

			var (
				schema     sr.Schema
				outSubject any = out.Unknown
				outVersion any = out.Unknown
				outID          = id
			)
			if id > 0 {
				schema, err = scl.SchemaByID(ctx, id)
				if err != nil {
					return dieErr("get schema by id", err)
				}
			} else {
				version, verr := parseVersion(versionStr)
				if verr != nil {
					return verr
				}
				ss, gerr := scl.SchemaByVersion(ctx, subject, version)
				if gerr != nil {
					return dieErr("get schema by subject and version", gerr)
				}
				schema = ss.Schema
				outSubject, outVersion, outID = ss.Subject, ss.Version, ss.ID
			}
			if schema.References == nil {
				schema.References = []sr.SchemaReference{}
			}

			switch cl.Format() {
			case out.FormatJSON:
				out.MarshalJSON(cl.Command(), 1, map[string]any{
					"subject":    outSubject,
					"version":    outVersion,
					"id":         outID,
					"type":       schema.Type.String(),
					"schema":     schema.Schema,
					"references": schema.References,
				})
			case out.FormatAWK:
				out.AwkRow(outSubject, outVersion, outID, schema.Type, awkText(schema.Schema))
			default:
				if meta {
					if id > 0 {
						fmt.Fprintf(os.Stderr, "id=%d type=%s\n", outID, schema.Type)
					} else {
						fmt.Fprintf(os.Stderr, "subject=%s version=%d id=%d type=%s\n", outSubject, outVersion, outID, schema.Type)
					}
				}
				fmt.Println(schema.Schema)
			}
			return nil
		},
	}
	cmd.Flags().IntVarP(&id, "id", "i", 0, "global schema id to fetch")
	cmd.Flags().StringVarP(&subject, "subject", "S", "", "old name for the subject argument")
	cmd.Flags().MarkHidden("subject")
	cmd.Flags().StringVarP(&versionStr, "version", "v", "latest", "version to fetch with a subject (number or 'latest')")
	cmd.Flags().BoolVarP(&meta, "meta", "m", false, "print subject/version/id/type metadata to stderr in text mode")
	out.Columns(cmd, "SUBJECT", "VERSION", "ID", "TYPE", "SCHEMA")
	return cmd
}

func schemaListCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list [SUBJECT]",
		Aliases: []string{"ls"},
		Short:   "List schemas across all subjects, or the versions of one subject.",
		Long: `List schemas across all subjects, or the versions of one subject.

With no argument, lists every schema version across all subjects, sorted by
subject and then version. With a SUBJECT, lists that subject's versions in
order. Each row is the subject, version, id, and type.

EXAMPLES:
  kcl registry schema list                  # everything registered
  kcl registry schema list mytopic-value    # the versions of one subject
  kcl registry schema list --show-deleted   # soft deleted versions too

SEE ALSO:
  kcl registry schema get      the text of one version
  kcl registry subject list    subject names alone
`,
		Args: cobra.MaximumNArgs(1),
	}
	listCtx := showDeletedFlag(cmd, "schemas")
	out.Columns(cmd, "SUBJECT", "VERSION", "ID", "TYPE")
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		scl, err := srClient(cl)
		if err != nil {
			return err
		}
		ctx := listCtx()

		var schemas []sr.SubjectSchema
		if len(args) == 1 {
			schemas, err = scl.Schemas(ctx, args[0])
		} else {
			schemas, err = scl.AllSchemas(ctx)
		}
		if err != nil {
			return dieErr("list schemas", err)
		}
		slices.SortFunc(schemas, func(a, b sr.SubjectSchema) int {
			return cmp.Or(cmp.Compare(a.Subject, b.Subject), cmp.Compare(a.Version, b.Version))
		})

		tw := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "schemas", "SUBJECT", "VERSION", "ID", "TYPE")
		for _, s := range schemas {
			tw.Row(s.Subject, s.Version, s.ID, s.Type)
		}
		return tw.Flush()
	}
	return cmd
}

// versionsCommand is the old "registry versions SUBJECT", now "schema list
// SUBJECT": the same command, with the subject required.
func versionsCommand(cl *client.Client) *cobra.Command {
	cmd := schemaListCommand(cl)
	cmd.Aliases = []string{"vs"}
	cmd.Args = cobra.ExactArgs(1)
	return cmd
}
