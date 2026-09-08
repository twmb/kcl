package registry

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func schemaCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "schema",
		Aliases: []string{"s"},
		Short:   "Register and fetch schemas.",
	}
	cmd.AddCommand(
		schemaCreateCommand(cl),
		schemaGetCommand(cl),
		schemaListCommand(cl),
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

  kcl registry schema create mytopic-value -s schema.avsc
  cat schema.avsc | kcl registry schema create mytopic-value
  kcl registry schema create mytopic-value -t protobuf -s msg.proto \
      -r 'other.proto:other-value:1'
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
			if err != nil {
				return dieErr("register schema", err)
			}

			if cl.Format() == out.FormatJSON {
				out.MarshalJSON("registry.schema.register", 1, map[string]any{
					"subject": ss.Subject,
					"version": ss.Version,
					"id":      ss.ID,
					"type":    typ.String(),
				})
				return nil
			}
			tw := out.NewFormattedTable(cl.Format(), "registry.schema.register", 1, "schemas", "SUBJECT", "VERSION", "ID")
			tw.Row(ss.Subject, ss.Version, ss.ID)
			tw.Flush()
			return nil
		},
	}
	cmd.Flags().StringVarP(&schemaPath, "schema", "s", "", "path to the schema file, or - for stdin (default stdin)")
	cmd.Flags().StringVarP(&typeStr, "type", "t", "avro", "schema type: avro, protobuf, or json")
	cmd.Flags().StringArrayVarP(&references, "reference", "r", nil, "schema reference in name:subject:version form (repeatable)")
	cmd.Flags().BoolVar(&normalize, "normalize", false, "ask the registry to normalize the schema before registering")
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
		Use:     "get",
		Aliases: []string{"describe", "fetch"},
		Short:   "Fetch a schema by id, or by subject and version.",
		Long: `Fetch a schema by id, or by subject and version.

Fetch a schema by global id, or by subject and version.

Exactly one of --id or --subject must be given. With --subject, --version
defaults to "latest"; pass a specific version number to fetch an older one.

By default only the schema text is printed (so it can be piped). Use --meta to
also print the subject/version/id/type to stderr. In JSON output format the
full structured schema (including references) is always printed.

  kcl registry schema get --id 5
  kcl registry schema get -S mytopic-value
  kcl registry schema get -S mytopic-value -v 2 --meta
`,
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if (id > 0) == (subject != "") {
				return out.Errf(out.ExitUsage, "exactly one of --id or --subject must be specified")
			}

			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			ctx := context.Background()

			var (
				schema       sr.Schema
				outSubject   string
				outVersion   int
				haveSubjVers bool
				outID        = id
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
				haveSubjVers = true
			}

			if cl.Format() == out.FormatJSON {
				fields := map[string]any{
					"id":     outID,
					"type":   schema.Type.String(),
					"schema": schema.Schema,
				}
				if haveSubjVers {
					fields["subject"] = outSubject
					fields["version"] = outVersion
				}
				if len(schema.References) > 0 {
					fields["references"] = schema.References
				}
				out.MarshalJSON("registry.schema.get", 1, fields)
				return nil
			}

			if meta {
				if haveSubjVers {
					fmt.Fprintf(os.Stderr, "subject=%s version=%d id=%d type=%s\n", outSubject, outVersion, outID, schema.Type)
				} else {
					fmt.Fprintf(os.Stderr, "id=%d type=%s\n", outID, schema.Type)
				}
			}
			fmt.Println(schema.Schema)
			return nil
		},
	}
	cmd.Flags().IntVarP(&id, "id", "i", 0, "global schema id to fetch")
	cmd.Flags().StringVarP(&subject, "subject", "S", "", "subject to fetch a schema from")
	cmd.Flags().StringVarP(&versionStr, "version", "v", "latest", "version to fetch with --subject (number or 'latest')")
	cmd.Flags().BoolVarP(&meta, "meta", "m", false, "print subject/version/id/type metadata to stderr in text mode")
	return cmd
}

func schemaListCommand(cl *client.Client) *cobra.Command {
	var showDeleted bool
	cmd := &cobra.Command{
		Use:     "list [SUBJECT]",
		Aliases: []string{"ls"},
		Short:   "List schemas across all subjects, or all versions of one subject.",
		Long: `List schemas across all subjects, or all versions of one subject.

List registered schemas.

With no argument, lists every schema across all subjects. With a SUBJECT, lists
that subject's schema versions. Each row shows the subject, version, id, and
type.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			scl, err := srClient(cl)
			if err != nil {
				return err
			}
			ctx := context.Background()
			if showDeleted {
				ctx = sr.WithParams(ctx, sr.ShowDeleted)
			}

			var schemas []sr.SubjectSchema
			if len(args) == 1 {
				schemas, err = scl.Schemas(ctx, args[0])
			} else {
				schemas, err = scl.AllSchemas(ctx)
			}
			if err != nil {
				return dieErr("list schemas", err)
			}

			tw := out.NewFormattedTable(cl.Format(), "registry.schema.list", 1, "schemas", "SUBJECT", "VERSION", "ID", "TYPE")
			for _, s := range schemas {
				tw.Row(s.Subject, s.Version, s.ID, s.Type)
			}
			tw.Flush()
			return nil
		},
	}
	cmd.Flags().BoolVar(&showDeleted, "show-deleted", false, "include soft-deleted schemas")
	return cmd
}
