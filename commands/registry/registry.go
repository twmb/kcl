// Package registry contains cobra commands for administering a Schema
// Registry: registering and fetching schemas, listing and deleting subjects
// and versions, and reading or setting compatibility levels and modes.
//
// It is a thin CLI over franz-go's pkg/sr typed API. The registry is a
// separate HTTP service from the Kafka brokers, configured independently via
// -R/--registry, -X registry.urls=..., or the [registry] section of the
// config file.
package registry

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

// Command returns the parent "registry" command and all of its subcommands.
func Command(cl *client.Client) *cobra.Command {
	var context string
	cmd := &cobra.Command{
		Use:     "registry",
		Aliases: []string{"sr"},
		Short:   "Schema Registry administration (schemas, subjects, compatibility, mode).",
		Long: `Schema Registry administration (schemas, subjects, compatibility, mode).

The Schema Registry is a separate HTTP service from the Kafka brokers. Point
kcl at it with any of:

  -R, --registry        comma-separated registry URLs (highest priority)
  -X registry.urls=...  same, via the generic config-opt flag
  config file           a [registry] section (or per-profile)

Auth is optional: basic auth via registry.user / registry.pass, or a bearer
token via registry.bearer_token. TLS for https URLs is configured with the
registry.tls.* keys, mirroring the Kafka tls.* keys.

EXAMPLES:
  kcl registry -R http://localhost:8081 subject list       # the subjects in a registry
  kcl registry schema create mytopic-value -s schema.avsc  # register a schema
  kcl registry schema get mytopic-value                    # print its latest version
  kcl registry compatibility set BACKWARD mytopic-value    # set a subject's level

SEE ALSO:
  kcl registry subject         list and delete subjects
  kcl registry schema          register, fetch, list, delete, and check schemas
  kcl registry compatibility   get and set compatibility levels
  kcl registry mode            get and set the registry mode
  kcl registry context         list and delete contexts
`,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			cl.SetRegistryContext(context)
			return nil
		},
	}

	cmd.PersistentFlags().StringVar(&context, "context", "", "schema registry context (namespace) to scope operations to")

	cmd.AddCommand(
		subjectCommand(cl),
		schemaCommand(cl),
		compatCommand(cl),
		modeCommand(cl),
		contextCommand(cl),

		// The old names, kept so that a script keeps working.
		oldName(cl, subjectListCommand(cl), "subjects", "registry subject list"),
		oldName(cl, versionsCommand(cl), "versions SUBJECT", "registry schema list"),
		oldName(cl, schemaReferencesCommand(cl), "references SUBJECT", "registry schema references"),
		oldDeleteCommand(cl),
	)

	return cmd
}

// oldName makes cmd the old name of the command at path, kept so that an old
// script keeps working. It is out of the help, cobra notes the new name on
// stderr, and what it prints names the new path.
func oldName(cl *client.Client, cmd *cobra.Command, use, path string) *cobra.Command {
	cmd.Use = use
	cmd.Hidden = true
	cmd.Deprecated = "use 'kcl " + path + "' instead"
	cmd.PreRun = func(*cobra.Command, []string) {
		cl.SetCommand(out.CommandName("kcl " + path))
	}
	return cmd
}

// srClient builds the configured Schema Registry client. The registry URL
// defaults to localhost:8081 when unset, so the errors surfaced here are
// configuration conflicts (e.g. both bearer token and basic auth) or TLS
// setup failures; those are reported as usage errors (exit code 2).
func srClient(cl *client.Client) (*sr.Client, error) {
	scl, err := cl.SchemaRegistryClient()
	if err != nil {
		return nil, out.Errf(out.ExitUsage, "%v", err)
	}
	return scl, nil
}

// dieErr converts a registry error into a kcl error, unwrapping the registry's
// structured ResponseError into a concise message including the error code.
func dieErr(action string, err error) error {
	var re *sr.ResponseError
	if errors.As(err, &re) {
		msg := re.Message
		if msg == "" {
			msg = strings.TrimSpace(string(re.Raw))
		}
		if msg == "" {
			return out.Errf(out.ExitError, "unable to %s: HTTP %d (the registry may not support this operation)", action, re.StatusCode)
		}
		return out.Errf(out.ExitError, "unable to %s: %s (error code %d, http %d)", action, msg, re.ErrorCode, re.StatusCode)
	}
	return out.Errf(out.ExitError, "unable to %s: %v", action, err)
}

// resultCells is the ERROR and MESSAGE of a result row whose request the
// registry answered with err: "" and "" on success, else the registry's error
// name (SUBJECT_NOT_FOUND), or its code when we do not know the name, and its
// message. Anything but a registry answer, a refused connection say, is not a
// per-item result: ok is false and the caller returns err through dieErr.
func resultCells(err error) (errName, message string, ok bool) {
	if err == nil {
		return "", "", true
	}
	var re *sr.ResponseError
	if !errors.As(err, &re) {
		return "", "", false
	}
	switch e := re.SchemaError(); {
	case e != nil && e != sr.ErrUnknown:
		errName = e.Name
	case re.ErrorCode != 0:
		errName = strconv.Itoa(re.ErrorCode)
	default:
		errName = "HTTP_" + strconv.Itoa(re.StatusCode)
	}
	message = re.Message
	if message == "" {
		message = strings.TrimSpace(string(re.Raw))
	}
	return errName, message, true
}

// parseSchemaType parses an Avro/Protobuf/JSON schema type string.
func parseSchemaType(s string) (sr.SchemaType, error) {
	var t sr.SchemaType
	if err := t.UnmarshalText([]byte(s)); err != nil {
		return 0, out.Errf(out.ExitUsage, "%v (valid: avro, protobuf, json)", err)
	}
	return t, nil
}

// readSchema reads a schema definition from the given path, or from stdin if
// path is "" or "-".
func readSchema(path string) (string, error) {
	var (
		b   []byte
		err error
	)
	if path == "" || path == "-" {
		b, err = io.ReadAll(os.Stdin)
		if err != nil {
			return "", out.Errf(out.ExitError, "unable to read schema from stdin: %v", err)
		}
	} else {
		b, err = os.ReadFile(path)
		if err != nil {
			// os.ReadFile wraps the reason in a PathError that repeats
			// the path, so the unwrapped reason is what we print: the
			// path is already in the sentence, once, unquoted.
			reason := err
			var pe *fs.PathError
			if errors.As(err, &pe) {
				reason = pe.Err
			}
			return "", out.Errf(out.ExitUsage, "unable to read schema file %s: %v", path, reason)
		}
	}
	if len(b) == 0 {
		return "", out.Errf(out.ExitUsage, "schema is empty")
	}
	return string(b), nil
}

// parseReferences parses repeated "name:subject:version" reference specs.
func parseReferences(refs []string) ([]sr.SchemaReference, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	out := make([]sr.SchemaReference, 0, len(refs))
	for _, ref := range refs {
		name, rest, ok := strings.Cut(ref, ":")
		if !ok {
			return nil, refErr(ref)
		}
		subject, vstr, ok := strings.Cut(rest, ":")
		if !ok {
			return nil, refErr(ref)
		}
		version, err := strconv.Atoi(vstr)
		if err != nil {
			return nil, refErr(ref)
		}
		out = append(out, sr.SchemaReference{Name: name, Subject: subject, Version: version})
	}
	return out, nil
}

func refErr(ref string) error {
	return out.Errf(out.ExitUsage, "invalid reference %q: must be name:subject:version", ref)
}

// parseVersion parses a version flag value, accepting "latest" as -1.
func parseVersion(s string) (int, error) {
	if s == "" || strings.EqualFold(s, "latest") {
		return -1, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 1 {
		return 0, out.Errf(out.ExitUsage, "invalid version %q: must be a positive integer or 'latest'", s)
	}
	return v, nil
}

// resolveVersion is version as a number: itself, or, for -1, the subject's
// latest version as the registry reports it now. A delete or a compatibility
// check answers nothing about which version "latest" was, so we ask first,
// act on the number, and the row names what was done.
func resolveVersion(ctx context.Context, scl *sr.Client, subject string, version int) (int, error) {
	if version != -1 {
		return version, nil
	}
	ss, err := scl.SchemaByVersion(ctx, subject, -1)
	if err != nil {
		return 0, err
	}
	return ss.Version, nil
}

// awkText is s as one awk field. A schema can span lines, a .proto most of
// all, and a row that spans lines is not TSV. The escapes are the ones Go and
// JSON already write, so a reader knows them.
func awkText(s string) string {
	return awkEscaper.Replace(s)
}

var awkEscaper = strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\r", "\\r", "\t", "\\t")

// showDeletedFlag adds --show-deleted to cmd and returns the context a list
// runs under, which asks the registry for soft deleted entries too when the
// flag is set.
func showDeletedFlag(cmd *cobra.Command, what string) func() context.Context {
	var showDeleted bool
	cmd.Flags().BoolVar(&showDeleted, "show-deleted", false, "include soft-deleted "+what)
	return func() context.Context {
		ctx := context.Background()
		if showDeleted {
			ctx = sr.WithParams(ctx, sr.ShowDeleted)
		}
		return ctx
	}
}
