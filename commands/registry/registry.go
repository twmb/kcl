// Package registry contains cobra commands for administering a Schema
// Registry: registering and fetching schemas, listing and deleting subjects
// and versions, and reading or setting compatibility levels and modes.
//
// It is a thin CLI over franz-go's pkg/sr typed API. The registry is a
// separate HTTP service from the Kafka brokers, configured independently via
// -R/--registry, -X registry.urls=..., or the schema_registry section of the
// config file.
package registry

import (
	"errors"
	"fmt"
	"io"
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
		Long: `Schema Registry administration.

The Schema Registry is a separate HTTP service from the Kafka brokers. Point
kcl at it with any of:

  -R, --registry        comma-separated registry URLs (highest priority)
  -X registry.urls=...  same, via the generic config-opt flag
  config file           a [schema_registry] section (or per-profile)

Auth is optional: basic auth via registry.user / registry.pass, or a bearer
token via registry.bearer_token. TLS for https URLs is configured with the
registry.tls.* keys, mirroring the Kafka tls.* keys.

Examples:

  kcl registry -R http://localhost:8081 subjects
  kcl registry schema create mytopic-value -s schema.avsc
  kcl registry schema get -S mytopic-value
  kcl registry compat set BACKWARD mytopic-value
`,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			cl.SetRegistryContext(context)
			return nil
		},
	}

	cmd.PersistentFlags().StringVar(&context, "context", "", "schema registry context (namespace) to scope operations to")

	cmd.AddCommand(
		subjectsCommand(cl),
		schemaCommand(cl),
		versionsCommand(cl),
		referencesCommand(cl),
		deleteCommand(cl),
		compatCommand(cl),
		modeCommand(cl),
		contextCommand(cl),
	)

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
			return "", out.Errf(out.ExitError, "unable to read schema file %q: %v", path, err)
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

// versionString renders a version int for display, mapping -1 to "latest".
func versionString(v int) string {
	if v < 0 {
		return "latest"
	}
	return fmt.Sprintf("%d", v)
}
