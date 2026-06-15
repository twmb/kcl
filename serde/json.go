package serde

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/twmb/franz-go/pkg/sr"
)

// jsonCodec validates JSON against a JSON Schema and ships the JSON bytes
// unchanged (compacted). JSON Schema is "self-describing" on the wire: the
// payload is the JSON document itself, so encode/decode only validate and
// normalize whitespace rather than transcode.
type jsonCodec struct {
	schema *jsonschema.Schema
}

func newJSONCodec(schemaText string, refs []sr.SchemaReference, fetch refFetcher) (*jsonCodec, error) {
	c := jsonschema.NewCompiler()
	// Referenced schemas are added under their reference name (the $ref URI/$id
	// the main schema uses) so the compiler resolves $refs locally.
	if err := addJSONRefs(c, refs, fetch, map[string]bool{}); err != nil {
		return nil, err
	}
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(schemaText))
	if err != nil {
		return nil, fmt.Errorf("unable to parse JSON schema: %w", err)
	}
	const loc = "schema.json"
	if err := c.AddResource(loc, doc); err != nil {
		return nil, fmt.Errorf("unable to add JSON schema: %w", err)
	}
	schema, err := c.Compile(loc)
	if err != nil {
		return nil, fmt.Errorf("unable to compile JSON schema: %w", err)
	}
	return &jsonCodec{schema: schema}, nil
}

func addJSONRefs(c *jsonschema.Compiler, refs []sr.SchemaReference, fetch refFetcher, seen map[string]bool) error {
	for _, ref := range refs {
		if seen[ref.Name] {
			continue
		}
		seen[ref.Name] = true
		if fetch == nil {
			return fmt.Errorf("JSON schema references %q but no registry is available to resolve it", ref.Name)
		}
		rs, err := fetch(ref.Subject, ref.Version)
		if err != nil {
			return fmt.Errorf("unable to fetch referenced schema %q (%s): %w", ref.Name, refKey(ref), err)
		}
		if err := addJSONRefs(c, rs.References, fetch, seen); err != nil {
			return err
		}
		doc, err := jsonschema.UnmarshalJSON(strings.NewReader(rs.Schema))
		if err != nil {
			return fmt.Errorf("unable to parse referenced JSON schema %q: %w", ref.Name, err)
		}
		if err := c.AddResource(ref.Name, doc); err != nil {
			return fmt.Errorf("unable to add referenced JSON schema %q: %w", ref.Name, err)
		}
	}
	return nil
}

// validate parses b as JSON and validates it against the schema.
func (c *jsonCodec) validate(b []byte) error {
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("input is not valid JSON: %w", err)
	}
	if err := c.schema.Validate(inst); err != nil {
		return fmt.Errorf("input does not match JSON schema: %w", err)
	}
	return nil
}

func (c *jsonCodec) encode(jsonIn []byte, _ []int) ([]byte, error) {
	if err := c.validate(jsonIn); err != nil {
		return nil, err
	}
	var compacted bytes.Buffer
	if err := json.Compact(&compacted, jsonIn); err != nil {
		return nil, fmt.Errorf("unable to compact JSON: %w", err)
	}
	return compacted.Bytes(), nil
}

func (c *jsonCodec) decode(body []byte, _ []int) ([]byte, error) {
	// Decoding is an inspection path: show what is actually on the wire rather
	// than rejecting records that a non-validating producer (or schema drift)
	// may have written. The body is already the JSON document, so it is
	// returned unchanged without re-validating against the schema.
	return body, nil
}
