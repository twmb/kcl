package serde

import (
	"fmt"

	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"
)

// avroCodec converts between JSON and Avro binary using a parsed schema.
//
// JSON in -> binary: the input JSON is decoded against the schema into a
// native Go value, then re-encoded as Avro binary. JSON out <- binary: the
// reverse, decoding Avro binary into a native value and encoding it as JSON.
type avroCodec struct {
	schema *avro.Schema
	// tagUnions has decode print each union value as {"branch": value}.
	tagUnions bool
}

func newAvroCodec(schemaText string, refs []sr.SchemaReference, fetch refFetcher, tagUnions bool) (*avroCodec, error) {
	// Each codec parses into a cache of its own. A cache refuses a second
	// schema that defines a name the cache already has, even with the same
	// definition, and two schemas from one registry often define the same
	// name, such as two versions of one subject.
	//
	// Referenced schemas define named types this schema uses. They must be
	// parsed into the cache (dependencies first) before this schema so the
	// named types resolve.
	var cache avro.SchemaCache
	if err := parseAvroRefs(&cache, refs, fetch, map[string]bool{}); err != nil {
		return nil, err
	}
	schema, err := cache.Parse(schemaText)
	if err != nil {
		return nil, fmt.Errorf("unable to parse avro schema: %w", err)
	}
	return &avroCodec{schema: schema, tagUnions: tagUnions}, nil
}

func parseAvroRefs(cache *avro.SchemaCache, refs []sr.SchemaReference, fetch refFetcher, seen map[string]bool) error {
	for _, ref := range refs {
		key := refKey(ref)
		if seen[key] {
			continue
		}
		seen[key] = true
		if fetch == nil {
			return fmt.Errorf("avro schema references %q but no registry is available to resolve it", ref.Name)
		}
		rs, err := fetch(ref.Subject, ref.Version)
		if err != nil {
			return fmt.Errorf("unable to fetch referenced schema %q (%s): %w", ref.Name, key, err)
		}
		if err := parseAvroRefs(cache, rs.References, fetch, seen); err != nil {
			return err
		}
		if _, err := cache.Parse(rs.Schema); err != nil {
			return fmt.Errorf("unable to parse referenced avro schema %q: %w", ref.Name, err)
		}
	}
	return nil
}

func (c *avroCodec) encode(jsonIn []byte, _ []int) ([]byte, error) {
	// TaggedUnions keeps the branch DecodeJSON reads for each union value,
	// the branch a tagged value names or the first branch a bare value fits,
	// so Encode writes that branch rather than picking one from the Go value.
	var native any
	if err := c.schema.DecodeJSON(jsonIn, &native, avro.TaggedUnions()); err != nil {
		return nil, fmt.Errorf("input does not match avro schema: %w", err)
	}
	body, err := c.schema.Encode(native)
	if err != nil {
		return nil, fmt.Errorf("unable to encode avro binary: %w", err)
	}
	return body, nil
}

func (c *avroCodec) decode(body []byte, _ []int) ([]byte, error) {
	// TaggedUnions keeps the branch of each union value, so EncodeJSON writes
	// each value as the branch it was written with rather than as a branch it
	// picks from the Go value.
	var native any
	if _, err := c.schema.Decode(body, &native, avro.TaggedUnions()); err != nil {
		return nil, fmt.Errorf("unable to decode avro binary: %w", err)
	}
	var opts []avro.Opt
	if c.tagUnions {
		opts = append(opts, avro.TaggedUnions())
	}
	jsonOut, err := c.schema.EncodeJSON(native, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to encode avro value as JSON: %w", err)
	}
	return jsonOut, nil
}
