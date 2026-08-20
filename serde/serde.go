// Package serde bridges user-facing JSON and the Schema Registry binary wire
// format used by Kafka clients. It is used by "kcl produce" (JSON in, schema
// binary out) and "kcl consume" (schema binary in, JSON out).
//
// Wire framing (the magic 0 byte, the 4-byte big-endian schema id, and the
// protobuf message-index prefix) is handled entirely by franz-go's pkg/sr
// ConfluentHeader, so this package never reimplements the wire format. What
// this package adds is the JSON<->schema-body conversion for each schema type
// (Avro via twmb/avro, JSON Schema via a validator, Protobuf via dynamicpb)
// and the resolution/caching of schemas fetched from the registry.
package serde

import (
	"fmt"
	"strconv"

	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"
)

// confluentHeader encodes and decodes the Schema Registry wire header. It is
// stateless, so a single shared value is safe for concurrent use.
var confluentHeader sr.SerdeHeader = new(sr.ConfluentHeader)

// codec converts between user JSON and a schema's binary body (the bytes that
// follow the wire header). Implementations exist per schema type.
type codec interface {
	// encode converts user-provided JSON into the schema's binary body. index
	// is the protobuf message index selecting which message to encode (nil,
	// and ignored, for Avro and JSON Schema).
	encode(jsonIn []byte, index []int) (body []byte, err error)
	// decode converts a schema binary body into JSON. index is the protobuf
	// message index decoded from the wire (nil for Avro and JSON Schema).
	decode(body []byte, index []int) (jsonOut []byte, err error)
}

// refFetcher fetches a referenced schema by subject and version. It is used to
// transitively resolve a schema's references from the registry.
type refFetcher func(subject string, version int) (sr.Schema, error)

// buildCodec constructs a codec for a schema fetched from (or destined for)
// the registry. The Avro cache is shared so repeated schemas parse once.
//
// schema.References are resolved transitively via fetch: referenced schemas are
// fetched from the registry and fed to the parser (Avro named types, Protobuf
// imports, JSON Schema $refs) so a schema that references others can encode and
// decode. fetch may be nil only for schemas with no references.
func buildCodec(cache *avro.SchemaCache, schema sr.Schema, fetch refFetcher) (codec, error) {
	switch schema.Type {
	case sr.TypeAvro:
		return newAvroCodec(cache, schema.Schema, schema.References, fetch)
	case sr.TypeJSON:
		return newJSONCodec(schema.Schema, schema.References, fetch)
	case sr.TypeProtobuf:
		return newProtoCodec(schema.Schema, schema.References, fetch)
	default:
		return nil, fmt.Errorf("unsupported schema type %q", schema.Type)
	}
}

// refKey uniquely identifies a referenced schema for cycle detection.
func refKey(r sr.SchemaReference) string {
	return r.Subject + "@" + strconv.Itoa(r.Version)
}

// TopicSubject returns the subject for a topic under the default
// TopicNameStrategy: "<topic>-key" for keys, "<topic>-value" for values.
func TopicSubject(topic string, isKey bool) string {
	if isKey {
		return topic + "-key"
	}
	return topic + "-value"
}
