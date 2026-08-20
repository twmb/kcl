package serde

import (
	"context"
	"fmt"
	"sync"

	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"
)

// Decoder converts Schema Registry wire-format bytes into JSON. It fetches and
// caches schemas by id from the registry as it encounters them, so a single
// Decoder can decode a stream of records referencing many schemas.
type Decoder struct {
	cl    *sr.Client
	fetch refFetcher
	mu    sync.Mutex
	avro  avro.SchemaCache
	byID  map[int]*decEntry
}

type decEntry struct {
	typ   sr.SchemaType
	codec codec
}

// NewDecoder returns a Decoder that resolves schemas via cl.
func NewDecoder(cl *sr.Client) *Decoder {
	return &Decoder{
		cl:    cl,
		fetch: clientRefFetcher(cl),
		byID:  make(map[int]*decEntry),
	}
}

// clientRefFetcher returns a refFetcher backed by an sr.Client.
func clientRefFetcher(cl *sr.Client) refFetcher {
	return func(subject string, version int) (sr.Schema, error) {
		ss, err := cl.SchemaByVersion(context.Background(), subject, version)
		return ss.Schema, err
	}
}

// Decode converts one Schema Registry wire-format value into JSON.
//
// If the bytes are not SR-framed (missing magic byte or too short), it returns
// an error wrapping sr.ErrBadHeader, which callers can detect to fall back to
// printing the raw bytes.
func (d *Decoder) Decode(wire []byte) ([]byte, error) {
	id, rest, err := confluentHeader.DecodeID(wire)
	if err != nil {
		return nil, err // wraps sr.ErrBadHeader for non-SR input
	}
	e, err := d.entryFor(id)
	if err != nil {
		return nil, err
	}
	var index []int
	if e.typ == sr.TypeProtobuf {
		index, rest, err = confluentHeader.DecodeIndex(rest, 0)
		if err != nil {
			return nil, fmt.Errorf("unable to decode protobuf message index: %w", err)
		}
	}
	return e.codec.decode(rest, index)
}

func (d *Decoder) entryFor(id int) (*decEntry, error) {
	d.mu.Lock()
	e, ok := d.byID[id]
	d.mu.Unlock()
	if ok {
		return e, nil
	}

	// Fetch the schema and build the codec without holding the lock, so a
	// concurrent decode of a different id is not blocked on network I/O (the
	// fetch can recurse into transitive reference fetches). This is safe:
	// avro.SchemaCache is itself concurrency-safe, and the reference fetcher
	// talks to the sr client directly rather than re-entering the Decoder.
	schema, err := d.cl.SchemaByID(context.Background(), id)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch schema id %d from registry: %w", id, err)
	}
	c, err := buildCodec(&d.avro, schema, d.fetch)
	if err != nil {
		return nil, fmt.Errorf("schema id %d: %w", id, err)
	}
	e = &decEntry{typ: schema.Type, codec: c}

	// Cache it, deferring to a concurrent insert of the same id.
	d.mu.Lock()
	if existing, ok := d.byID[id]; ok {
		e = existing
	} else {
		d.byID[id] = e
	}
	d.mu.Unlock()
	return e, nil
}
