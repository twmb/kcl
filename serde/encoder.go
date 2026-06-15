package serde

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"
)

// Spec describes how to resolve an existing schema used to encode a value or
// key. Exactly one of ID, Subject, or Topic selects the resolution path;
// producing never registers schemas (use "kcl registry schema create" for
// that).
type Spec struct {
	ID      int    // >0: encode with this exact schema id
	Subject string // explicit subject
	Topic   bool   // derive the subject from the produce topic (TopicNameStrategy)
	Version string // version for subject/topic resolution (number or "latest")
	Message string // protobuf: message name to encode (optional)
}

// Active reports whether the spec requests schema-registry encoding.
func (s Spec) Active() bool {
	return s.ID > 0 || s.Subject != "" || s.Topic
}

// DerivesSubject reports whether the subject is derived from the produce topic
// (TopicNameStrategy). Such specs encode every record against the one subject
// derived from the produce topic, so they are unsafe when the input carries a
// per-record topic (a %t verb).
func (s Spec) DerivesSubject() bool {
	return s.Topic
}

// Encoder encodes user JSON into the Schema Registry wire format for a single
// resolved schema.
type Encoder struct {
	id    int
	index []int // protobuf message index, nil for avro/json
	codec codec
}

// NewEncoder resolves a schema per spec and returns an Encoder. topic is used
// to derive the default subject (TopicNameStrategy) when no explicit subject
// is given; isKey selects "-key" vs "-value".
func NewEncoder(cl *sr.Client, topic string, isKey bool, spec Spec) (*Encoder, error) {
	ctx := context.Background()
	var cache avro.SchemaCache

	// Exactly one resolution path: id, explicit subject, or topic strategy.
	set := 0
	for _, on := range []bool{spec.ID > 0, spec.Subject != "", spec.Topic} {
		if on {
			set++
		}
	}
	switch {
	case set == 0:
		return nil, errors.New("no schema resolution specified")
	case set > 1:
		return nil, errors.New("specify only one of id, subject, or topic")
	}

	var (
		schema sr.Schema
		id     int
	)
	switch {
	case spec.ID > 0:
		id = spec.ID
		s, err := cl.SchemaByID(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("unable to fetch schema id %d: %w", id, err)
		}
		schema = s

	default:
		subject := spec.Subject
		if spec.Topic {
			if topic == "" {
				return nil, errors.New("the topic schema strategy requires a produce topic")
			}
			subject = TopicSubject(topic, isKey)
		}
		version, err := parseVersion(spec.Version)
		if err != nil {
			return nil, err
		}
		ss, err := cl.SchemaByVersion(ctx, subject, version)
		if err != nil {
			return nil, fmt.Errorf("unable to fetch schema for subject %q version %s: %w", subject, spec.Version, err)
		}
		id, schema = ss.ID, ss.Schema
	}

	c, err := buildCodec(&cache, schema, clientRefFetcher(cl))
	if err != nil {
		return nil, err
	}

	var index []int
	if schema.Type == sr.TypeProtobuf {
		pc := c.(*protoCodec)
		if spec.Message != "" {
			index, err = pc.messageIndexByName(spec.Message)
			if err != nil {
				return nil, err
			}
		} else {
			index = []int{0}
		}
	}

	return &Encoder{id: id, index: index, codec: c}, nil
}

// Encode converts jsonIn into wire-format bytes appended to dst.
func (e *Encoder) Encode(dst, jsonIn []byte) ([]byte, error) {
	body, err := e.codec.encode(jsonIn, e.index)
	if err != nil {
		return nil, err
	}
	out, err := confluentHeader.AppendEncode(dst, e.id, e.index)
	if err != nil {
		return nil, err
	}
	return append(out, body...), nil
}

// ID returns the resolved schema id used for encoding.
func (e *Encoder) ID() int { return e.id }

func parseVersion(s string) (int, error) {
	if s == "" || strings.EqualFold(s, "latest") {
		return -1, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 1 {
		return 0, fmt.Errorf("invalid version %q: must be a positive integer or 'latest'", s)
	}
	return v, nil
}
