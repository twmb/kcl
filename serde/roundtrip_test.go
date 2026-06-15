package serde_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/twmb/kcl/serde"
)

// jsonEqual compares two JSON documents for semantic equality (ignoring key
// order and insignificant whitespace).
func jsonEqual(t *testing.T, want, got []byte) {
	t.Helper()
	var wv, gv any
	if err := json.Unmarshal(want, &wv); err != nil {
		t.Fatalf("want is not valid JSON (%v): %s", err, want)
	}
	if err := json.Unmarshal(got, &gv); err != nil {
		t.Fatalf("got is not valid JSON (%v): %s", err, got)
	}
	if !reflect.DeepEqual(wv, gv) {
		t.Fatalf("JSON mismatch:\n want %s\n  got %s", want, got)
	}
}

func newClient(t *testing.T) (*sr.Client, *srfake.Registry) {
	t.Helper()
	reg := srfake.New()
	t.Cleanup(reg.Close)
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatalf("unable to create sr client: %v", err)
	}
	return cl, reg
}

// register registers a schema and returns its id.
func register(t *testing.T, cl *sr.Client, subject string, s sr.Schema) int {
	t.Helper()
	ss, err := cl.CreateSchema(context.Background(), subject, s)
	if err != nil {
		t.Fatalf("unable to register schema: %v", err)
	}
	return ss.ID
}

func roundTrip(t *testing.T, cl *sr.Client, id int, in string) []byte {
	t.Helper()
	enc, err := serde.NewEncoder(cl, "", false, serde.Spec{ID: id})
	if err != nil {
		t.Fatalf("unable to build encoder: %v", err)
	}
	wire, err := enc.Encode(nil, []byte(in))
	if err != nil {
		t.Fatalf("unable to encode: %v", err)
	}
	// The wire bytes must start with the magic 0 byte and the 4-byte id.
	if len(wire) < 5 || wire[0] != 0 {
		t.Fatalf("encoded bytes missing SR header: %x", wire)
	}
	dec := serde.NewDecoder(cl)
	out, err := dec.Decode(wire)
	if err != nil {
		t.Fatalf("unable to decode: %v", err)
	}
	return out
}

func TestAvroRoundTrip(t *testing.T) {
	cl, _ := newClient(t)
	id := register(t, cl, "avro-value", sr.Schema{
		Type:   sr.TypeAvro,
		Schema: `{"type":"record","name":"User","fields":[{"name":"id","type":"string"},{"name":"age","type":"int"}]}`,
	})
	const in = `{"id":"a","age":7}`
	out := roundTrip(t, cl, id, in)
	jsonEqual(t, []byte(in), out)
}

func TestJSONSchemaRoundTrip(t *testing.T) {
	cl, _ := newClient(t)
	id := register(t, cl, "json-value", sr.Schema{
		Type:   sr.TypeJSON,
		Schema: `{"type":"object","properties":{"id":{"type":"string"},"n":{"type":"integer"}},"required":["id","n"]}`,
	})
	const in = `{"id":"a","n":7}`
	out := roundTrip(t, cl, id, in)
	jsonEqual(t, []byte(in), out)
}

func TestJSONSchemaValidationRejects(t *testing.T) {
	cl, _ := newClient(t)
	id := register(t, cl, "json-value", sr.Schema{
		Type:   sr.TypeJSON,
		Schema: `{"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]}`,
	})
	enc, err := serde.NewEncoder(cl, "", false, serde.Spec{ID: id})
	if err != nil {
		t.Fatalf("unable to build encoder: %v", err)
	}
	// "n" should be an integer; a string must fail validation.
	if _, err := enc.Encode(nil, []byte(`{"n":"notint"}`)); err == nil {
		t.Fatal("expected validation error for non-integer n, got nil")
	}
}

func TestProtobufRoundTrip(t *testing.T) {
	cl, _ := newClient(t)
	id := register(t, cl, "proto-value", sr.Schema{
		Type:   sr.TypeProtobuf,
		Schema: "syntax = \"proto3\";\nmessage User {\n  string id = 1;\n  int32 age = 2;\n}\n",
	})
	const in = `{"id":"a","age":7}`
	out := roundTrip(t, cl, id, in)
	jsonEqual(t, []byte(in), out)
}

func TestAvroReferences(t *testing.T) {
	cl, _ := newClient(t)
	// Register the referenced type first.
	register(t, cl, "address-value", sr.Schema{
		Type:   sr.TypeAvro,
		Schema: `{"type":"record","name":"Address","namespace":"com.ex","fields":[{"name":"street","type":"string"}]}`,
	})
	// Register a schema that references it by name.
	id := register(t, cl, "person-value", sr.Schema{
		Type:       sr.TypeAvro,
		Schema:     `{"type":"record","name":"Person","namespace":"com.ex","fields":[{"name":"name","type":"string"},{"name":"addr","type":"com.ex.Address"}]}`,
		References: []sr.SchemaReference{{Name: "com.ex.Address", Subject: "address-value", Version: 1}},
	})
	const in = `{"name":"a","addr":{"street":"s"}}`
	out := roundTrip(t, cl, id, in)
	jsonEqual(t, []byte(in), out)
}

func TestProtobufReferences(t *testing.T) {
	cl, _ := newClient(t)
	register(t, cl, "common-value", sr.Schema{
		Type:   sr.TypeProtobuf,
		Schema: "syntax = \"proto3\";\npackage common;\nmessage Addr {\n  string street = 1;\n}\n",
	})
	id := register(t, cl, "person-value", sr.Schema{
		Type:       sr.TypeProtobuf,
		Schema:     "syntax = \"proto3\";\nimport \"common.proto\";\nmessage Person {\n  string name = 1;\n  common.Addr addr = 2;\n}\n",
		References: []sr.SchemaReference{{Name: "common.proto", Subject: "common-value", Version: 1}},
	})
	const in = `{"name":"a","addr":{"street":"s"}}`
	out := roundTrip(t, cl, id, in)
	jsonEqual(t, []byte(in), out)
}

func TestDecodeNonSRBytesReturnsBadHeader(t *testing.T) {
	cl, _ := newClient(t)
	dec := serde.NewDecoder(cl)
	if _, err := dec.Decode([]byte("not sr framed")); err == nil {
		t.Fatal("expected an error decoding non-SR bytes")
	}
}

func TestProtobufMessageByName(t *testing.T) {
	cl, _ := newClient(t)
	id := register(t, cl, "multi-value", sr.Schema{
		Type:   sr.TypeProtobuf,
		Schema: "syntax = \"proto3\";\nmessage A {\n  string a = 1;\n}\nmessage B {\n  string b = 1;\n}\n",
	})
	// Select the second message (B) by name; its index must be encoded into the
	// wire header and decode back to B.
	enc, err := serde.NewEncoder(cl, "", false, serde.Spec{ID: id, Message: "B"})
	if err != nil {
		t.Fatalf("NewEncoder(message): %v", err)
	}
	const in = `{"b":"hi"}`
	wire, err := enc.Encode(nil, []byte(in))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := serde.NewDecoder(cl).Decode(wire)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	jsonEqual(t, []byte(in), out)
}

func TestEncodeInvalidInputJSON(t *testing.T) {
	cl, _ := newClient(t)
	id := register(t, cl, "avro-value", sr.Schema{
		Type:   sr.TypeAvro,
		Schema: `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`,
	})
	enc, err := serde.NewEncoder(cl, "", false, serde.Spec{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.Encode(nil, []byte(`not json`)); err == nil {
		t.Fatal("expected error encoding invalid JSON input")
	}
}

func TestEncoderRejectsMultipleResolutions(t *testing.T) {
	cl, _ := newClient(t)
	_, err := serde.NewEncoder(cl, "t", false, serde.Spec{ID: 5, Subject: "foo-value"})
	if err == nil {
		t.Fatal("expected error when both id and subject are given")
	}
}

func TestEncoderVersionWithoutSubjectOrTopic(t *testing.T) {
	cl, _ := newClient(t)
	// No topic, no subject, no id/file, just a version -> clear error.
	_, err := serde.NewEncoder(cl, "", false, serde.Spec{Version: "3"})
	if err == nil {
		t.Fatal("expected error: version with no subject/topic")
	}
}

func TestTopicSubject(t *testing.T) {
	if got := serde.TopicSubject("foo", false); got != "foo-value" {
		t.Errorf("value subject = %q, want foo-value", got)
	}
	if got := serde.TopicSubject("foo", true); got != "foo-key" {
		t.Errorf("key subject = %q, want foo-key", got)
	}
}

func TestSpecDerivesSubject(t *testing.T) {
	if !(serde.Spec{Topic: true}).DerivesSubject() {
		t.Error("topic-strategy spec should derive subject")
	}
	if (serde.Spec{ID: 1}).DerivesSubject() {
		t.Error("id spec should not derive subject")
	}
	if (serde.Spec{Subject: "x"}).DerivesSubject() {
		t.Error("explicit-subject spec should not derive subject")
	}
}

func TestEncodeBySubjectVersion(t *testing.T) {
	cl, _ := newClient(t)
	register(t, cl, "sub-value", sr.Schema{
		Type:   sr.TypeAvro,
		Schema: `{"type":"record","name":"R","fields":[{"name":"id","type":"string"}]}`,
	})
	// Resolve via the topic strategy (topic "sub" -> subject "sub-value").
	enc, err := serde.NewEncoder(cl, "sub", false, serde.Spec{Topic: true, Version: "latest"})
	if err != nil {
		t.Fatalf("unable to build encoder by subject: %v", err)
	}
	const in = `{"id":"x"}`
	wire, err := enc.Encode(nil, []byte(in))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec := serde.NewDecoder(cl)
	out, err := dec.Decode(wire)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	jsonEqual(t, []byte(in), out)
}
