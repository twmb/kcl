package serde

import (
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"

	"github.com/twmb/franz-go/pkg/sr"
)

// protoCodec converts between JSON and Protobuf binary using a descriptor
// compiled from the registry-stored .proto source. The Schema Registry wire
// format prefixes the body with a message index identifying which message in
// the file the payload uses; that index is handled by the caller via the
// pkg/sr ConfluentHeader and threaded into encode/decode here.
//
// Referenced schemas (imported .proto files) are resolved from the registry:
// each reference's name is the import path used in an `import "...";` statement,
// and its content is fetched and supplied to the parser.
type protoCodec struct {
	fd *desc.FileDescriptor
}

const protoFilename = "schema.proto"

func newProtoCodec(schemaText string, refs []sr.SchemaReference, fetch refFetcher) (*protoCodec, error) {
	files := map[string]string{protoFilename: schemaText}
	if err := collectProtoRefs(files, refs, fetch); err != nil {
		return nil, err
	}
	p := protoparse.Parser{
		Accessor: protoparse.FileContentsFromMap(files),
	}
	fds, err := p.ParseFiles(protoFilename)
	if err != nil {
		return nil, fmt.Errorf("unable to parse protobuf schema: %w", err)
	}
	return &protoCodec{fd: fds[0]}, nil
}

// collectProtoRefs fetches referenced .proto files (transitively) into files,
// keyed by their import path (the reference name).
func collectProtoRefs(files map[string]string, refs []sr.SchemaReference, fetch refFetcher) error {
	for _, ref := range refs {
		if _, ok := files[ref.Name]; ok {
			continue
		}
		if fetch == nil {
			return fmt.Errorf("protobuf schema imports %q but no registry is available to resolve it", ref.Name)
		}
		rs, err := fetch(ref.Subject, ref.Version)
		if err != nil {
			return fmt.Errorf("unable to fetch referenced schema %q (%s): %w", ref.Name, refKey(ref), err)
		}
		files[ref.Name] = rs.Schema
		if err := collectProtoRefs(files, rs.References, fetch); err != nil {
			return err
		}
	}
	return nil
}

// messageAt resolves the message descriptor for a Schema Registry message
// index. The index is a path: the first element selects a top-level message,
// each subsequent element selects a nested message. An empty index selects the
// first top-level message (the wire shortcut for the common single-message
// case).
func (c *protoCodec) messageAt(index []int) (*desc.MessageDescriptor, error) {
	msgs := c.fd.GetMessageTypes()
	if len(index) == 0 {
		index = []int{0}
	}
	var md *desc.MessageDescriptor
	for _, i := range index {
		if i < 0 || i >= len(msgs) {
			return nil, fmt.Errorf("protobuf message index %v is out of range for the schema", index)
		}
		md = msgs[i]
		msgs = md.GetNestedMessageTypes()
	}
	return md, nil
}

func (c *protoCodec) encode(jsonIn []byte, index []int) ([]byte, error) {
	md, err := c.messageAt(index)
	if err != nil {
		return nil, err
	}
	msg := dynamic.NewMessage(md)
	if err := msg.UnmarshalJSON(jsonIn); err != nil {
		return nil, fmt.Errorf("input does not match protobuf message %s: %w", md.GetName(), err)
	}
	body, err := msg.Marshal()
	if err != nil {
		return nil, fmt.Errorf("unable to encode protobuf binary: %w", err)
	}
	return body, nil
}

func (c *protoCodec) decode(body []byte, index []int) ([]byte, error) {
	md, err := c.messageAt(index)
	if err != nil {
		return nil, err
	}
	msg := dynamic.NewMessage(md)
	if err := msg.Unmarshal(body); err != nil {
		return nil, fmt.Errorf("unable to decode protobuf binary: %w", err)
	}
	jsonOut, err := msg.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("unable to encode protobuf value as JSON: %w", err)
	}
	return jsonOut, nil
}

// messageIndexByName finds the message-index path for a fully-qualified or
// simple message name, used when a user selects a specific protobuf message to
// encode by name rather than by index.
func (c *protoCodec) messageIndexByName(name string) ([]int, error) {
	var walk func(msgs []*desc.MessageDescriptor, prefix []int) []int
	walk = func(msgs []*desc.MessageDescriptor, prefix []int) []int {
		for i, md := range msgs {
			idx := append(append([]int(nil), prefix...), i)
			if md.GetName() == name || md.GetFullyQualifiedName() == name {
				return idx
			}
			if found := walk(md.GetNestedMessageTypes(), idx); found != nil {
				return found
			}
		}
		return nil
	}
	if idx := walk(c.fd.GetMessageTypes(), nil); idx != nil {
		return idx, nil
	}
	return nil, fmt.Errorf("protobuf message %q not found in schema", name)
}
