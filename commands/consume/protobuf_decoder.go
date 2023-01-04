package consume

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

type pbDecoder struct {
	md *desc.MessageDescriptor
}

// construct a pbDecoder
// protoFile support the following formats:
//  1. Proto Source Files. Location of imported protos will be inferred based on the "import" statement in the protoFile.
//  2. Protoset Files. Protoset files contain binary encoded google.protobuf.FileDescriptorSet protos, example to create
//     a protoset file: `protoc --proto_path=. --descriptor_set_out=sample.protoset --include_imports sample.proto`.
//     The "include_imports" include all dependencies of the input file in the set, so that the set is self-contained.
func newPBDecoder(protoFile string, messageName string) (pbs *pbDecoder, err error) {
	if _, err = os.Stat(protoFile); err != nil {
		return nil, fmt.Errorf("file %s not found", protoFile)
	}

	ext := filepath.Ext(protoFile)
	var md *desc.MessageDescriptor
	if strings.Compare(ext, ".proto") == 0 {
		p := &protoparse.Parser{
			ImportPaths:      []string{path.Dir(protoFile)},
			InferImportPaths: true,
		}
		var fds []*desc.FileDescriptor
		if fds, err = p.ParseFiles(path.Base(protoFile)); err != nil {
			return nil, fmt.Errorf("failed to parse proto file: %v", err)
		}
		md = fds[0].FindMessage(messageName)
	} else if strings.Compare(ext, ".protoset") == 0 {
		var b []byte
		if b, err = os.ReadFile(protoFile); err != nil {
			return nil, fmt.Errorf("failed to read protoset file: %v", err)
		}
		fds := &descriptorpb.FileDescriptorSet{}
		if err := proto.Unmarshal(b, fds); err != nil {
			return nil, fmt.Errorf("failed to parse protoset file: %v", err)
		}
		var fd *desc.FileDescriptor
		if fd, err = desc.CreateFileDescriptorFromSet(fds); err != nil {
			return nil, fmt.Errorf("failed to create fd: %v", err)
		}
		md = fd.FindMessage(messageName)
	} else {
		return nil, fmt.Errorf("unsupported file type: %v", protoFile)
	}

	if md == nil {
		return nil, fmt.Errorf("proto-message %s not found, make sure it is in 'package.message' format", messageName)
	}
	return &pbDecoder{md: md}, nil
}

func (c *pbDecoder) jsonString(pbData []byte) (bytes []byte, err error) {
	dyMsg := dynamic.NewMessage(c.md)
	if err = dyMsg.Unmarshal(pbData); err != nil {
		return nil, err
	}
	if bytes, err = dyMsg.MarshalJSON(); err != nil {
		return nil, err
	}

	return bytes, nil
}
