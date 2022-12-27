package consume

import (
	"fmt"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
)

type PBDecoder struct {
	message string
	md      *desc.MessageDescriptor
}

func NewPBDecoder(protoFile string, messageName string) (pbs *PBDecoder, err error) {
	if _, err = os.Stat(protoFile); err != nil {
		return nil, fmt.Errorf("file %s not found", protoFile)
	}

	var p protoparse.Parser
	var fds []*desc.FileDescriptor
	if fds, err = p.ParseFiles(protoFile); err != nil {
		return nil, fmt.Errorf("failed to parse given files: %v", err)
	}

	md := fds[0].FindMessage(messageName)
	if md == nil {
		return nil, fmt.Errorf("proto-message %s not found, make sure it is in 'package.message' format", messageName)
	}
	return &PBDecoder{md: md, message: messageName}, nil
}

func (c *PBDecoder) JsonString(pbData []byte) (bytes []byte, err error) {
	dyMsg := dynamic.NewMessage(c.md)
	if err = dyMsg.Unmarshal(pbData); err != nil {
		return nil, err
	}
	if bytes, err = dyMsg.MarshalJSONPB(&jsonpb.Marshaler{Indent: "\t"}); err != nil {
		return nil, err
	}

	return bytes, nil
}
