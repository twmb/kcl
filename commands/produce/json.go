package produce

import (
	"encoding/json/v2"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/out"
)

// jsonFormatName is the -o/--output-format value that selects JSON records,
// matched exactly the way consume matches it: -o 'json%t' is an ordinary
// format string.
const jsonFormatName = "json"

// producedRecord is the object -o json prints per record. On a failure the
// cluster assigned no offset or timestamp, and no partition unless the record
// was partitioned before it failed, so those are null.
type producedRecord struct {
	Topic     string `json:"topic"`
	Partition any    `json:"partition"`
	Offset    any    `json:"offset"`
	Timestamp any    `json:"timestamp"`
	Error     string `json:"error"`
}

func marshalProduced(r *kgo.Record, err error) []byte {
	doc := producedRecord{Topic: r.Topic, Partition: out.Unknown, Offset: out.Unknown, Timestamp: out.Unknown}
	if r.Partition >= 0 {
		doc.Partition = r.Partition
	}
	if err != nil {
		doc.Error = err.Error()
	} else {
		doc.Offset = r.Offset
		doc.Timestamp = r.Timestamp.UnixMilli()
	}
	// The struct cannot fail to marshal.
	b, _ := json.Marshal(doc)
	return append(b, '\n')
}
