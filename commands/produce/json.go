package produce

import (
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/out"
)

// jsonFormatName is the -f/--format and -o/--output-format value that selects
// JSON records, matched exactly the way consume matches it: -f 'json%v' is an
// ordinary format string.
const jsonFormatName = "json"

// jsonRecord is the object "kcl consume -f json" writes, read back. Offset,
// leader epoch, and delivery count are accepted so a consume line round
// trips, and ignored: the cluster assigns them.
//
// Key, value, and header values are raw so that three inputs stay distinct:
// absent or null is a nil component, a string is its bytes, and any other
// JSON value (what consume --decode embeds) is its compact text. Bytes that
// are not UTF-8 arrive in the _base64 field instead, as consume writes them.
type jsonRecord struct {
	Topic         string         `json:"topic"`
	Partition     *int32         `json:"partition"`
	Offset        int64          `json:"offset"`
	Timestamp     *int64         `json:"timestamp"` // millis
	LeaderEpoch   int32          `json:"leader_epoch"`
	Key           jsontext.Value `json:"key"`
	KeyBase64     string         `json:"key_base64"`
	Value         jsontext.Value `json:"value"`
	ValueBase64   string         `json:"value_base64"`
	Headers       []jsonHeader   `json:"headers"`
	DeliveryCount int32          `json:"delivery_count"`
}

type jsonHeader struct {
	Key         string         `json:"key"`
	Value       jsontext.Value `json:"value"`
	ValueBase64 string         `json:"value_base64"`
}

// jsonReader reads one record per JSON object from stdin. Objects may be one
// per line or run together; the decoder finds the boundaries.
type jsonReader struct {
	dec *jsontext.Decoder
	n   int // objects read, to name the bad one
}

func newJSONReader(r io.Reader) *jsonReader {
	return &jsonReader{dec: jsontext.NewDecoder(r)}
}

// ReadRecord returns the next record, or io.EOF once the input is done. A
// misspelled field is an error rather than a silent drop.
func (j *jsonReader) ReadRecord() (*kgo.Record, error) {
	var in jsonRecord
	err := json.UnmarshalDecode(j.dec, &in, json.RejectUnknownMembers(true))
	if err == io.EOF {
		return nil, io.EOF
	}
	j.n++
	if err != nil {
		return nil, fmt.Errorf("record %d: %s", j.n, jsonErrText(err))
	}

	r := &kgo.Record{Topic: in.Topic, Partition: -1}
	if in.Partition != nil {
		r.Partition = *in.Partition
	}
	if in.Timestamp != nil {
		r.Timestamp = time.UnixMilli(*in.Timestamp)
	}
	if r.Key, err = component("key", in.Key, in.KeyBase64); err != nil {
		return nil, fmt.Errorf("record %d: %v", j.n, err)
	}
	if r.Value, err = component("value", in.Value, in.ValueBase64); err != nil {
		return nil, fmt.Errorf("record %d: %v", j.n, err)
	}
	for i, h := range in.Headers {
		v, err := component("value", h.Value, h.ValueBase64)
		if err != nil {
			return nil, fmt.Errorf("record %d: header %d: %v", j.n, i, err)
		}
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: h.Key, Value: v})
	}
	return r, nil
}

// jsonErrText says what was wrong with an object in the input's terms, where
// the decoder's own text names our Go types.
func jsonErrText(err error) string {
	var se *json.SemanticError
	if errors.As(err, &se) {
		switch {
		case errors.Is(se.Err, json.ErrUnknownName):
			return fmt.Sprintf("unknown field %q", se.JSONPointer)
		case se.JSONPointer == "":
			return fmt.Sprintf("want an object, got a JSON %s", kindName(se.JSONKind))
		default:
			return fmt.Sprintf("field %q: got a JSON %s, want %s", se.JSONPointer, kindName(se.JSONKind), goName(se.GoType))
		}
	}
	var sy *jsontext.SyntacticError
	if errors.As(err, &sy) && sy.Err != nil {
		return fmt.Sprintf("%v (input byte %d)", sy.Err, sy.ByteOffset)
	}
	return err.Error()
}

func kindName(k jsontext.Kind) string {
	switch k {
	case '{':
		return "object"
	case '[':
		return "array"
	}
	return k.String()
}

func goName(t reflect.Type) string {
	if t == nil {
		return "another type"
	}
	switch t.Kind() {
	case reflect.String:
		return "a string"
	case reflect.Int32, reflect.Int64:
		return "a number"
	case reflect.Slice:
		return "an array"
	case reflect.Struct:
		return "an object"
	}
	return "a " + t.String()
}

// component turns a key, value, or header value back into bytes. The plain
// field and its _base64 twin are exclusive, since consume writes one or the
// other; an empty _base64 counts as absent, the way consume omits it.
func component(name string, raw jsontext.Value, b64 string) ([]byte, error) {
	if b64 != "" {
		if len(raw) > 0 && raw.Kind() != 'n' {
			return nil, fmt.Errorf("both %s and %s_base64 are set", name, name)
		}
		b, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("%s_base64: %v", name, err)
		}
		return b, nil
	}
	switch raw.Kind() {
	case jsontext.KindInvalid, 'n': // absent, null
		return nil, nil
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("%s: %v", name, err)
		}
		return []byte(s), nil
	}
	v := raw.Clone()
	if err := v.Compact(); err != nil {
		return nil, fmt.Errorf("%s: %v", name, err)
	}
	return v, nil
}

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

// jsonPartitioner sends a record whose JSON object named a partition there,
// and hands the rest to kgo's default partitioner, so a consume dump replays
// onto the same partitions while a hand-written object without one is
// placed the way any other record is.
type jsonPartitioner struct{ def kgo.Partitioner }

func newJSONPartitioner() kgo.Partitioner {
	return jsonPartitioner{def: kgo.UniformBytesPartitioner(64<<10, true, true, nil)}
}

func (p jsonPartitioner) ForTopic(t string) kgo.TopicPartitioner {
	return &jsonTopicPartitioner{def: p.def.ForTopic(t)}
}

type jsonTopicPartitioner struct{ def kgo.TopicPartitioner }

func (p *jsonTopicPartitioner) RequiresConsistency(r *kgo.Record) bool {
	return r.Partition >= 0 || p.def.RequiresConsistency(r)
}

func (p *jsonTopicPartitioner) Partition(r *kgo.Record, n int) int {
	if r.Partition >= 0 {
		return int(r.Partition)
	}
	return p.def.Partition(r, n)
}

// PartitionByBackup and OnNewBatch are the optional interfaces the default
// partitioner implements; kgo asks for them by type assertion, so we forward
// them.
func (p *jsonTopicPartitioner) PartitionByBackup(r *kgo.Record, n int, backup kgo.TopicBackupIter) int {
	if r.Partition >= 0 {
		return int(r.Partition)
	}
	if b, ok := p.def.(kgo.TopicBackupPartitioner); ok {
		return b.PartitionByBackup(r, n, backup)
	}
	return p.def.Partition(r, n)
}

func (p *jsonTopicPartitioner) OnNewBatch() {
	if o, ok := p.def.(kgo.TopicPartitionerOnNewBatch); ok {
		o.OnNewBatch()
	}
}
