package consume

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
)

// jsonFormatName is the reserved -f/--format value that selects JSON record
// output. It is matched exactly: any other value -- including one that merely
// contains "json", such as -f 'json%v' -- is handed to kgo.NewRecordFormatter
// as an ordinary format string.
const jsonFormatName = "json"

// jsonHeader is one record header. The key is a string on the wire; the value
// is bytes and so follows the same UTF-8 rule as the record key and value.
type jsonHeader struct {
	Key         string          `json:"key"`
	Value       json.RawMessage `json:"value,omitempty"`
	ValueBase64 string          `json:"value_base64,omitempty"`
}

// jsonRecord is the wire shape of one record in JSON output mode.
//
// Field names match rpk's envelope where the two overlap, so jq expressions
// written against rpk keep working.
//
// Key/Value are pre-encoded json.RawMessage rather than string so that three
// states are representable in one field: JSON null for a nil (tombstone or
// absent) component, a JSON string for text, and a bare JSON value for a
// component that --decode turned into JSON. When the bytes are not valid
// UTF-8, the plain field is omitted entirely and the _base64 variant carries
// the payload instead -- see encodeComponent.
type jsonRecord struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	Timestamp   int64  `json:"timestamp"` // millis
	LeaderEpoch int32  `json:"leader_epoch"`

	Key       json.RawMessage `json:"key,omitempty"`
	KeyBase64 string          `json:"key_base64,omitempty"`

	Value       json.RawMessage `json:"value,omitempty"`
	ValueBase64 string          `json:"value_base64,omitempty"`

	Headers []jsonHeader `json:"headers,omitempty"`

	// DeliveryCount is only meaningful when share consuming, so it is
	// omitted otherwise rather than reported as a misleading zero.
	DeliveryCount *int32 `json:"delivery_count,omitempty"`
}

var jsonNull = json.RawMessage("null")

// encodeComponent renders one byte component (a key, a value, or a header
// value) into either a JSON value or a base64 string, returning exactly one of
// the two.
//
// The rules, in order:
//
//   - nil is JSON null. A nil component and an empty one are distinct on the
//     wire -- the record-batch decode preserves it -- so they stay distinct
//     here: nil is null, empty is "".
//   - If decoded is true, the bytes came back from a Schema Registry decode
//     and are already JSON text, so they are embedded as a JSON value. This
//     is what makes `jq .value.count` work rather than requiring fromjson.
//     A decode that failed leaves the original bytes behind, so validity is
//     re-checked rather than assumed.
//   - Valid UTF-8 becomes a JSON string.
//   - Anything else is base64. Coercing arbitrary bytes through a Go string
//     into json.Marshal would replace invalid UTF-8 with U+FFFD, silently
//     corrupting exactly the binary payloads this tool exists to inspect.
func encodeComponent(b []byte, decoded bool) (raw json.RawMessage, b64 string) {
	if b == nil {
		return jsonNull, ""
	}
	if decoded && json.Valid(b) {
		return json.RawMessage(b), ""
	}
	if utf8.Valid(b) {
		q, err := json.Marshal(string(b))
		if err == nil { // marshaling a valid UTF-8 string cannot fail
			return q, ""
		}
	}
	return nil, base64.StdEncoding.EncodeToString(b)
}

// buildJSONFormatFn installs a format function that writes one JSON object per
// record, newline delimited.
func (co *consumeOutput) buildJSONFormatFn(shareGroup bool) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false) // record payloads are data, not HTML
	co.format = func(r *kgo.Record, _ *kgo.FetchPartition) {
		j := jsonRecord{
			Topic:       r.Topic,
			Partition:   r.Partition,
			Offset:      r.Offset,
			Timestamp:   r.Timestamp.UnixMilli(),
			LeaderEpoch: r.LeaderEpoch,
		}
		j.Key, j.KeyBase64 = encodeComponent(r.Key, co.decodeKey)
		j.Value, j.ValueBase64 = encodeComponent(r.Value, co.decodeValue)
		if len(r.Headers) > 0 {
			j.Headers = make([]jsonHeader, 0, len(r.Headers))
			for _, h := range r.Headers {
				jh := jsonHeader{Key: h.Key}
				jh.Value, jh.ValueBase64 = encodeComponent(h.Value, false)
				j.Headers = append(j.Headers, jh)
			}
		}
		if shareGroup {
			dc := r.DeliveryCount()
			j.DeliveryCount = &dc
		}
		// Encode writes a trailing newline itself. The type is built here
		// and cannot fail to marshal.
		enc.Encode(&j)
	}
}
