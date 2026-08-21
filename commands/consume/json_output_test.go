package consume

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// captureJSON runs the JSON format function over the given records and returns
// one decoded object per record.
func captureJSON(t *testing.T, shareGroup bool, co *consumeOutput, recs ...*kgo.Record) []map[string]any {
	t.Helper()

	// buildJSONFormatFn binds os.Stdout when it is called, not when the
	// format function runs, so the pipe has to be in place first.
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	co.buildJSONFormatFn(shareGroup)

	for _, rec := range recs {
		co.format(rec, nil)
	}
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	r.Close()

	var out []map[string]any
	dec := json.NewDecoder(bytes.NewReader(b))
	for dec.More() {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			t.Fatalf("output is not a stream of JSON objects (%v): %s", err, b)
		}
		out = append(out, m)
	}
	if len(out) != len(recs) {
		t.Fatalf("got %d objects for %d records: %s", len(out), len(recs), b)
	}
	return out
}

func rec(topic string, key, value []byte) *kgo.Record {
	return &kgo.Record{
		Topic:       topic,
		Partition:   3,
		Offset:      1482,
		Timestamp:   time.UnixMilli(1755645291123),
		LeaderEpoch: 7,
		Key:         key,
		Value:       value,
	}
}

func TestJSONOutputShape(t *testing.T) {
	co := new(consumeOutput)
	got := captureJSON(t, false, co, rec("orders", []byte("user-1"), []byte("hello")))[0]

	for k, want := range map[string]any{
		"topic":        "orders",
		"partition":    float64(3),
		"offset":       float64(1482),
		"timestamp":    float64(1755645291123),
		"leader_epoch": float64(7),
		"key":          "user-1",
		"value":        "hello",
	} {
		if got[k] != want {
			t.Errorf("%s = %#v, want %#v", k, got[k], want)
		}
	}
	// Not a share consume, so the delivery count would be a misleading zero.
	if _, ok := got["delivery_count"]; ok {
		t.Errorf("delivery_count present without --share-group: %v", got)
	}
	// Headers are omitted rather than emitted as an empty array.
	if _, ok := got["headers"]; ok {
		t.Errorf("headers present with no headers: %v", got)
	}
}

// TestJSONOutputNilAndBinary is the output-level counterpart to
// TestEncodeComponent: a nil component must stay distinguishable from an empty
// one, and non-UTF-8 bytes must move to the _base64 field with the plain field
// gone, so a consumer can tell which it got.
func TestJSONOutputNilAndBinary(t *testing.T) {
	co := new(consumeOutput)
	got := captureJSON(t, false, co,
		rec("t", nil, []byte{}),                    // nil key, empty value
		rec("t", []byte{0xff, 0xfe}, []byte("ok")), // binary key
	)

	if v, ok := got[0]["key"]; !ok || v != nil {
		t.Errorf("nil key should be JSON null, got %#v (present=%v)", v, ok)
	}
	if got[0]["value"] != "" {
		t.Errorf("empty value should be \"\", got %#v", got[0]["value"])
	}

	if _, ok := got[1]["key"]; ok {
		t.Errorf("binary key should omit the plain field, got %v", got[1])
	}
	if got[1]["key_base64"] != "//4=" {
		t.Errorf("key_base64 = %#v, want %q", got[1]["key_base64"], "//4=")
	}
	if got[1]["value"] != "ok" {
		t.Errorf("value = %#v, want %q", got[1]["value"], "ok")
	}
}

// TestJSONOutputDecodeEmbeds pins the --decode interaction: a component that
// decoded to JSON is spliced in as a JSON value, not nested as a string, so
// `jq .value.count` works without fromjson.
func TestJSONOutputDecodeEmbeds(t *testing.T) {
	body := []byte(`{"id":"a","count":42}`)

	// Without --decode the same bytes are just text, and stay a string.
	plain := captureJSON(t, false, new(consumeOutput), rec("t", nil, body))[0]
	if _, ok := plain["value"].(string); !ok {
		t.Errorf("without --decode, value should be a string, got %#v", plain["value"])
	}

	// With --decode it becomes an object.
	co := &consumeOutput{decodeValue: true}
	decoded := captureJSON(t, false, co, rec("t", nil, body))[0]
	obj, ok := decoded["value"].(map[string]any)
	if !ok {
		t.Fatalf("with --decode, value should be an object, got %#v", decoded["value"])
	}
	if obj["count"] != float64(42) {
		t.Errorf("value.count = %#v, want 42", obj["count"])
	}
}

func TestJSONOutputHeaders(t *testing.T) {
	r := rec("t", nil, []byte("v"))
	r.Headers = []kgo.RecordHeader{
		{Key: "trace-id", Value: []byte("abc")},
		{Key: "raw", Value: []byte{0xff}},
	}
	got := captureJSON(t, false, new(consumeOutput), r)[0]

	hs, ok := got["headers"].([]any)
	if !ok || len(hs) != 2 {
		t.Fatalf("headers = %#v", got["headers"])
	}
	h0 := hs[0].(map[string]any)
	if h0["key"] != "trace-id" || h0["value"] != "abc" {
		t.Errorf("header 0 = %v", h0)
	}
	// Header values follow the same UTF-8 rule as keys and values.
	h1 := hs[1].(map[string]any)
	if h1["value_base64"] != "/w==" {
		t.Errorf("header 1 = %v, want value_base64 //w==", h1)
	}
}

func TestJSONOutputShareGroup(t *testing.T) {
	got := captureJSON(t, true, new(consumeOutput), rec("t", nil, []byte("v")))[0]
	if _, ok := got["delivery_count"]; !ok {
		t.Errorf("delivery_count missing under --share-group: %v", got)
	}
}
