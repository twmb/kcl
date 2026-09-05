package fake

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestParseRequestKey(t *testing.T) {
	tests := []struct {
		in   string
		want kmsg.Key
		err  bool
	}{
		{"fetch", kmsg.Fetch, false},
		{"Fetch", kmsg.Fetch, false},
		{"FETCH", kmsg.Fetch, false},
		{"produce", kmsg.Produce, false},
		{"listoffsets", kmsg.ListOffsets, false},
		{"0", kmsg.Produce, false},
		{"1", kmsg.Fetch, false},
		{"9999", 0, true},
		{"nope", 0, true},
		{"", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseRequestKey(tt.in)
			if tt.err {
				if err == nil {
					t.Errorf("parseRequestKey(%q) expected an error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRequestKey(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("parseRequestKey(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseErrorCode(t *testing.T) {
	tests := []struct {
		in   string
		want int16
		err  bool
	}{
		{"UNKNOWN_TOPIC_ID", 100, false},
		{"unknown_topic_id", 100, false},
		{"INVALID_TOPIC_EXCEPTION", 17, false},
		{"NOT_LEADER_FOR_PARTITION", 6, false},
		{"NOT_LEADER_OR_FOLLOWER", 6, false}, // Kafka's name since 2.6; kerr keeps the original
		{"not_leader_or_follower", 6, false},
		{"100", 100, false},
		{"-1", -1, false},
		{"0", 0, true}, // not an error
		{"9999", 0, true},
		{"nope", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseErrorCode(tt.in)
			if tt.err {
				if err == nil {
					t.Errorf("parseErrorCode(%q) expected an error, got %v", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseErrorCode(%q): %v", tt.in, err)
			}
			if got.Code != tt.want {
				t.Errorf("parseErrorCode(%q) = %d, want %d", tt.in, got.Code, tt.want)
			}
		})
	}
}

func TestRuleFault(t *testing.T) {
	r := Rule{
		Keys:       []string{"fetch", "1"},
		Nodes:      []int32{0, 2},
		Topic:      "foo",
		TopicID:    "465a97c5-9919-152e-1827-030ce374ec71",
		Partitions: []int32{0, 1},
		Group:      "g",
		TxnID:      "t",
		Resource:   "r",
		TopLevel:   true,
		Error:      "UNKNOWN_TOPIC_ID",
		Count:      -1,
	}
	f, err := r.fault()
	if err != nil {
		t.Fatal(err)
	}
	if want := []kmsg.Key{kmsg.Fetch, kmsg.Fetch}; !reflect.DeepEqual(f.Keys, want) {
		t.Errorf("Keys = %v, want %v", f.Keys, want)
	}
	wantID := [16]byte{0x46, 0x5a, 0x97, 0xc5, 0x99, 0x19, 0x15, 0x2e, 0x18, 0x27, 0x03, 0x0c, 0xe3, 0x74, 0xec, 0x71}
	if f.TopicID != wantID {
		t.Errorf("TopicID = %x, want %x", f.TopicID, wantID)
	}
	if f.Err != kerr.UnknownTopicID {
		t.Errorf("Err = %v, want UNKNOWN_TOPIC_ID", f.Err)
	}
	if f.Topic != "foo" || f.Group != "g" || f.TxnID != "t" || f.Resource != "r" || !f.TopLevel || f.Count != -1 {
		t.Errorf("fault did not carry the rule through: %+v", f)
	}

	// An unset error leaves kfake its own default.
	f, err = Rule{Topic: "foo"}.fault()
	if err != nil {
		t.Fatal(err)
	}
	if f.Err != nil {
		t.Errorf("Err = %v, want nil so kfake defaults it", f.Err)
	}

	for _, bad := range []Rule{
		{Keys: []string{"nope"}},
		{TopicID: "not-a-uuid"},
		{Error: "NOT_AN_ERROR"},
	} {
		if _, err := bad.fault(); err == nil {
			t.Errorf("%+v: expected an error", bad)
		}
	}
}

// A rule we echo back should carry only what was set: json v2's omitempty
// keeps a false or a zero, which put "top_level":false on every listing.
func TestRuleMarshal(t *testing.T) {
	for _, tt := range []struct {
		rule Rule
		want string
	}{
		{Rule{}, `{}`},
		{Rule{Topic: "foo"}, `{"topic":"foo"}`},
		{Rule{TopLevel: true}, `{"top_level":true}`},
		{Rule{Count: -1}, `{"count":-1}`},
		{Rule{Keys: []string{"fetch"}, Error: "UNKNOWN_TOPIC_ID"}, `{"keys":["fetch"],"error":"UNKNOWN_TOPIC_ID"}`},
	} {
		b, err := json.Marshal(tt.rule)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != tt.want {
			t.Errorf("marshal(%+v) = %s, want %s", tt.rule, b, tt.want)
		}
	}
}

func TestParseRules(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		path := dir + "/" + name
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return "@" + path
	}

	arr := `[
  {"topic":"foo","error":"UNKNOWN_TOPIC_ID","count":1},
  {"topic":"bar","count":-1}
]
`
	tests := []struct {
		name string
		in   string
		want []Rule
		err  bool
	}{
		{"object", `{"topic":"foo"}`, []Rule{{Topic: "foo"}}, false},
		{"array", `[{"topic":"foo"},{"topic":"bar"}]`, []Rule{{Topic: "foo"}, {Topic: "bar"}}, false},
		{"leading space", "  \n {\"topic\":\"foo\"}", []Rule{{Topic: "foo"}}, false},
		{"file object", write("one.json", `{"topic":"foo"}`), []Rule{{Topic: "foo"}}, false},
		{"file array", write("many.json", arr), []Rule{{Topic: "foo", Error: "UNKNOWN_TOPIC_ID", Count: 1}, {Topic: "bar", Count: -1}}, false},
		{"unknown member", `{"topci":"foo"}`, nil, true},
		{"not json", `nope`, nil, true},
		{"missing file", "@" + dir + "/nope.json", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRules(tt.in)
			if tt.err {
				if err == nil {
					t.Errorf("parseRules(%q) expected an error, got %+v", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRules(%q): %v", tt.in, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseRules(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestFaults(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "foo"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	srv := httptest.NewServer(controlHandler(c))
	defer srv.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := context.Background()

	do := func(t *testing.T, method, path string, body any) (int, map[string]any) {
		t.Helper()
		var rdr *bytes.Reader
		if body != nil {
			b, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			rdr = bytes.NewReader(b)
		} else {
			rdr = bytes.NewReader(nil)
		}
		req, err := http.NewRequest(method, srv.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var got map[string]any
		json.UnmarshalRead(resp.Body, &got) //nolint:errcheck // an empty body is fine
		return resp.StatusCode, got
	}

	// listOffsets returns partition 0's error code.
	listOffsets := func(t *testing.T) int16 {
		t.Helper()
		req := kmsg.NewPtrListOffsetsRequest()
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = "foo"
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition = 0
		rp.Timestamp = -1
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("unexpected response shape %+v", resp)
		}
		return resp.Topics[0].Partitions[0].ErrorCode
	}

	if code := listOffsets(t); code != 0 {
		t.Fatalf("unfaulted list offsets returned %d", code)
	}

	// INVALID_TOPIC_EXCEPTION is not retriable, so the client hands it
	// back rather than asking again and consuming the budget.
	code, got := do(t, http.MethodPost, "/faults", map[string]any{"rules": []Rule{{
		Keys:  []string{"listoffsets"},
		Topic: "foo",
		Error: "INVALID_TOPIC_EXCEPTION",
		Count: 1,
	}}})
	if code != http.StatusOK {
		t.Fatalf("installing: status %d (%v)", code, got)
	}
	id, ok := got["id"].(float64)
	if !ok || id != 1 {
		t.Fatalf("id = %v, want 1", got["id"])
	}

	if code := listOffsets(t); code != 17 {
		t.Errorf("faulted list offsets returned %d, want 17", code)
	}
	// The budget was one request, so the next is answered normally.
	if code := listOffsets(t); code != 0 {
		t.Errorf("list offsets after the budget returned %d, want 0", code)
	}

	t.Run("list shows hits", func(t *testing.T) {
		code, got := do(t, http.MethodGet, "/faults", nil)
		if code != http.StatusOK {
			t.Fatalf("status %d", code)
		}
		fs, _ := got["faults"].([]any)
		if len(fs) != 1 {
			t.Fatalf("faults = %v, want 1", got["faults"])
		}
		f := fs[0].(map[string]any)
		if f["hits"] != float64(1) {
			t.Errorf("hits = %v, want 1", f["hits"])
		}
	})

	t.Run("wait", func(t *testing.T) {
		// Already at one hit, so this returns without blocking.
		code, got := do(t, http.MethodPost, "/faults/1/wait", map[string]any{"hits": 1, "timeout": "5s"})
		if code != http.StatusOK {
			t.Fatalf("status %d (%v)", code, got)
		}
		// Two hits never come; the fault spent its budget.
		code, got = do(t, http.MethodPost, "/faults/1/wait", map[string]any{"hits": 2, "timeout": "50ms"})
		if code != http.StatusRequestTimeout {
			t.Fatalf("status %d (%v), want 408", code, got)
		}
	})

	t.Run("topic id selector", func(t *testing.T) {
		id := c.TopicInfo("foo").TopicID
		code, got := do(t, http.MethodPost, "/faults", map[string]any{"rules": []Rule{{
			Keys:    []string{"metadata"},
			TopicID: hex.EncodeToString(id[:]),
			Error:   "INVALID_TOPIC_EXCEPTION",
			Count:   1,
		}}})
		if code != http.StatusOK {
			t.Fatalf("installing: status %d (%v)", code, got)
		}
		req := kmsg.NewPtrMetadataRequest()
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr("foo")
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) == 0 {
			t.Fatalf("unexpected response shape %+v", resp)
		}
		if code := resp.Topics[0].Partitions[0].ErrorCode; code != 17 {
			t.Errorf("metadata faulted by topic ID returned %d, want 17", code)
		}
	})

	t.Run("bad rules", func(t *testing.T) {
		for _, body := range []map[string]any{
			{"rules": []Rule{{Error: "NOT_AN_ERROR"}}},
			{"rules": []Rule{{Keys: []string{"nope"}}}},
			{"rules": []Rule{}},
		} {
			if code, got := do(t, http.MethodPost, "/faults", body); code != http.StatusBadRequest {
				t.Errorf("%v: status %d (%v), want 400", body, code, got)
			}
		}
	})

	t.Run("unknown member", func(t *testing.T) {
		req := `{"rules":[{"topci":"foo"}]}`
		resp, err := http.Post(srv.URL+"/faults", "application/json", bytes.NewReader([]byte(req)))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status %d, want 400 for a misspelled member", resp.StatusCode)
		}
	})

	t.Run("remove", func(t *testing.T) {
		if code, got := do(t, http.MethodDelete, "/faults/1", nil); code != http.StatusOK {
			t.Fatalf("status %d (%v)", code, got)
		}
		if code, _ := do(t, http.MethodDelete, "/faults/1", nil); code != http.StatusNotFound {
			t.Errorf("status %d, want 404 removing twice", code)
		}
	})

	t.Run("remove all", func(t *testing.T) {
		do(t, http.MethodDelete, "/faults", nil) // clear what earlier subtests left
		do(t, http.MethodPost, "/faults", map[string]any{"rules": []Rule{{Topic: "foo"}}})
		do(t, http.MethodPost, "/faults", map[string]any{"rules": []Rule{{Topic: "bar"}}})
		code, got := do(t, http.MethodDelete, "/faults", nil)
		if code != http.StatusOK {
			t.Fatalf("status %d (%v)", code, got)
		}
		if got["removed"] != float64(2) {
			t.Errorf("removed = %v, want 2", got["removed"])
		}
	})
}
