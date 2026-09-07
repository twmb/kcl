package fake

import (
	"bytes"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
)

// TestControlMethods pins the methods we expect to reach remotely. Discovery
// is by name, so a kfake rename would otherwise turn into a 404 in whatever
// test happened to use it rather than a failure here.
func TestControlMethods(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	got := make(map[string]bool)
	for _, m := range controlMethods(c) {
		got[m.Name] = true
	}

	want := []string{
		"AddNode",
		"ApplyRetention",
		"Compact",
		"CoordinatorFor",
		"LeaderFor",
		"ListenAddrs",
		"MoveTopicPartition",
		"PartitionInfo",
		"PartitionInfos",
		"RehashCoordinators",
		"RemoveNode",
		"SetFollowers",
		"ShufflePartitionLeaders",
		"TopicIDInfo",
		"TopicInfo",
	}
	for _, name := range want {
		if !got[name] {
			t.Errorf("method %s is not callable; did kfake rename it?", name)
		}
	}

	// Methods taking a callback cannot cross a wire, and the skip list is
	// only correct while the names on it exist.
	for _, name := range []string{"Control", "ControlKey", "SleepControl"} {
		if got[name] {
			t.Errorf("method %s takes a func and should not be callable", name)
		}
	}
	ct := reflect.TypeOf(c)
	for name := range controlSkip {
		if got[name] {
			t.Errorf("method %s is skipped but was offered anyway", name)
		}
		if _, ok := ct.MethodByName(name); !ok {
			t.Errorf("skipped method %s no longer exists; drop it from controlSkip", name)
		}
	}
}

func TestBuildArg(t *testing.T) {
	tests := []struct {
		name string
		typ  reflect.Type
		in   string
		want any
		err  bool
	}{
		{"string bare", reflect.TypeFor[string](), "foo", "foo", false},
		{"string with spaces", reflect.TypeFor[string](), "a b", "a b", false},
		{"int32", reflect.TypeFor[int32](), "3", int32(3), false},
		{"int32 negative", reflect.TypeFor[int32](), "-1", int32(-1), false},
		{"int32 not a number", reflect.TypeFor[int32](), "three", nil, true},
		{"int slice", reflect.TypeFor[[]int32](), "[1,2,3]", []int32{1, 2, 3}, false},
		{"topic id hex", reflect.TypeFor[[16]byte](), "465a97c59919152e1827030ce374ec71", [16]byte{0x46, 0x5a, 0x97, 0xc5, 0x99, 0x19, 0x15, 0x2e, 0x18, 0x27, 0x03, 0x0c, 0xe3, 0x74, 0xec, 0x71}, false},
		{"topic id dashed", reflect.TypeFor[[16]byte](), "465a97c5-9919-152e-1827-030ce374ec71", [16]byte{0x46, 0x5a, 0x97, 0xc5, 0x99, 0x19, 0x15, 0x2e, 0x18, 0x27, 0x03, 0x0c, 0xe3, 0x74, 0xec, 0x71}, false},
		{"topic id junk", reflect.TypeFor[[16]byte](), "nope", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildArg(tt.typ, tt.in)
			if tt.err {
				if err == nil {
					t.Errorf("buildArg(%s, %q) expected an error", tt.typ, tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildArg(%s, %q): %v", tt.typ, tt.in, err)
			}
			if !reflect.DeepEqual(got.Interface(), tt.want) {
				t.Errorf("buildArg(%s, %q) = %v, want %v", tt.typ, tt.in, got.Interface(), tt.want)
			}
		})
	}
}

func TestControlHandler(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "foo"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	srv := httptest.NewServer(controlHandler(c))
	defer srv.Close()

	call := func(t *testing.T, method string, args ...string) (int, map[string]any) {
		t.Helper()
		if args == nil {
			args = []string{}
		}
		b, err := json.Marshal(map[string]any{"args": args})
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.Post(srv.URL+"/call/"+method, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var got map[string]any
		if err := json.UnmarshalRead(resp.Body, &got); err != nil {
			t.Fatalf("decoding %s: %v", method, err)
		}
		return resp.StatusCode, got
	}

	t.Run("methods", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/methods")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var got struct {
			Methods []controlMethod `json:"methods"`
		}
		if err := json.UnmarshalRead(resp.Body, &got); err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, m := range got.Methods {
			if m.Name == "MoveTopicPartition" {
				found = true
				if m.Signature == "" {
					t.Error("MoveTopicPartition has no signature")
				}
			}
		}
		if !found {
			t.Error("MoveTopicPartition missing from /methods")
		}
	})

	t.Run("no args no results", func(t *testing.T) {
		code, got := call(t, "ShufflePartitionLeaders")
		if code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%v)", code, got)
		}
		if got["result"] != nil {
			t.Errorf("result = %v, want null", got["result"])
		}
	})

	t.Run("result", func(t *testing.T) {
		code, got := call(t, "TopicInfo", "foo")
		if code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%v)", code, got)
		}
		info, ok := got["result"].(map[string]any)
		if !ok {
			t.Fatalf("result = %#v, want an object", got["result"])
		}
		if info["Topic"] != "foo" {
			t.Errorf("Topic = %v, want foo", info["Topic"])
		}
	})

	// A topic ID we print has to be one we take back: json renders a byte
	// array as base64, which uuid.Parse does not read.
	t.Run("topic id round trip", func(t *testing.T) {
		_, got := call(t, "TopicInfo", "foo")
		info, ok := got["result"].(map[string]any)
		if !ok {
			t.Fatalf("result = %#v, want an object", got["result"])
		}
		id, _ := info["TopicID"].(string)
		if len(id) != 32 {
			t.Fatalf("TopicID = %q, want 32 hex chars", id)
		}
		code, got := call(t, "TopicIDInfo", id)
		if code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%v)", code, got)
		}
		back, ok := got["result"].(map[string]any)
		if !ok {
			t.Fatalf("result = %#v, want an object", got["result"])
		}
		if back["Topic"] != "foo" {
			t.Errorf("Topic = %v, want foo", back["Topic"])
		}
	})

	// A method returning only an error returns null on success and the
	// error text on failure. Moving to the leader it already has is a
	// no-op; a node that does not exist is not.
	t.Run("error return", func(t *testing.T) {
		leader := c.LeaderFor("foo", 0)
		if leader < 0 {
			t.Fatalf("no leader for foo/0")
		}
		code, got := call(t, "MoveTopicPartition", "foo", "0", jsonInt(leader))
		if code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%v)", code, got)
		}

		code, got = call(t, "MoveTopicPartition", "foo", "0", "12345")
		if code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (%v)", code, got)
		}
		if got["error"] == nil {
			t.Errorf("no error message in %v", got)
		}
	})

	t.Run("unknown method", func(t *testing.T) {
		code, got := call(t, "Nope")
		if code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404 (%v)", code, got)
		}
	})

	t.Run("wrong arg count", func(t *testing.T) {
		code, got := call(t, "TopicInfo")
		if code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (%v)", code, got)
		}
		if got["error"] == nil {
			t.Errorf("no error message in %v", got)
		}
	})

	t.Run("skipped method", func(t *testing.T) {
		code, _ := call(t, "Close")
		if code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", code)
		}
	})
}

func jsonInt(n int32) string {
	b, _ := json.Marshal(n)
	return string(b)
}
