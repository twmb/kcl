package fake

import (
	"bytes"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/twmb/kcl/out"
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
		// The call was fine, the cluster refused it, so the client exits
		// 1 rather than 2.
		if got["usage"] != nil {
			t.Errorf("usage = %v, want unset for a method that ran and failed", got["usage"])
		}
	})

	// A method that answers with a nil pointer found nothing. Handing back
	// null and exiting 0 reads as success, so it is an error, and not a
	// usage one: the call was fine.
	t.Run("nil result", func(t *testing.T) {
		code, got := call(t, "TopicInfo", "nosuch")
		if code == http.StatusOK {
			t.Fatalf("status = %d, want a failure (%v)", code, got)
		}
		if want := "TopicInfo nosuch: not found"; got["error"] != want {
			t.Errorf("error = %v, want %q", got["error"], want)
		}
		if got["usage"] != nil {
			t.Errorf("usage = %v, want unset", got["usage"])
		}

		code, got = call(t, "GroupInfo", "nosuch")
		if code == http.StatusOK {
			t.Fatalf("status = %d, want a failure (%v)", code, got)
		}
		if want := "GroupInfo nosuch: not found"; got["error"] != want {
			t.Errorf("error = %v, want %q", got["error"], want)
		}
	})

	t.Run("unknown method", func(t *testing.T) {
		code, got := call(t, "Nope")
		if code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404 (%v)", code, got)
		}
		if got["usage"] != true {
			t.Errorf("usage = %v, want true", got["usage"])
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
		if got["usage"] != true {
			t.Errorf("usage = %v, want true", got["usage"])
		}
	})

	t.Run("unbuildable argument", func(t *testing.T) {
		code, got := call(t, "MoveTopicPartition", "foo", "0", "two")
		if code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (%v)", code, got)
		}
		if got["usage"] != true {
			t.Errorf("usage = %v, want true", got["usage"])
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

// TestControlDoExitCodes pins what the client exits with: 2 when the endpoint
// says the call itself was wrong, 1 when the cluster ran it and refused, and
// 1 for anything we cannot read.
func TestControlDoExitCodes(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   int
	}{
		{"unknown method", http.StatusNotFound, `{"error":"unknown method \"Nope\"","usage":true}`, out.ExitUsage},
		{"cluster refused", http.StatusBadRequest, `{"error":"node 9 not found"}`, out.ExitError},
		{"no fault", http.StatusNotFound, `{"error":"no fault 99"}`, out.ExitError},
		{"timed out", http.StatusRequestTimeout, `{"error":"timed out after 2s waiting for 100 hit(s), have 1"}`, out.ExitError},
		{"not a document", http.StatusInternalServerError, `nope`, out.ExitError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				io.WriteString(w, tt.body) //nolint:errcheck // the test client is right here
			}))
			defer srv.Close()

			var into struct{}
			err := controlDo(http.MethodGet, srv.Listener.Addr().String(), "/methods", nil, &into)
			if err == nil {
				t.Fatal("expected an error")
			}
			if got := out.ExitCode(err); got != tt.want {
				t.Errorf("exit code = %d, want %d (%v)", got, tt.want, err)
			}
		})
	}
}

// TestControlCall drives the call command the way main.go wires it: --format
// is a persistent flag on the root, and the root turns a flag error into exit
// 2.
func TestControlCall(t *testing.T) {
	var gotPath string
	var gotArgs []string
	status := http.StatusOK
	body := `{"result":null}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		var req struct {
			Args []string `json:"args"`
		}
		if err := json.UnmarshalRead(r.Body, &req); err != nil {
			t.Error(err)
		}
		gotArgs = req.Args
		w.WriteHeader(status)
		io.WriteString(w, body) //nolint:errcheck // the test client is right here
	}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	run := func(args ...string) error {
		gotPath, gotArgs = "", nil
		root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
		root.PersistentFlags().String("format", out.FormatText, "output format")
		root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
			return out.Errf(out.ExitUsage, "%v", err)
		})
		root.AddCommand(controlCallCommand(&addr))
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(args)
		return root.Execute()
	}

	// -- is how an argument that starts with a dash reaches the method.
	t.Run("dash argument after --", func(t *testing.T) {
		if err := run("call", "--", "DeleteRecords", "foo", "0", "-1"); err != nil {
			t.Fatalf("call: %v", err)
		}
		if want := "/call/DeleteRecords"; gotPath != want {
			t.Errorf("path = %q, want %q", gotPath, want)
		}
		if want := []string{"foo", "0", "-1"}; !reflect.DeepEqual(gotArgs, want) {
			t.Errorf("args = %q, want %q", gotArgs, want)
		}
	})

	// Without it, pflag reads -1 as a flag; the error says what to do.
	t.Run("dash argument without --", func(t *testing.T) {
		err := run("call", "DeleteRecords", "foo", "0", "-1")
		if err == nil {
			t.Fatal("expected an error")
		}
		want := "unknown shorthand flag: '1' in -1; put -- before METHOD to pass an argument that starts with -"
		if err.Error() != want {
			t.Errorf("error = %q, want %q", err, want)
		}
		if got := out.ExitCode(err); got != out.ExitUsage {
			t.Errorf("exit code = %d, want %d", got, out.ExitUsage)
		}
	})

	// A flag after the arguments is still a flag, as everywhere else in kcl.
	t.Run("flag after the arguments", func(t *testing.T) {
		status, body = http.StatusBadRequest, `{"error":"TopicInfo nosuch: not found"}`
		defer func() { status, body = http.StatusOK, `{"result":null}` }()
		err := run("call", "TopicInfo", "nosuch", "--format", "json")
		if err == nil {
			t.Fatal("expected an error")
		}
		if want := "TopicInfo nosuch: not found"; err.Error() != want {
			t.Errorf("error = %q, want %q", err, want)
		}
		if got := out.ExitCode(err); got != out.ExitError {
			t.Errorf("exit code = %d, want %d", got, out.ExitError)
		}
		if want := []string{"nosuch"}; !reflect.DeepEqual(gotArgs, want) {
			t.Errorf("args = %q, want %q", gotArgs, want)
		}
		doc := out.ErrorDoc(err, "fake.control.call")
		if doc["code"] != out.ExitError || doc["error"] != "TopicInfo nosuch: not found" {
			t.Errorf("error doc = %v", doc)
		}
	})
}
