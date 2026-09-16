package fake

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/out"
)

func TestGroupWaitMatch(t *testing.T) {
	stable := &kfake.GroupInfo{
		Group: "g1",
		State: "Stable",
		Epoch: 3,
		Members: []kfake.GroupMember{
			{MemberID: "a", Assignment: map[string][]int32{"foo": {0, 1}}},
			{MemberID: "b", Assignment: map[string][]int32{"foo": {2}}},
		},
	}
	empty := &kfake.GroupInfo{Group: "g1", State: "Empty"}
	n := func(i int) *int { return &i }

	tests := []struct {
		name string
		wait groupWait
		info *kfake.GroupInfo
		want bool
	}{
		{"members", groupWait{Members: n(2)}, stable, true},
		{"members too few", groupWait{Members: n(1)}, stable, false},
		{"members none left", groupWait{Members: n(0)}, empty, true},
		{"state", groupWait{State: "Stable"}, stable, true},
		{"state without case", groupWait{State: "stable"}, stable, true},
		{"state other", groupWait{State: "Empty"}, stable, false},
		{"assigned", groupWait{Assigned: n(3)}, stable, true},
		{"assigned other", groupWait{Assigned: n(2)}, stable, false},
		{"assigned none", groupWait{Assigned: n(0)}, empty, true},
		{"all three", groupWait{Members: n(2), State: "stable", Assigned: n(3)}, stable, true},
		{"all three, one off", groupWait{Members: n(2), State: "stable", Assigned: n(4)}, stable, false},
		{"no such group", groupWait{Members: n(2)}, nil, false},
		{"no such group, none wanted", groupWait{Members: n(0)}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.wait.match(tt.info); got != tt.want {
				t.Errorf("match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupShape(t *testing.T) {
	tests := []struct {
		name string
		info *kfake.GroupInfo
		want string
	}{
		{"missing", nil, "no such group"},
		{"empty", &kfake.GroupInfo{State: "Empty"}, "Empty, 0 member(s), 0 assigned"},
		{
			"stable",
			&kfake.GroupInfo{State: "Stable", Members: []kfake.GroupMember{{Assignment: map[string][]int32{"foo": {0, 1, 2}}}}},
			"Stable, 1 member(s), 3 assigned",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := groupShape(tt.info); got != tt.want {
				t.Errorf("groupShape() = %q, want %q", got, tt.want)
			}
		})
	}
}

// A wait with no selector would return the moment the group exists, which is
// not a wait, so it says what to give it.
func TestGroupWaitSelectorsRequired(t *testing.T) {
	addr := "127.0.0.1:1" // never dialed: we fail before the request
	cmd := groupWaitCommand(&addr)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"g1"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected an error")
	}
	if want := "give at least one of --members, --state, --assigned"; err.Error() != want {
		t.Errorf("error = %q, want %q", err, want)
	}
	if got := out.ExitCode(err); got != out.ExitUsage {
		t.Errorf("exit code = %d, want %d", got, out.ExitUsage)
	}
}

func TestGroupWait(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "foo"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	srv := httptest.NewServer(controlHandler(c))
	defer srv.Close()

	do := func(t *testing.T, body any) (int, map[string]any) {
		t.Helper()
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.Post(srv.URL+"/groups/wait", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var got map[string]any
		json.UnmarshalRead(resp.Body, &got) //nolint:errcheck // an empty body is fine
		return resp.StatusCode, got
	}

	// A group nobody joined never forms, and the wait says both that it is
	// not there and how long we gave it.
	t.Run("no such group", func(t *testing.T) {
		code, got := do(t, groupWait{Group: "nosuch", Members: ptr(1), Timeout: "200ms"})
		if code != http.StatusRequestTimeout {
			t.Fatalf("status %d (%v), want 408", code, got)
		}
		if want := "timed out after 200ms waiting for group nosuch: no such group"; got["error"] != want {
			t.Errorf("error = %v, want %q", got["error"], want)
		}
		if got["usage"] != nil {
			t.Errorf("usage = %v, want unset for a timeout", got["usage"])
		}
	})

	t.Run("no selectors", func(t *testing.T) {
		code, got := do(t, groupWait{Group: "g1", Timeout: "200ms"})
		if code != http.StatusBadRequest {
			t.Fatalf("status %d (%v), want 400", code, got)
		}
		if want := "give at least one of members, state, assigned"; got["error"] != want {
			t.Errorf("error = %v, want %q", got["error"], want)
		}
		if got["usage"] != true {
			t.Errorf("usage = %v, want true", got["usage"])
		}
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup("g1"),
		kgo.ConsumeTopics("foo"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go cl.PollFetches(pctx) //nolint:errcheck // polling is how the consumer joins

	members := func(t *testing.T, got map[string]any) []any {
		t.Helper()
		info, ok := got["group"].(map[string]any)
		if !ok {
			t.Fatalf("group = %#v, want an object", got["group"])
		}
		if info["Group"] != "g1" {
			t.Errorf("Group = %v, want g1", info["Group"])
		}
		ms, _ := info["Members"].([]any)
		return ms
	}

	t.Run("members", func(t *testing.T) {
		code, got := do(t, groupWait{Group: "g1", Members: ptr(1), Timeout: "30s"})
		if code != http.StatusOK {
			t.Fatalf("status %d (%v)", code, got)
		}
		if ms := members(t, got); len(ms) != 1 {
			t.Errorf("members = %d, want 1", len(ms))
		}
	})

	t.Run("state and members", func(t *testing.T) {
		code, got := do(t, groupWait{Group: "g1", State: "stable", Members: ptr(1), Timeout: "30s"})
		if code != http.StatusOK {
			t.Fatalf("status %d (%v)", code, got)
		}
		info, _ := got["group"].(map[string]any)
		if info["State"] != "Stable" {
			t.Errorf("State = %v, want Stable", info["State"])
		}
	})

	// The one member holds every partition of the seeded topic.
	t.Run("assigned", func(t *testing.T) {
		code, got := do(t, groupWait{Group: "g1", Assigned: ptr(3), Timeout: "30s"})
		if code != http.StatusOK {
			t.Fatalf("status %d (%v)", code, got)
		}
		members(t, got)
	})

	// A second member never joins, so the wait runs out and says what the
	// group looked like when it did.
	t.Run("timeout says the shape", func(t *testing.T) {
		code, got := do(t, groupWait{Group: "g1", Members: ptr(2), Timeout: "200ms"})
		if code != http.StatusRequestTimeout {
			t.Fatalf("status %d (%v), want 408", code, got)
		}
		if want := "timed out after 200ms waiting for group g1: Stable, 1 member(s), 3 assigned"; got["error"] != want {
			t.Errorf("error = %v, want %q", got["error"], want)
		}
	})

	// The client carries the timeout back as an exit 1: the wait ran out,
	// which is not a command you typed wrong.
	t.Run("client exit code", func(t *testing.T) {
		addr := srv.Listener.Addr().String()
		root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
		root.PersistentFlags().String("format", out.FormatText, "output format")
		root.AddCommand(groupCommand(&addr))
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs([]string{"group", "wait", "g1", "--members", "2", "--timeout", "200ms"})

		err := root.Execute()
		if err == nil {
			t.Fatal("expected an error")
		}
		if want := "timed out after 200ms waiting for group g1: Stable, 1 member(s), 3 assigned"; err.Error() != want {
			t.Errorf("error = %q, want %q", err, want)
		}
		if got := out.ExitCode(err); got != out.ExitError {
			t.Errorf("exit code = %d, want %d", got, out.ExitError)
		}
	})

	// A run that works prints one row, and json prints kfake's own group.
	t.Run("output", func(t *testing.T) {
		addr := srv.Listener.Addr().String()
		run := func(format string) string {
			t.Helper()
			root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
			root.PersistentFlags().String("format", out.FormatText, "output format")
			root.AddCommand(groupCommand(&addr))
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			root.SetArgs([]string{"group", "wait", "g1", "--members", "1", "--format", format})
			return captureStdout(t, func() {
				if err := root.Execute(); err != nil {
					t.Errorf("%s: %v", format, err)
				}
			})
		}
		if want := "GROUP  STATE   EPOCH  MEMBERS  ASSIGNED\ng1     Stable  1      1        3\n"; run(out.FormatText) != want {
			t.Errorf("text = %q, want %q", run(out.FormatText), want)
		}
		if want := "g1\tStable\t1\t1\t3\n"; run(out.FormatAWK) != want {
			t.Errorf("awk = %q, want %q", run(out.FormatAWK), want)
		}
		got := run(out.FormatJSON)
		var doc struct {
			Command string           `json:"_command"`
			Version int              `json:"_version"`
			Group   *kfake.GroupInfo `json:"group"`
		}
		if err := json.Unmarshal([]byte(got), &doc); err != nil {
			t.Fatalf("json %q: %v", got, err)
		}
		if doc.Command != "fake.control.group.wait" || doc.Version != 1 {
			t.Errorf("_command = %q, _version = %d", doc.Command, doc.Version)
		}
		if doc.Group == nil || doc.Group.Group != "g1" || doc.Group.State != "Stable" || doc.Group.NumAssigned() != 3 {
			t.Errorf("group = %+v", doc.Group)
		}
	})
}

func ptr[T any](v T) *T { return &v }
