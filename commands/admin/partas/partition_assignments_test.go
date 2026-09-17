package partas

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/commands/admin/configs"
	"github.com/twmb/kcl/out"
)

func runReassign(t *testing.T, addr, format string, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl), configs.Command(cl))
	root.SetArgs(append([]string{"--no-config-file", "-B", addr, "--format", format}, args...))

	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	return string(b), err
}

type resultsDoc struct {
	Results []struct {
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Error     string `json:"error"`
		Message   string `json:"message"`
	} `json:"results"`
}

func newCluster(t *testing.T) string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "foo"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()[0]
}

// TestAlterCancelResults pins the result shape and the exit code: a clean
// partition exits 0, an unknown one or a cancel with nothing to cancel
// exits 1 after its row prints, and rows sort by partition.
func TestAlterCancelResults(t *testing.T) {
	addr := newCluster(t)

	raw, err := runReassign(t, addr, "json", "reassign", "alter", "foo:1->0;0->0")
	if err != nil {
		t.Fatalf("alter: %v\n%s", err, raw)
	}
	var doc resultsDoc
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if len(doc.Results) != 2 || doc.Results[0].Partition != 0 || doc.Results[1].Partition != 1 || doc.Results[0].Error != "" {
		t.Errorf("alter doc = %+v, want partitions 0 and 1, clean", doc)
	}

	for _, args := range [][]string{
		{"reassign", "alter", "nosuch:0->0"},
		{"reassign", "cancel", "foo:0"},
	} {
		raw, err := runReassign(t, addr, "json", args...)
		if code := out.ExitCode(err); err == nil || code != out.ExitError {
			t.Fatalf("%v: err = %v (exit %d), want a silent exit 1\n%s", args, err, code, raw)
		}
		doc = resultsDoc{}
		if err := json.Unmarshal([]byte(raw), &doc); err != nil {
			t.Fatalf("%v: not JSON: %v\n%s", args, err, raw)
		}
		if len(doc.Results) != 1 || doc.Results[0].Error == "" {
			t.Errorf("%v: doc = %+v, want one failed row", args, doc)
		}
		awk, _ := runReassign(t, addr, "awk", args...)
		if n := len(strings.Split(strings.TrimSuffix(awk, "\n"), "\t")); n != len(resultHeaders) {
			t.Errorf("%v: awk row = %q, want %d fields", args, awk, len(resultHeaders))
		}
	}
}

func TestParseErrorsExitUsage(t *testing.T) {
	for _, args := range [][]string{
		{"reassign", "alter", "foo:0:1"},
		{"reassign", "alter", "foo:0->x"},
		{"reassign", "cancel", "foo:x"},
		{"reassign", "cancel", "foo"},
		{"reassign", "list", "foo"},
	} {
		_, err := runReassign(t, "localhost:1", "text", args...)
		if code := out.ExitCode(err); err == nil || code != out.ExitUsage {
			t.Errorf("%v: err = %v (exit %d), want exit 2", args, err, code)
		}
	}
}

// TestAlterThrottle pins that --throttle sets the broker rates and the topic
// replica lists before the reassignment, on a one-broker cluster where the
// only broker is both the source and the destination.
func TestAlterThrottle(t *testing.T) {
	addr := newCluster(t)

	raw, err := runReassign(t, addr, "json", "reassign", "alter", "foo:0->0", "--throttle", "5000")
	if err != nil {
		t.Fatalf("alter --throttle: %v\n%s", err, raw)
	}

	configsOf := func(args ...string) map[string]string {
		t.Helper()
		raw, err := runReassign(t, addr, "json", append([]string{"config", "describe"}, args...)...)
		if err != nil {
			t.Fatalf("config describe %v: %v", args, err)
		}
		var doc struct {
			Configs []struct {
				Key    string `json:"key"`
				Value  string `json:"value"`
				Source string `json:"source"`
			} `json:"configs"`
		}
		if err := json.Unmarshal([]byte(raw), &doc); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, raw)
		}
		got := make(map[string]string)
		for _, c := range doc.Configs {
			if strings.HasPrefix(c.Source, "DYNAMIC") {
				got[c.Key] = c.Value
			}
		}
		return got
	}
	broker := configsOf("0", "-tb")
	if broker[brokerLeaderThrottle] != "5000" || broker[brokerFollowerThrottle] != "5000" {
		t.Errorf("broker 0 dynamic configs = %v, want both rates at 5000", broker)
	}
	topic := configsOf("foo", "-tt")
	if topic[topicLeaderThrottle] != "0:0" || topic[topicFollowerThrottle] != "" {
		t.Errorf("foo dynamic configs = %v, want leader 0:0 and follower empty", topic)
	}

	// A topic with no replicas cannot be throttled, and nothing is asked
	// of the cluster.
	_, err = runReassign(t, addr, "json", "reassign", "alter", "nosuch:0->0", "--throttle", "5000")
	if err == nil || !strings.Contains(err.Error(), "no current replicas") {
		t.Errorf("throttling an unknown topic: err = %v, want a no current replicas error", err)
	}
}
