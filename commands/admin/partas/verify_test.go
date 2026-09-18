package partas

import (
	"encoding/json"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

type verifyDoc struct {
	Command string `json:"_command"`
	Results []struct {
		Topic     string  `json:"topic"`
		Partition int32   `json:"partition"`
		Current   []int32 `json:"current_replicas"`
		Target    []int32 `json:"target_replicas"`
		Status    *string `json:"status"`
		Error     string  `json:"error"`
		Message   string  `json:"message"`
	} `json:"results"`
	Cleared struct {
		Brokers []int32  `json:"brokers"`
		Topics  []string `json:"topics"`
	} `json:"throttles_cleared"`
}

func runVerify(t *testing.T, addr string, plan ...string) (verifyDoc, error) {
	t.Helper()
	raw, err := runReassign(t, addr, "json", append([]string{"reassign", "verify"}, plan...)...)
	var doc verifyDoc
	if raw == "" {
		return doc, err
	}
	if jerr := json.Unmarshal([]byte(strings.SplitN(raw, "\n", 2)[0]), &doc); jerr != nil {
		t.Fatalf("not JSON: %v\n%s", jerr, raw)
	}
	if doc.Command != "reassign.verify" || doc.Cleared.Brokers == nil || doc.Cleared.Topics == nil {
		t.Errorf("doc = %+v, want _command reassign.verify and throttles_cleared arrays", doc)
	}
	// A clean row's ERROR is "", as ErrorColumn has it, never null.
	if strings.Contains(raw, `"error":null`) {
		t.Errorf("a row's error is null:\n%s", raw)
	}
	return doc, err
}

func status(s *string) string {
	if s == nil {
		return "<null>"
	}
	return *s
}

// TestVerify pins each STATUS and the exit rule on the one-broker fake,
// where every partition's replicas are [0] and a reassignment completes at
// once: a plan the cluster matches is complete and clears the throttle the
// alter set, a plan it does not match differs and clears nothing, a plan
// with a partition the metadata faults has an unknown row, and a partition
// the cluster still lists is in progress.
func TestVerify(t *testing.T) {
	c, addr := newCluster(t)

	if _, err := runReassign(t, addr, "json", "reassign", "alter", "foo:0->0", "--throttle", "5000"); err != nil {
		t.Fatalf("alter --throttle: %v", err)
	}
	if got := dynamicConfigs(t, addr, "0", "-tb"); got[brokerLeaderThrottle] != "5000" {
		t.Fatalf("broker 0 dynamic configs = %v, want the throttle set", got)
	}

	// A plan that differs clears nothing and exits 1.
	doc, err := runVerify(t, addr, "foo:0->7")
	if code := out.ExitCode(err); err != out.ErrSilent || code != out.ExitError {
		t.Fatalf("differs: err = %v (exit %d), want a silent exit 1", err, code)
	}
	if len(doc.Results) != 1 || status(doc.Results[0].Status) != statusDiffers || !slices.Equal(doc.Results[0].Current, []int32{0}) || !slices.Equal(doc.Results[0].Target, []int32{7}) || doc.Results[0].Error != "" {
		t.Errorf("differs doc = %+v", doc)
	}
	if len(doc.Cleared.Brokers) != 0 || len(doc.Cleared.Topics) != 0 {
		t.Errorf("differs cleared %+v, want nothing", doc.Cleared)
	}
	if got := dynamicConfigs(t, addr, "0", "-tb"); got[brokerLeaderThrottle] != "5000" || got[brokerFollowerThrottle] != "5000" {
		t.Errorf("broker 0 dynamic configs = %v, want the throttle still set", got)
	}

	// A faulted partition has an unknown row, and the complete partition
	// beside it clears nothing.
	fault := c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Metadata}, Topic: "foo", Partitions: []int32{1}, Err: kerr.LeaderNotAvailable, Count: -1})
	doc, err = runVerify(t, addr, "foo:0->0;1->0")
	fault.Remove()
	if code := out.ExitCode(err); err != out.ErrSilent || code != out.ExitError {
		t.Fatalf("fault: err = %v (exit %d), want a silent exit 1", err, code)
	}
	if len(doc.Results) != 2 || status(doc.Results[0].Status) != statusComplete || doc.Results[1].Status != nil || doc.Results[1].Current != nil || doc.Results[1].Error != "LEADER_NOT_AVAILABLE" {
		t.Errorf("fault doc = %+v", doc)
	}
	if got := dynamicConfigs(t, addr, "0", "-tb"); len(doc.Cleared.Brokers) != 0 || got[brokerLeaderThrottle] != "5000" {
		t.Errorf("fault cleared %+v, broker 0 = %v; want the throttle still set", doc.Cleared, got)
	}

	// A partition the cluster lists as reassigning is in progress. kfake
	// never lists one, so a control answers the list until it is dropped.
	var inProgress atomic.Bool
	inProgress.Store(true)
	c.ControlKey(kmsg.ListPartitionReassignments.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
		if !inProgress.Load() {
			c.DropControl()
			return nil, nil, false
		}
		c.KeepControl()
		resp := kmsg.NewPtrListPartitionReassignmentsResponse()
		rt := kmsg.NewListPartitionReassignmentsResponseTopic()
		rt.Topic = "foo"
		rp := kmsg.NewListPartitionReassignmentsResponseTopicPartition()
		rp.Partition = 0
		rp.Replicas = []int32{0, 1}
		rp.AddingReplicas = []int32{1}
		rt.Partitions = append(rt.Partitions, rp)
		resp.Topics = append(resp.Topics, rt)
		return resp, nil, true
	})
	doc, err = runVerify(t, addr, "foo:0->0")
	if code := out.ExitCode(err); err != out.ErrSilent || code != out.ExitError {
		t.Fatalf("in progress: err = %v (exit %d), want a silent exit 1", err, code)
	}
	if len(doc.Results) != 1 || status(doc.Results[0].Status) != statusInProgress || doc.Results[0].Error != "" {
		t.Errorf("in progress doc = %+v", doc)
	}
	inProgress.Store(false)

	// The plan the alter took is complete: exit 0, and the throttle is
	// cleared on the broker and the topic.
	doc, err = runVerify(t, addr, "foo:0->0")
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if len(doc.Results) != 1 || status(doc.Results[0].Status) != statusComplete || !slices.Equal(doc.Results[0].Current, []int32{0}) {
		t.Errorf("complete doc = %+v", doc)
	}
	if !slices.Equal(doc.Cleared.Brokers, []int32{0}) || !slices.Equal(doc.Cleared.Topics, []string{"foo"}) {
		t.Errorf("complete cleared %+v, want broker 0 and topic foo", doc.Cleared)
	}
	for _, args := range [][]string{{"0", "-tb"}, {"foo", "-tt"}} {
		got := dynamicConfigs(t, addr, args...)
		for _, key := range []string{brokerLeaderThrottle, brokerFollowerThrottle, topicLeaderThrottle, topicFollowerThrottle} {
			if _, ok := got[key]; ok {
				t.Errorf("%v dynamic configs = %v, want %s cleared", args, got, key)
			}
		}
	}

	// A delete that fails is the command's error after the table prints,
	// and the other resources are cleared.
	c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.IncrementalAlterConfigs}, Resource: "foo", Err: kerr.TopicAuthorizationFailed})
	doc, err = runVerify(t, addr, "foo:0->0")
	if code := out.ExitCode(err); err == nil || err == out.ErrSilent || code != out.ExitError || !strings.Contains(err.Error(), "topic foo: TOPIC_AUTHORIZATION_FAILED") {
		t.Fatalf("failed clear: err = %v (exit %d), want an error naming topic foo", err, code)
	}
	if !slices.Equal(doc.Cleared.Brokers, []int32{0}) || len(doc.Cleared.Topics) != 0 {
		t.Errorf("failed clear cleared %+v, want broker 0 only", doc.Cleared)
	}
}

// TestVerifyAwk pins that an awk row has one field per registered header,
// and that the list cells are comma joined.
func TestVerifyAwk(t *testing.T) {
	_, addr := newCluster(t)

	root := &cobra.Command{Use: "kcl"}
	cmd, _, err := Command(client.New(root)).Find([]string{"verify"})
	if err != nil {
		t.Fatal(err)
	}
	header := strings.Split(strings.TrimSuffix(out.AwkHeader(cmd), "\n"), "\t")
	if !slices.Equal(header, verifyHeaders) {
		t.Errorf("registered %v, want %v", header, verifyHeaders)
	}

	raw, err := runReassign(t, addr, "awk", "reassign", "verify", "foo:0->0;1->0,7")
	if err != out.ErrSilent {
		t.Fatalf("err = %v, want a silent exit 1", err)
	}
	rows := strings.Split(strings.TrimSuffix(raw, "\n"), "\n")
	if len(rows) != 2 {
		t.Fatalf("awk rows = %q, want 2", rows)
	}
	for _, row := range rows {
		if n := len(strings.Split(row, "\t")); n != len(header) {
			t.Errorf("awk row has %d fields, header has %d: %q", n, len(header), row)
		}
	}
	if rows[0] != "foo\t0\t0\t0\tcomplete\t-\t-" || rows[1] != "foo\t1\t0\t0,7\tdiffers\t-\t-" {
		t.Errorf("awk rows = %q", rows)
	}
}
