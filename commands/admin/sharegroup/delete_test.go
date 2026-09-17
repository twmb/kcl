package sharegroup

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func TestDeleteGroupResult(t *testing.T) {
	msg := "group has 3 active members"
	empty := ""
	for _, test := range []struct {
		name        string
		group       kmsg.DeleteGroupsResponseGroup
		wantErr     string
		wantMessage string
	}{
		{
			name:  "ok",
			group: kmsg.DeleteGroupsResponseGroup{Group: "g"},
		},
		{
			name:    "error, no message",
			group:   kmsg.DeleteGroupsResponseGroup{Group: "g", ErrorCode: kerr.NonEmptyGroup.Code},
			wantErr: "NON_EMPTY_GROUP",
		},
		{
			name:        "error with a message",
			group:       kmsg.DeleteGroupsResponseGroup{Group: "g", ErrorCode: kerr.NonEmptyGroup.Code, ErrorMessage: &msg},
			wantErr:     "NON_EMPTY_GROUP",
			wantMessage: msg,
		},
		{
			name:    "an empty message is not a message",
			group:   kmsg.DeleteGroupsResponseGroup{Group: "g", ErrorCode: kerr.NonEmptyGroup.Code, ErrorMessage: &empty},
			wantErr: "NON_EMPTY_GROUP",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			errStr, message := deleteGroupResult(test.group)
			if errStr != test.wantErr {
				t.Errorf("error = %q, want %q", errStr, test.wantErr)
			}
			if message != test.wantMessage {
				t.Errorf("message = %q, want %q", message, test.wantMessage)
			}
		})
	}
}

// TestDeleteJSONShape pins the four columns and their JSON keys. kfake sends
// no message, so the key is present and empty.
func TestDeleteJSONShape(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs([]string{
		"--no-config-file", "-B", c.ListenAddrs()[0],
		"-X", "dial_timeout=2s", "-X", "retry_timeout=10s",
		"--format", "json", "share-group", "delete", "nosuchgroup",
	})

	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err = root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	if err != out.ErrSilent {
		t.Fatalf("err = %v, want ErrSilent\n%s", err, b)
	}

	var doc struct {
		Command string `json:"_command"`
		Results []struct {
			Broker  int32  `json:"broker"`
			Group   string `json:"group"`
			Error   string `json:"error"`
			Message string `json:"message"`
		} `json:"results"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, b)
	}
	if doc.Command != "share-group.delete" || len(doc.Results) != 1 {
		t.Fatalf("doc = %+v\n%s", doc, b)
	}
	got := doc.Results[0]
	if got.Group != "nosuchgroup" || got.Error != "GROUP_ID_NOT_FOUND" || got.Message != "" {
		t.Errorf("result = %+v", got)
	}
	var keys map[string]any
	json.Unmarshal(b, &keys)
	results := keys["results"].([]any)
	if _, ok := results[0].(map[string]any)["message"]; !ok {
		t.Errorf("no message key: %s", b)
	}
}

// TestDeleteLiveGroup pins the delete against a share group that exists:
// an Empty one, whose member left, deletes cleanly, and one with a member
// answers NON_EMPTY_GROUP and exits 1. kfake answered GROUP_ID_NOT_FOUND for
// every share group before franz-go #1457.
func TestDeleteLiveGroup(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(t.Context(), 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	produceN(t, cl, "t", 2)

	// The member leaves when joinShareGroup returns.
	joinShareGroup(t, c, "sg-empty", 2, "t")
	stdout, err := runShareGroup(t, c, "", "share-group", "delete", "sg-empty", "--format", "awk")
	if err != nil {
		t.Fatalf("delete sg-empty: %v\n%s", err, stdout)
	}
	if rows := awkRows(stdout); len(rows) != 1 || rows[0][1] != "sg-empty" || rows[0][2] != "-" {
		t.Errorf("rows = %q, want sg-empty deleted with no error", rows)
	}

	// A member that stays.
	c.SetGroupConfigs("sg-live", map[string]string{"share.auto.offset.reset": "earliest"})
	m, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ShareGroup("sg-live"), kgo.ConsumeTopics("t"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if fs := m.PollFetches(ctx); fs.Err() != nil {
		t.Fatal(fs.Err())
	}
	stdout, err = runShareGroup(t, c, "", "share-group", "delete", "sg-live", "--format", "awk")
	if err != out.ErrSilent {
		t.Fatalf("delete sg-live: err = %v, want ErrSilent\n%s", err, stdout)
	}
	if rows := awkRows(stdout); len(rows) != 1 || rows[0][1] != "sg-live" || rows[0][2] != "NON_EMPTY_GROUP" {
		t.Errorf("rows = %q, want sg-live refused with NON_EMPTY_GROUP", rows)
	}
}
