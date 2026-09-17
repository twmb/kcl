package group

import (
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
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
		"--format", "json", "group", "delete", "nosuchgroup",
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
	if doc.Command != "group.delete" || len(doc.Results) != 1 {
		t.Fatalf("doc = %+v\n%s", doc, b)
	}
	got := doc.Results[0]
	if got.Group != "nosuchgroup" || got.Error != "GROUP_ID_NOT_FOUND" || got.Message != "" {
		t.Errorf("result = %+v", got)
	}
	if !json.Valid(b) {
		t.Errorf("invalid JSON: %s", b)
	}
	var keys map[string]any
	json.Unmarshal(b, &keys)
	results := keys["results"].([]any)
	if _, ok := results[0].(map[string]any)["message"]; !ok {
		t.Errorf("no message key: %s", b)
	}
}
