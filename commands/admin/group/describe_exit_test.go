package group

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

// TestDescribeMissingGroupFails pins that describing a group the broker does
// not know exits non-zero, as describing a missing topic does, while a group
// that exists describes cleanly.
func TestDescribeMissingGroupFails(t *testing.T) {
	c, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Join a group so that one exists.
	member, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumerGroup("g1"), kgo.ConsumeTopics("t"))
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	member.PollFetches(ctx)
	cancel()

	for _, test := range []struct {
		group   string
		wantErr error
	}{
		{"g1", nil},
		{"nope", out.ErrSilent},
	} {
		t.Run(test.group, func(t *testing.T) {
			root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
			cl := client.New(root)
			root.AddCommand(Command(cl))
			root.SetArgs([]string{"--no-config-file", "-B", c.ListenAddrs()[0], "-X", "dial_timeout=2s", "-X", "retry_timeout=10s", "group", "describe", test.group})
			r, w, _ := os.Pipe()
			old := os.Stdout
			os.Stdout = w
			err := root.Execute()
			w.Close()
			os.Stdout = old
			io.ReadAll(r)
			if err != test.wantErr {
				t.Fatalf("err = %v, want %v", err, test.wantErr)
			}
		})
	}
}
