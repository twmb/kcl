package topic

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
)

func partitionCount(t *testing.T, addr, topic string) int {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	req := kmsg.NewPtrMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatal(err)
	}
	return len(resp.Topics[0].Partitions)
}

func TestAddPartitions(t *testing.T) {
	for _, test := range []struct {
		name      string
		args      []string
		wantParts int
		wantErr   string
	}{
		{name: "-n", args: []string{"foo", "-n", "2"}, wantParts: 4},
		{name: "-a per partition", args: []string{"foo", "-a", "0", "-a", "0"}, wantParts: 4},
		{name: "-a in one value", args: []string{"foo", "-a", "0:0"}, wantParts: 4},
		{name: "old form", args: []string{"-t", "foo", "0", ":", "0"}, wantParts: 4},
		{name: "--total above", args: []string{"foo", "--total", "5"}, wantParts: 5},
		{name: "--total at is nothing to do", args: []string{"foo", "--total", "2"}, wantParts: 2},
		{name: "--total below fails", args: []string{"foo", "--total", "1"}, wantErr: "has 2 partitions, more than --total 1"},
		{name: "--total with matching -a", args: []string{"foo", "--total", "4", "-a", "0", "-a", "0"}, wantParts: 4},
		{name: "--total with wrong -a count", args: []string{"foo", "--total", "4", "-a", "0"}, wantErr: "needs 2 new partitions but -a lists 1"},
		{name: "-n and --total", args: []string{"foo", "-n", "1", "--total", "3"}, wantErr: "exclusive"},
		{name: "-n and -a", args: []string{"foo", "-n", "1", "-a", "0"}, wantErr: "exclusive"},
		{name: "nothing", args: []string{"foo"}, wantErr: "nothing to add"},
		{name: "two topics", args: []string{"foo", "bar", "-n", "1"}, wantErr: "takes one topic"},
		{name: "wrong replica count", args: []string{"foo", "-a", "0,1"}, wantErr: "each -a must list 1 brokers"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "foo"))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			addr := c.ListenAddrs()[0]

			root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
			cl := client.New(root)
			root.AddCommand(Command(cl))
			root.SetArgs(append([]string{"--no-config-file", "-B", addr, "-X", "dial_timeout=2s", "-X", "retry_timeout=10s", "topic", "add-partitions"}, test.args...))
			r, w, _ := os.Pipe()
			old := os.Stdout
			os.Stdout = w
			execErr := root.Execute()
			w.Close()
			os.Stdout = old
			io.ReadAll(r)

			if test.wantErr != "" {
				if execErr == nil || !strings.Contains(execErr.Error(), test.wantErr) {
					t.Fatalf("err = %v, want containing %q", execErr, test.wantErr)
				}
				return
			}
			if execErr != nil {
				t.Fatal(execErr)
			}
			if got := partitionCount(t, addr, "foo"); got != test.wantParts {
				t.Errorf("partitions = %d, want %d", got, test.wantParts)
			}
		})
	}
}
