package topic

import (
	"context"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
		code      int
	}{
		{name: "-n", args: []string{"foo", "-n", "2"}, wantParts: 4},
		{name: "-a per partition", args: []string{"foo", "-a", "0", "-a", "0"}, wantParts: 4},
		{name: "-a in one value", args: []string{"foo", "-a", "0:0"}, wantParts: 4},
		{name: "old form", args: []string{"-t", "foo", "0", ":", "0"}, wantParts: 4},
		{name: "--total above", args: []string{"foo", "--total", "5"}, wantParts: 5},
		{name: "--total at is nothing to do", args: []string{"foo", "--total", "2"}, wantParts: 2},
		{name: "--total below fails", args: []string{"foo", "--total", "1"}, wantErr: "has 2 partitions, more than --total 1", code: 1},
		{name: "--total with matching -a", args: []string{"foo", "--total", "4", "-a", "0", "-a", "0"}, wantParts: 4},
		{name: "--total with wrong -a count", args: []string{"foo", "--total", "4", "-a", "0"}, wantErr: "needs 2 new partitions but -a lists 1", code: 2},
		{name: "-n and --total", args: []string{"foo", "-n", "1", "--total", "3"}, wantErr: "exclusive", code: 2},
		{name: "-n and -a", args: []string{"foo", "-n", "1", "-a", "0"}, wantErr: "exclusive", code: 2},
		{name: "nothing", args: []string{"foo"}, wantErr: "nothing to add", code: 2},
		{name: "two topics", args: []string{"foo", "bar", "-n", "1"}, wantErr: "takes one topic", code: 2},
		{name: "wrong replica count", args: []string{"foo", "-a", "0,1"}, wantErr: "each -a must list 1 brokers", code: 2},
		{name: "missing topic is a result row", args: []string{"nosuch", "-n", "1"}, code: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, addr := newCluster(t, 2, 0, "foo")
			got, code := runKcl(t, addr, append([]string{"add-partitions"}, test.args...)...)
			if code != test.code {
				t.Fatalf("exit %d, want %d\n%s", code, test.code, got)
			}
			if test.wantErr != "" {
				return
			}
			if test.code != 0 {
				if !strings.Contains(got, "UNKNOWN_TOPIC_OR_PARTITION") {
					t.Errorf("stdout = %q, want the error row", got)
				}
				return
			}
			if got := partitionCount(t, addr, "foo"); got != test.wantParts {
				t.Errorf("partitions = %d, want %d", got, test.wantParts)
			}
		})
	}
}
