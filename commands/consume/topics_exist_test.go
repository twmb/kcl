package consume

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/out"
)

func TestCheckTopicsExist(t *testing.T) {
	c, err := kfake.NewCluster(kfake.SeedTopics(1, "exists"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, test := range []struct {
		name    string
		topics  []string
		wantErr string
	}{
		{name: "present", topics: []string{"exists"}},
		{name: "missing", topics: []string{"nope"}, wantErr: `unable to consume topic "nope"`},
		{name: "one of two missing", topics: []string{"exists", "nope"}, wantErr: `"nope"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := checkTopicsExist(ctx, cl, test.topics)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			var ce *out.ExitCodeError
			if err == nil || !strings.Contains(err.Error(), test.wantErr) || !errors.As(err, &ce) || ce.Code != out.ExitError {
				t.Fatalf("err = %v, want exit %d containing %q", err, out.ExitError, test.wantErr)
			}
		})
	}
}
