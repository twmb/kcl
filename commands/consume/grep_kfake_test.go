package consume

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestGrepFilterWithKfake(t *testing.T) {
	c, err := kfake.NewCluster()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx := context.Background()
	adm := kadm.NewClient(cl)

	_, err = adm.CreateTopic(ctx, 1, 1, nil, "grep-test")
	if err != nil {
		t.Fatal(err)
	}

	// Produce records with keys and headers.
	records := []*kgo.Record{
		{Topic: "grep-test", Key: []byte("user-123"), Value: []byte("login event")},
		{Topic: "grep-test", Key: []byte("user-456"), Value: []byte("error: timeout")},
		{Topic: "grep-test", Key: []byte("user-123"), Value: []byte("logout event"),
			Headers: []kgo.RecordHeader{{Key: "trace-id", Value: []byte("abc-def")}}},
		{Topic: "grep-test", Key: []byte("system"), Value: []byte("warning: low disk")},
	}
	for _, r := range records {
		results := cl.ProduceSync(ctx, r)
		if results.FirstErr() != nil {
			t.Fatalf("produce error: %v", results.FirstErr())
		}
	}

	// Test key filter.
	filters, _ := parseGrepFilters([]string{"k:^user-123$"})
	matches := 0
	for _, r := range records {
		if matchAll(filters, r) {
			matches++
		}
	}
	if matches != 2 {
		t.Errorf("key filter: got %d matches, want 2", matches)
	}

	// Test value filter.
	filters, _ = parseGrepFilters([]string{"v:error"})
	matches = 0
	for _, r := range records {
		if matchAll(filters, r) {
			matches++
		}
	}
	if matches != 1 {
		t.Errorf("value filter: got %d matches, want 1", matches)
	}

	// Test combined filter: key AND negated value.
	filters, _ = parseGrepFilters([]string{"k:^user", "!v:error"})
	matches = 0
	for _, r := range records {
		if matchAll(filters, r) {
			matches++
		}
	}
	if matches != 2 {
		t.Errorf("combined filter: got %d matches, want 2 (user-123 login, user-123 logout)", matches)
	}

	// Test header filter.
	filters, _ = parseGrepFilters([]string{"h:trace-id=abc.*"})
	matches = 0
	for _, r := range records {
		if matchAll(filters, r) {
			matches++
		}
	}
	if matches != 1 {
		t.Errorf("header filter: got %d matches, want 1", matches)
	}
}
