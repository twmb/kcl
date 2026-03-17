package consume

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestGrepFilterKey(t *testing.T) {
	filters, err := parseGrepFilters([]string{"k:^user-123$"})
	if err != nil {
		t.Fatal(err)
	}

	match := matchAll(filters, &kgo.Record{Key: []byte("user-123")})
	if !match {
		t.Error("expected match for exact key")
	}

	match = matchAll(filters, &kgo.Record{Key: []byte("user-456")})
	if match {
		t.Error("expected no match for different key")
	}
}

func TestGrepFilterValue(t *testing.T) {
	filters, err := parseGrepFilters([]string{"v:error"})
	if err != nil {
		t.Fatal(err)
	}

	match := matchAll(filters, &kgo.Record{Value: []byte("an error occurred")})
	if !match {
		t.Error("expected match for value containing error")
	}

	match = matchAll(filters, &kgo.Record{Value: []byte("all good")})
	if match {
		t.Error("expected no match for value without error")
	}
}

func TestGrepFilterNegate(t *testing.T) {
	filters, err := parseGrepFilters([]string{"!v:warning"})
	if err != nil {
		t.Fatal(err)
	}

	match := matchAll(filters, &kgo.Record{Value: []byte("an error")})
	if !match {
		t.Error("expected match for value without warning")
	}

	match = matchAll(filters, &kgo.Record{Value: []byte("a warning here")})
	if match {
		t.Error("expected no match for value containing warning")
	}
}

func TestGrepFilterHeaderKey(t *testing.T) {
	filters, err := parseGrepFilters([]string{"hk:trace"})
	if err != nil {
		t.Fatal(err)
	}

	r := &kgo.Record{
		Headers: []kgo.RecordHeader{
			{Key: "trace-id", Value: []byte("abc")},
		},
	}
	if !matchAll(filters, r) {
		t.Error("expected match for header key matching trace")
	}

	r2 := &kgo.Record{
		Headers: []kgo.RecordHeader{
			{Key: "content-type", Value: []byte("json")},
		},
	}
	if matchAll(filters, r2) {
		t.Error("expected no match for header key not matching trace")
	}
}

func TestGrepFilterHeaderValue(t *testing.T) {
	filters, err := parseGrepFilters([]string{"hv:^abc"})
	if err != nil {
		t.Fatal(err)
	}

	r := &kgo.Record{
		Headers: []kgo.RecordHeader{
			{Key: "trace-id", Value: []byte("abc123")},
		},
	}
	if !matchAll(filters, r) {
		t.Error("expected match for header value starting with abc")
	}
}

func TestGrepFilterSpecificHeader(t *testing.T) {
	filters, err := parseGrepFilters([]string{"h:trace-id=abc.*"})
	if err != nil {
		t.Fatal(err)
	}

	r := &kgo.Record{
		Headers: []kgo.RecordHeader{
			{Key: "trace-id", Value: []byte("abc123")},
		},
	}
	if !matchAll(filters, r) {
		t.Error("expected match for specific header")
	}

	r2 := &kgo.Record{
		Headers: []kgo.RecordHeader{
			{Key: "other-id", Value: []byte("abc123")},
		},
	}
	if matchAll(filters, r2) {
		t.Error("expected no match for wrong header name")
	}
}

func TestGrepFilterTopic(t *testing.T) {
	filters, err := parseGrepFilters([]string{`t:^logs\.`})
	if err != nil {
		t.Fatal(err)
	}

	if !matchAll(filters, &kgo.Record{Topic: "logs.app"}) {
		t.Error("expected match for topic logs.app")
	}
	if matchAll(filters, &kgo.Record{Topic: "events"}) {
		t.Error("expected no match for topic events")
	}
}

func TestGrepFilterMultipleAND(t *testing.T) {
	filters, err := parseGrepFilters([]string{"v:error", "!v:warning"})
	if err != nil {
		t.Fatal(err)
	}

	// matches both: has error, no warning
	if !matchAll(filters, &kgo.Record{Value: []byte("an error occurred")}) {
		t.Error("expected match: has error, no warning")
	}

	// fails first: no error
	if matchAll(filters, &kgo.Record{Value: []byte("all good")}) {
		t.Error("expected no match: no error")
	}

	// fails second: has both error and warning
	if matchAll(filters, &kgo.Record{Value: []byte("error warning")}) {
		t.Error("expected no match: has both error and warning")
	}
}

func TestGrepFilterEmpty(t *testing.T) {
	filters, err := parseGrepFilters(nil)
	if err != nil {
		t.Fatal(err)
	}

	// No filters = match everything.
	if !matchAll(filters, &kgo.Record{Value: []byte("anything")}) {
		t.Error("no filters should match everything")
	}
}

func TestGrepFilterErrors(t *testing.T) {
	tests := []string{
		"invalid",      // no prefix
		"k:[invalid",   // bad regex
		"h:noequals",   // h: without =
		"x:something",  // unknown prefix
	}
	for _, p := range tests {
		_, err := parseGrepFilters([]string{p})
		if err == nil {
			t.Errorf("expected error for %q", p)
		}
	}
}
