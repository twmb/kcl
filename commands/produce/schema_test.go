package produce

import (
	"testing"

	"github.com/twmb/kcl/serde"
)

func TestParseSchemaSpec(t *testing.T) {
	tests := []struct {
		in   string
		want serde.Spec
	}{
		{"topic", serde.Spec{Topic: true}},
		{"topic@3", serde.Spec{Topic: true, Version: "3"}},
		{"topic@latest", serde.Spec{Topic: true, Version: "latest"}},
		{"topic#com.acme.Order", serde.Spec{Topic: true, Message: "com.acme.Order"}},
		{"orders-value", serde.Spec{Subject: "orders-value"}},
		{"orders-value@2", serde.Spec{Subject: "orders-value", Version: "2"}},
		{"subject:orders-value@2", serde.Spec{Subject: "orders-value", Version: "2"}},
		{"subject:weird:name@5", serde.Spec{Subject: "weird:name", Version: "5"}},
		{"id:42", serde.Spec{ID: 42}},
		{"id:9#Order", serde.Spec{ID: 9, Message: "Order"}},
		{"42", serde.Spec{Subject: "42"}},             // bare number is a subject, not an id
		{"topicfoo", serde.Spec{Subject: "topicfoo"}}, // only exact "topic" is the keyword
	}
	for _, tt := range tests {
		got, err := parseSchemaSpec(tt.in)
		if err != nil {
			t.Errorf("parseSchemaSpec(%q): unexpected error %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("parseSchemaSpec(%q) = %+v, want %+v", tt.in, got, tt.want)
		}
	}

	for _, bad := range []string{"", "id:", "id:abc", "id:0", "id:-1", "subject:"} {
		if _, err := parseSchemaSpec(bad); err == nil {
			t.Errorf("parseSchemaSpec(%q): expected error", bad)
		}
	}
}
