package fake

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
)

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		in   string
		want kfake.LogLevel
		err  bool
	}{
		{"", kfake.LogLevelNone, false},
		{"none", kfake.LogLevelNone, false},
		{"NONE", kfake.LogLevelNone, false},
		{"error", kfake.LogLevelError, false},
		{"warn", kfake.LogLevelWarn, false},
		{"info", kfake.LogLevelInfo, false},
		{"debug", kfake.LogLevelDebug, false},
		{"DEBUG", kfake.LogLevelDebug, false},
		{"trace", 0, true},
		{"verbose", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseLogLevel(tt.in)
			if tt.err {
				if err == nil {
					t.Errorf("parseLogLevel(%q) expected error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLogLevel(%q) unexpected error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("parseLogLevel(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseBrokerConfigs(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want map[string]string
		err  bool
	}{
		{"nil", nil, nil, false},
		{"empty", []string{}, nil, false},
		{"single", []string{"foo=bar"}, map[string]string{"foo": "bar"}, false},
		{"multiple", []string{"foo=bar", "baz=qux"}, map[string]string{"foo": "bar", "baz": "qux"}, false},
		{"empty value", []string{"foo="}, map[string]string{"foo": ""}, false},
		{"embedded equals", []string{"foo=a=b=c"}, map[string]string{"foo": "a=b=c"}, false},
		{"no equals", []string{"foo"}, nil, true},
		{"mixed valid+invalid", []string{"foo=bar", "baz"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBrokerConfigs(tt.in)
			if tt.err {
				if err == nil {
					t.Errorf("expected error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseSeedTopics(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []seedTopic
		err  bool
	}{
		{"nil", nil, nil, false},
		{"bare name", []string{"foo"}, []seedTopic{{"foo", -1}}, false},
		{"name:partitions", []string{"foo:3"}, []seedTopic{{"foo", 3}}, false},
		{"multiple repeatable", []string{"foo:3", "bar:2"}, []seedTopic{{"foo", 3}, {"bar", 2}}, false},
		{"non-int partitions", []string{"foo:abc"}, nil, true},
		{"zero partitions", []string{"foo:0"}, nil, true},
		{"negative partitions", []string{"foo:-1"}, nil, true},
		{"missing partition count", []string{"foo:"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSeedTopics(tt.in)
			if tt.err {
				if err == nil {
					t.Errorf("expected error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseSASLUsers(t *testing.T) {
	// Set predictable env vars for the env-expansion test.
	t.Setenv("TEST_KCL_USER", "alice")
	t.Setenv("TEST_KCL_PASS", "secret")
	t.Setenv("TEST_KCL_EMPTY", "")

	tests := []struct {
		name string
		in   []string
		want []saslUser
		err  string
	}{
		{
			name: "plain literal",
			in:   []string{"plain:alice:pw"},
			want: []saslUser{{"PLAIN", "alice", "pw"}},
		},
		{
			name: "uppercase mechanism accepted",
			in:   []string{"PLAIN:alice:pw"},
			want: []saslUser{{"PLAIN", "alice", "pw"}},
		},
		{
			name: "scram-sha-256",
			in:   []string{"scram-sha-256:bob:pw"},
			want: []saslUser{{"SCRAM-SHA-256", "bob", "pw"}},
		},
		{
			name: "scram-sha-512",
			in:   []string{"scram-sha-512:bob:pw"},
			want: []saslUser{{"SCRAM-SHA-512", "bob", "pw"}},
		},
		{
			name: "env expansion on user and pass",
			in:   []string{"plain:$TEST_KCL_USER:$TEST_KCL_PASS"},
			want: []saslUser{{"PLAIN", "alice", "secret"}},
		},
		{
			name: "env expansion with ${braces}",
			in:   []string{"plain:${TEST_KCL_USER}:${TEST_KCL_PASS}"},
			want: []saslUser{{"PLAIN", "alice", "secret"}},
		},
		{
			name: "multiple entries",
			in:   []string{"plain:admin:adminpw", "scram-sha-256:user:userpw"},
			want: []saslUser{
				{"PLAIN", "admin", "adminpw"},
				{"SCRAM-SHA-256", "user", "userpw"},
			},
		},
		{
			name: "unknown mechanism",
			in:   []string{"bogus:a:b"},
			err:  "mechanism",
		},
		{
			name: "too few fields",
			in:   []string{"plain:alice"},
			err:  "MECHANISM:USER:PASS",
		},
		{
			name: "empty user after expansion",
			in:   []string{"plain:$TEST_KCL_EMPTY:pw"},
			err:  "non-empty after env expansion",
		},
		{
			name: "empty password after expansion",
			in:   []string{"plain:alice:$TEST_KCL_EMPTY"},
			err:  "non-empty after env expansion",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSASLUsers(tt.in)
			if tt.err != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil (result=%v)", tt.err, got)
				}
				if !strings.Contains(err.Error(), tt.err) {
					t.Errorf("expected error containing %q, got %v", tt.err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestSASLEnvIndependence guards against a past bug where we
// shadowed os.Environ during testing; just confirms that env var
// expansion picks up the real process env.
func TestSASLEnvIndependence(t *testing.T) {
	// Skip if the user happens to have these set in their shell.
	if os.Getenv("KCLFAKETEST_X") != "" || os.Getenv("KCLFAKETEST_Y") != "" {
		t.Skip("KCLFAKETEST_X or _Y already set in env; skipping")
	}
	t.Setenv("KCLFAKETEST_X", "u")
	t.Setenv("KCLFAKETEST_Y", "p")
	got, err := parseSASLUsers([]string{"plain:$KCLFAKETEST_X:$KCLFAKETEST_Y"})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if got[0].user != "u" || got[0].pass != "p" {
		t.Errorf("env expansion failed: %+v", got[0])
	}
}
