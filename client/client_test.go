package client

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

func TestFormatDefault(t *testing.T) {
	c := &Client{format: "text"}
	if f := c.Format(); f != "text" {
		t.Errorf("Format() = %q, want text", f)
	}
}

func TestFormatJSON(t *testing.T) {
	c := &Client{format: "json"}
	if f := c.Format(); f != "json" {
		t.Errorf("Format() = %q, want json", f)
	}
}

func TestFormatAWK(t *testing.T) {
	c := &Client{format: "awk"}
	if f := c.Format(); f != "awk" {
		t.Errorf("Format() = %q, want awk", f)
	}
}

func TestFormatDumpJSONFallback(t *testing.T) {
	c := &Client{format: "text", asJSON: true}
	if f := c.Format(); f != "json" {
		t.Errorf("Format() with asJSON=true = %q, want json", f)
	}
}

func TestFormatExplicitOverridesDumpJSON(t *testing.T) {
	c := &Client{format: "awk", asJSON: true}
	if f := c.Format(); f != "awk" {
		t.Errorf("Format() with format=awk, asJSON=true = %q, want awk", f)
	}
}

func TestAsJSON(t *testing.T) {
	c := &Client{format: "json"}
	if !c.AsJSON() {
		t.Error("AsJSON() should be true when format=json")
	}
	c2 := &Client{format: "text"}
	if c2.AsJSON() {
		t.Error("AsJSON() should be false when format=text")
	}
}

func TestCfgFileProfileSelection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")

	err := os.WriteFile(path, []byte(`
current_profile = "prod"

[profiles.prod]
seed_brokers = ["kafka-prod:9092"]
broker_timeout = "10s"

[profiles.local]
seed_brokers = ["localhost:9092"]
broker_timeout = "5s"
`), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Test loading prod profile.
	c := &Client{
		cfgPath: path,
		format:  "text",
		cfg: Cfg{
			SeedBrokers:   []string{"default:9092"},
			BrokerTimeout: Duration(time.Second),
		},
	}
	c.parseCfgFile()
	if len(c.cfg.SeedBrokers) != 1 || c.cfg.SeedBrokers[0] != "kafka-prod:9092" {
		t.Errorf("expected prod brokers, got %v", c.cfg.SeedBrokers)
	}
	if c.cfg.BrokerTimeout.D() != 10*time.Second {
		t.Errorf("expected prod broker_timeout 10s, got %v", c.cfg.BrokerTimeout.D())
	}
}

func TestCfgFileProfileOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")

	err := os.WriteFile(path, []byte(`
current_profile = "prod"

[profiles.prod]
seed_brokers = ["kafka-prod:9092"]

[profiles.local]
seed_brokers = ["localhost:9092"]
`), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Test --profile override selects local instead of prod.
	c := &Client{
		cfgPath:     path,
		format:      "text",
		profileName: "local",
		cfg: Cfg{
			SeedBrokers: []string{"default:9092"},
		},
	}
	c.parseCfgFile()
	if len(c.cfg.SeedBrokers) != 1 || c.cfg.SeedBrokers[0] != "localhost:9092" {
		t.Errorf("expected local brokers, got %v", c.cfg.SeedBrokers)
	}
}

func TestCfgFileFlatBackwardCompat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")

	err := os.WriteFile(path, []byte(`
seed_brokers = ["old-broker:9092"]
broker_timeout = "3s"
`), 0644)
	if err != nil {
		t.Fatal(err)
	}

	c := &Client{
		cfgPath: path,
		format:  "text",
		cfg: Cfg{
			SeedBrokers:   []string{"default:9092"},
			BrokerTimeout: Duration(time.Second),
		},
	}
	c.parseCfgFile()
	if len(c.cfg.SeedBrokers) != 1 || c.cfg.SeedBrokers[0] != "old-broker:9092" {
		t.Errorf("expected old-broker, got %v", c.cfg.SeedBrokers)
	}
	if c.cfg.BrokerTimeout.D() != 3*time.Second {
		t.Errorf("expected broker_timeout 3s, got %v", c.cfg.BrokerTimeout.D())
	}
}

func TestCfgFileNoCfgFile(t *testing.T) {
	c := &Client{
		noCfgFile: true,
		format:    "text",
		cfg: Cfg{
			SeedBrokers:   []string{"default:9092"},
			BrokerTimeout: Duration(5 * time.Second),
		},
	}
	c.parseCfgFile()
	// Should not change defaults.
	if c.cfg.SeedBrokers[0] != "default:9092" {
		t.Errorf("noCfgFile should preserve defaults, got %v", c.cfg.SeedBrokers)
	}
}

func TestDurationText(t *testing.T) {
	// UnmarshalText: accepted forms.
	type tc struct {
		in   string
		want time.Duration
		err  bool
	}
	tests := []tc{
		{"5s", 5 * time.Second, false},
		{"500ms", 500 * time.Millisecond, false},
		{"2m30s", 2*time.Minute + 30*time.Second, false},
		{"1h", time.Hour, false},
		{"", 0, true},
		{"notaduration", 0, true},
		{"5", 0, true}, // bare number with no unit
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			var d Duration
			err := d.UnmarshalText([]byte(tt.in))
			if tt.err {
				if err == nil {
					t.Errorf("UnmarshalText(%q) expected error, got %v", tt.in, d.D())
				}
				return
			}
			if err != nil {
				t.Fatalf("UnmarshalText(%q): %v", tt.in, err)
			}
			if d.D() != tt.want {
				t.Errorf("UnmarshalText(%q) = %v, want %v", tt.in, d.D(), tt.want)
			}
		})
	}

	// MarshalText: round-trip.
	rt := []time.Duration{
		time.Second,
		500 * time.Millisecond,
		3*time.Minute + 15*time.Second,
	}
	for _, d := range rt {
		got, err := Duration(d).MarshalText()
		if err != nil {
			t.Fatalf("MarshalText(%v): %v", d, err)
		}
		var back Duration
		if err := back.UnmarshalText(got); err != nil {
			t.Fatalf("round-trip UnmarshalText(%q): %v", got, err)
		}
		if back.D() != d {
			t.Errorf("round-trip %v: text=%q back=%v", d, got, back.D())
		}
	}
}

// TestCfgBootstrapFlagOverridesAll ensures that setting the
// --bootstrap-servers (stored as c.bootstrapServers) wins over
// a profile's seed_brokers after config load.
func TestCfgBootstrapFlagOverridesAll(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/config.toml"
	if err := os.WriteFile(path, []byte(`
current_profile = "prod"

[profiles.prod]
seed_brokers = ["profile:9092"]
`), 0o644); err != nil {
		t.Fatal(err)
	}
	c := &Client{
		cfgPath:          path,
		format:           "text",
		bootstrapServers: []string{"cli-override:9092"},
		cfg:              Cfg{SeedBrokers: []string{"default:9092"}},
	}
	c.parseCfgFile()
	c.processOverrides()
	if len(c.cfg.SeedBrokers) != 1 || c.cfg.SeedBrokers[0] != "cli-override:9092" {
		t.Errorf("expected -B to win, got %v", c.cfg.SeedBrokers)
	}
}

func TestNormCfgKey(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"sasl.user", "sasl_user"},
		{"sasl_user", "sasl_user"}, // legacy underscore form
		{"registry.tls.server_name", "registry_tls_server_name"},
		{"registry_tls_server_name", "registry_tls_server_name"},
		{"SASL.User", "sasl_user"}, // case-insensitive
		{"seed_brokers", "seed_brokers"},
	}
	for _, tt := range tests {
		if got := normCfgKey(tt.in); got != tt.want {
			t.Errorf("normCfgKey(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestCfgOverridesDotAndUnderscore verifies that both the dotted (canonical)
// and legacy underscore -X key forms resolve to the same config fields,
// including the schema registry keys.
func TestCfgOverridesDotAndUnderscore(t *testing.T) {
	c := &Client{
		format: "text",
		flagOverrides: []string{
			"sasl.user=alice",  // dotted
			"sasl_pass=secret", // legacy underscore
			"registry.urls=http://a:8081,http://b:8081",
			"registry.tls.server_name=sr.example.com",
			"registry_user=bob", // legacy underscore for SR
		},
	}
	c.processOverrides()

	if c.cfg.SASL == nil || c.cfg.SASL.User != "alice" || c.cfg.SASL.Pass != "secret" {
		t.Errorf("sasl mismatch: %+v", c.cfg.SASL)
	}
	if c.cfg.SR == nil {
		t.Fatal("expected SR config to be set")
	}
	if want := []string{"http://a:8081", "http://b:8081"}; len(c.cfg.SR.URLs) != 2 || c.cfg.SR.URLs[0] != want[0] || c.cfg.SR.URLs[1] != want[1] {
		t.Errorf("registry urls = %v, want %v", c.cfg.SR.URLs, want)
	}
	if c.cfg.SR.User != "bob" {
		t.Errorf("registry user = %q, want bob", c.cfg.SR.User)
	}
	if c.cfg.SR.TLS == nil || c.cfg.SR.TLS.ServerName != "sr.example.com" {
		t.Errorf("registry tls server_name mismatch: %+v", c.cfg.SR.TLS)
	}
}

// TestRegistryFlagOverridesURLs verifies -R/--registry wins over -X.
func TestRegistryFlagOverridesURLs(t *testing.T) {
	c := &Client{
		format:        "text",
		registryURLs:  []string{"http://flag:8081"},
		flagOverrides: []string{"registry.urls=http://x:8081"},
	}
	c.processOverrides()
	if c.cfg.SR == nil || len(c.cfg.SR.URLs) != 1 || c.cfg.SR.URLs[0] != "http://flag:8081" {
		t.Errorf("expected -R to win, got %+v", c.cfg.SR)
	}
}

func TestSchemaRegistryClientAuthConflict(t *testing.T) {
	c := &Client{
		format:    "text",
		noCfgFile: true,
		cfg: Cfg{SR: &CfgSR{
			URLs:        []string{"http://x:8081"},
			User:        "u",
			BearerToken: "t",
		}},
	}
	if _, err := c.SchemaRegistryClient(); err == nil {
		t.Fatal("expected error: bearer token and basic auth are mutually exclusive")
	}
}

func TestSchemaRegistryClientDefaultURL(t *testing.T) {
	// No SR config -> should still build a client (defaults to localhost:8081).
	c := &Client{format: "text", noCfgFile: true}
	if _, err := c.SchemaRegistryClient(); err != nil {
		t.Fatalf("expected default-localhost client, got error: %v", err)
	}
}

func TestStrnorm(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"ScramSha256", "scramsha256"},
		{"SCRAM-SHA-256", "scramsha256"},
		{"scram_sha_256", "scramsha256"},
		{"  Plain  ", "plain"},
		{"AWS_MSK_IAM", "awsmskiam"},
	}
	for _, tt := range tests {
		got := Strnorm(tt.in)
		if got != tt.want {
			t.Errorf("Strnorm(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestCfgEncodeOmitsZeroDurations(t *testing.T) {
	var buf bytes.Buffer
	err := toml.NewEncoder(&buf).Encode(CfgFile{
		CurrentProfile: "p",
		Profiles: map[string]Cfg{
			"p": {SeedBrokers: []string{"a:9092"}, DialTimeout: Duration(2 * time.Second)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if strings.Contains(got, "0s") || strings.Count(got, "_timeout") != 1 || !strings.Contains(got, `dial_timeout = "2s"`) {
		t.Errorf("unexpected encoding:\n%s", got)
	}
}
