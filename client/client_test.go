package client

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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
