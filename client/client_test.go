package client

import (
	"os"
	"path/filepath"
	"testing"
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
timeout_ms = 10000

[profiles.local]
seed_brokers = ["localhost:9092"]
timeout_ms = 5000
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
			TimeoutMillis: 1000,
		},
	}
	c.parseCfgFile()
	if len(c.cfg.SeedBrokers) != 1 || c.cfg.SeedBrokers[0] != "kafka-prod:9092" {
		t.Errorf("expected prod brokers, got %v", c.cfg.SeedBrokers)
	}
	if c.cfg.TimeoutMillis != 10000 {
		t.Errorf("expected prod timeout 10000, got %d", c.cfg.TimeoutMillis)
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
timeout_ms = 3000
`), 0644)
	if err != nil {
		t.Fatal(err)
	}

	c := &Client{
		cfgPath: path,
		format:  "text",
		cfg: Cfg{
			SeedBrokers:   []string{"default:9092"},
			TimeoutMillis: 1000,
		},
	}
	c.parseCfgFile()
	if len(c.cfg.SeedBrokers) != 1 || c.cfg.SeedBrokers[0] != "old-broker:9092" {
		t.Errorf("expected old-broker, got %v", c.cfg.SeedBrokers)
	}
	if c.cfg.TimeoutMillis != 3000 {
		t.Errorf("expected timeout 3000, got %d", c.cfg.TimeoutMillis)
	}
}

func TestCfgFileNoCfgFile(t *testing.T) {
	c := &Client{
		noCfgFile: true,
		format:    "text",
		cfg: Cfg{
			SeedBrokers:   []string{"default:9092"},
			TimeoutMillis: 5000,
		},
	}
	c.parseCfgFile()
	// Should not change defaults.
	if c.cfg.SeedBrokers[0] != "default:9092" {
		t.Errorf("noCfgFile should preserve defaults, got %v", c.cfg.SeedBrokers)
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
