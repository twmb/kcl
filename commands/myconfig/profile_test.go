package myconfig

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/twmb/kcl/client"
)

func TestProfileNamesSort(t *testing.T) {
	cfgFile := client.CfgFile{
		Profiles: map[string]client.Cfg{
			"staging":    {SeedBrokers: []string{"staging:9092"}},
			"production": {SeedBrokers: []string{"prod:9092"}},
			"local":      {SeedBrokers: []string{"localhost:9092"}},
		},
	}
	names := profileNames(cfgFile)
	if len(names) != 3 {
		t.Fatalf("expected 3 names, got %d", len(names))
	}
	if names[0] != "local" || names[1] != "production" || names[2] != "staging" {
		t.Errorf("expected sorted names, got %v", names)
	}
}

func TestWriteAndReadCfgFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")

	original := client.CfgFile{
		CurrentProfile: "prod",
		Profiles: map[string]client.Cfg{
			"prod": {
				SeedBrokers:   []string{"kafka-prod:9092"},
				BrokerTimeout: client.Duration(10 * time.Second),
			},
			"local": {
				SeedBrokers:   []string{"localhost:9092"},
				BrokerTimeout: client.Duration(5 * time.Second),
			},
		},
	}

	writeCfgFile(path, original)

	// Read back.
	var loaded client.CfgFile
	_, err := toml.DecodeFile(path, &loaded)
	if err != nil {
		t.Fatalf("unable to decode written config: %v", err)
	}

	if loaded.CurrentProfile != "prod" {
		t.Errorf("current_profile = %q, want prod", loaded.CurrentProfile)
	}
	if len(loaded.Profiles) != 2 {
		t.Fatalf("expected 2 profiles, got %d", len(loaded.Profiles))
	}
	prod := loaded.Profiles["prod"]
	if len(prod.SeedBrokers) != 1 || prod.SeedBrokers[0] != "kafka-prod:9092" {
		t.Errorf("prod seed_brokers = %v", prod.SeedBrokers)
	}
	if prod.BrokerTimeout.D() != 10*time.Second {
		t.Errorf("prod broker_timeout = %v, want 10s", prod.BrokerTimeout.D())
	}
}

func TestBackwardCompatFlatConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")

	// Write a flat config (old format).
	err := os.WriteFile(path, []byte(`
seed_brokers = ["localhost:9092"]
broker_timeout = "5s"
`), 0644)
	if err != nil {
		t.Fatal(err)
	}

	var loaded client.CfgFile
	_, err = toml.DecodeFile(path, &loaded)
	if err != nil {
		t.Fatalf("unable to decode flat config: %v", err)
	}

	// No profiles means flat format.
	if len(loaded.Profiles) != 0 {
		t.Errorf("expected 0 profiles for flat config, got %d", len(loaded.Profiles))
	}
	if len(loaded.SeedBrokers) != 1 || loaded.SeedBrokers[0] != "localhost:9092" {
		t.Errorf("seed_brokers = %v", loaded.SeedBrokers)
	}
}
