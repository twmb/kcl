package client

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"reflect"
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
			BrokerTimeout: Dur(time.Second),
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
			BrokerTimeout: Dur(time.Second),
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
			BrokerTimeout: Dur(5 * time.Second),
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

func TestCfgFileMissingIsEmpty(t *testing.T) {
	c := &Client{
		cfgPath: filepath.Join(t.TempDir(), "missing", "config.toml"),
		format:  "text",
		cfg:     Cfg{SeedBrokers: []string{"default:9092"}},
	}
	c.parseCfgFile()
	c.processOverrides()
	if got := c.cfg.SeedBrokers; len(got) != 1 || got[0] != "default:9092" {
		t.Errorf("missing config file should preserve defaults, got %v", got)
	}
}

func TestFlagCfg(t *testing.T) {
	for _, test := range []struct {
		name      string
		flags     []string
		bootstrap []string
		registry  []string
		env       map[string]string
		want      Cfg
		wantErr   bool
	}{
		{
			name: "nothing given is the defaults",
			want: defaultCfg(),
		},
		{
			name:      "bootstrap shorthand",
			bootstrap: []string{"a:9092", "b:9092"},
			want:      Cfg{SeedBrokers: []string{"a:9092", "b:9092"}, BrokerTimeout: Dur(5 * time.Second)},
		},
		{
			name:      "bootstrap wins over -X seed_brokers",
			flags:     []string{"seed_brokers=x:9092"},
			bootstrap: []string{"a:9092"},
			want:      Cfg{SeedBrokers: []string{"a:9092"}, BrokerTimeout: Dur(5 * time.Second)},
		},
		{
			name:  "tls and sasl, dotted and legacy underscore",
			flags: []string{"tls.ca_cert_path=/ca.pem", "sasl.method=scram-sha-256", "sasl_user=alice", "dial_timeout=2s", "broker_timeout=1s"},
			want: Cfg{
				SeedBrokers:   []string{"localhost:9092"},
				BrokerTimeout: Dur(time.Second),
				DialTimeout:   Dur(2 * time.Second),
				TLS:           &CfgTLS{CACert: "/ca.pem"},
				SASL:          &CfgSASL{Method: "scram-sha-256", User: "alice"},
			},
		},
		{
			name:     "registry shorthand",
			registry: []string{"http://sr:8081"},
			want:     Cfg{SeedBrokers: []string{"localhost:9092"}, BrokerTimeout: Dur(5 * time.Second), SR: &CfgSR{URLs: []string{"http://sr:8081"}}},
		},
		{
			name:      "environment is ignored",
			env:       map[string]string{"KCL_SASL_PASS": "secret", "KCL_SEED_BROKERS": "env:9092"},
			bootstrap: []string{"a:9092"},
			want:      Cfg{SeedBrokers: []string{"a:9092"}, BrokerTimeout: Dur(5 * time.Second)},
		},
		{
			name:    "unknown key",
			flags:   []string{"nope=1"},
			wantErr: true,
		},
		{
			name:    "bad duration",
			flags:   []string{"dial_timeout=soon"},
			wantErr: true,
		},
		{
			name:    "missing equals",
			flags:   []string{"seed_brokers"},
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			for k, v := range test.env {
				t.Setenv(k, v)
			}
			c := &Client{
				envPfx:           "KCL_",
				flagOverrides:    test.flags,
				bootstrapServers: test.bootstrap,
				registryURLs:     test.registry,
				cfg:              Cfg{SeedBrokers: []string{"default:9092"}},
			}
			got, err := c.FlagCfg()
			if (err != nil) != test.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, test.wantErr)
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("got %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestCfgEncodeOmitsZeroDurations(t *testing.T) {
	var buf bytes.Buffer
	err := toml.NewEncoder(&buf).Encode(CfgFile{
		CurrentProfile: "p",
		Profiles: map[string]Cfg{
			"p": {SeedBrokers: []string{"a:9092"}, DialTimeout: Dur(2 * time.Second)},
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

func TestApplyFlagsKeysAndPreservation(t *testing.T) {
	c := &Client{
		flagOverrides:    []string{"sasl.user=me", "sasl_pass=pw"},
		bootstrapServers: []string{"a:9092"},
		registryURLs:     []string{"http://sr:8081"},
	}
	cfg := Cfg{SeedBrokers: []string{"old:9092"}, BrokerTimeout: Dur(10 * time.Second)}
	keys, err := c.ApplyFlags(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"sasl.user", "sasl_pass", "seed_brokers", "registry.urls"}; !reflect.DeepEqual(keys, want) {
		t.Errorf("keys = %v, want %v", keys, want)
	}
	if cfg.BrokerTimeout.D() != 10*time.Second || cfg.SeedBrokers[0] != "a:9092" || cfg.SASL.Pass != "pw" || cfg.SR.URLs[0] != "http://sr:8081" {
		t.Errorf("cfg = %+v sasl=%+v sr=%+v", cfg, cfg.SASL, cfg.SR)
	}
	if keys, err := (&Client{}).ApplyFlags(&Cfg{}); err != nil || len(keys) != 0 {
		t.Errorf("no flags: keys=%v err=%v", keys, err)
	}
}

// TestCfgFileLaysOverDefaults pins that a config file only changes the keys
// it has: a profile without broker_timeout keeps the 5s default, one written
// as "0s" is zero, and top level keys do not leak into a selected profile.
func TestCfgFileLaysOverDefaults(t *testing.T) {
	for _, test := range []struct {
		name        string
		file        string
		profile     string
		wantBrokers string
		wantTimeout time.Duration
	}{
		{
			name:        "profile without timeout keeps default",
			file:        "current_profile = \"p\"\n[profiles.p]\nseed_brokers = [\"p:9092\"]\n",
			wantBrokers: "p:9092",
			wantTimeout: 5 * time.Second,
		},
		{
			name:        "profile written as zero is zero",
			file:        "current_profile = \"p\"\n[profiles.p]\nbroker_timeout = \"0s\"\n",
			wantBrokers: "localhost:9092",
			wantTimeout: 0,
		},
		{
			name:        "top level keys do not leak into a profile",
			file:        "current_profile = \"p\"\nbroker_timeout = \"3s\"\n[profiles.p]\nseed_brokers = [\"p:9092\"]\n",
			wantBrokers: "p:9092",
			wantTimeout: 5 * time.Second,
		},
		{
			name:        "flat file without timeout keeps default",
			file:        "seed_brokers = [\"flat:9092\"]\n",
			wantBrokers: "flat:9092",
			wantTimeout: 5 * time.Second,
		},
		{
			name:        "flat file written as zero is zero",
			file:        "broker_timeout = \"0s\"\n",
			wantBrokers: "localhost:9092",
			wantTimeout: 0,
		},
		{
			name:        "-C selects and still lays over defaults",
			file:        "current_profile = \"a\"\n[profiles.a]\nbroker_timeout = \"1s\"\n[profiles.b]\nseed_brokers = [\"b:9092\"]\n",
			profile:     "b",
			wantBrokers: "b:9092",
			wantTimeout: 5 * time.Second,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(path, []byte(test.file), 0o644); err != nil {
				t.Fatal(err)
			}
			c := &Client{cfgPath: path, format: "text", profileName: test.profile, cfg: defaultCfg()}
			c.parseCfgFile()
			c.processOverrides()
			if len(c.cfg.SeedBrokers) != 1 || c.cfg.SeedBrokers[0] != test.wantBrokers {
				t.Errorf("seed_brokers = %v, want [%s]", c.cfg.SeedBrokers, test.wantBrokers)
			}
			if got := c.cfg.BrokerTimeout.D(); got != test.wantTimeout {
				t.Errorf("broker_timeout = %v, want %v", got, test.wantTimeout)
			}
		})
	}
}

func TestApplyCfgOptsUnsetAndBools(t *testing.T) {
	tlsOn := func() Cfg { return Cfg{TLS: &CfgTLS{InsecureSkipVerify: true, CACert: "/ca"}} }
	sasl := func() Cfg { return Cfg{SASL: &CfgSASL{Method: "plain", User: "u", Pass: "p"}} }
	for _, test := range []struct {
		name    string
		start   Cfg
		opts    []string
		want    Cfg
		wantErr string
	}{
		{name: "false does not create the tls table", opts: []string{"tls.insecure=false"}, want: Cfg{}},
		{name: "false on an existing table", start: tlsOn(), opts: []string{"tls.insecure=false"}, want: Cfg{TLS: &CfgTLS{CACert: "/ca"}}},
		{name: "bare boolean means true", opts: []string{"tls.insecure"}, want: Cfg{TLS: &CfgTLS{InsecureSkipVerify: true}}},
		{name: "empty boolean unsets", start: tlsOn(), opts: []string{"tls.insecure="}, want: Cfg{TLS: &CfgTLS{CACert: "/ca"}}},
		{name: "bad boolean", opts: []string{"tls.insecure=maybe"}, wantErr: "invalid boolean"},
		{name: "empty string unsets and keeps the table", start: sasl(), opts: []string{"sasl.user="}, want: Cfg{SASL: &CfgSASL{Method: "plain", Pass: "p"}}},
		{name: "unsetting in an absent table stays absent", opts: []string{"sasl.user="}, want: Cfg{}},
		{name: "table removal", start: sasl(), opts: []string{"sasl="}, want: Cfg{}},
		{name: "table with a value", opts: []string{"sasl=x"}, wantErr: "is a table"},
		{name: "slice unset", start: Cfg{SeedBrokers: []string{"a:1"}}, opts: []string{"seed_brokers="}, want: Cfg{}},
		{name: "slice with an empty element", opts: []string{"seed_brokers=a,,b"}, wantErr: "invalid empty value"},
		{name: "duration unset", start: Cfg{BrokerTimeout: Dur(time.Second)}, opts: []string{"broker_timeout="}, want: Cfg{}},
		{name: "duration zero is set", opts: []string{"broker_timeout=0s"}, want: Cfg{BrokerTimeout: Dur(0)}},
		{name: "use_tls bare", opts: []string{"use_tls"}, want: Cfg{TLS: &CfgTLS{}}},
		{name: "use_tls false removes the table", start: tlsOn(), opts: []string{"use_tls=false"}, want: Cfg{}},
		{name: "registry tls removal keeps the registry", start: Cfg{SR: &CfgSR{URLs: []string{"http://sr"}, TLS: &CfgTLS{CACert: "/ca"}}}, opts: []string{"registry.tls="}, want: Cfg{SR: &CfgSR{URLs: []string{"http://sr"}}}},
		{name: "registry tls key creates both tables", opts: []string{"registry.tls.insecure"}, want: Cfg{SR: &CfgSR{TLS: &CfgTLS{InsecureSkipVerify: true}}}},
		{name: "bare non-boolean", opts: []string{"sasl.user"}, wantErr: "needs a value; sasl.user= unsets it"},
		{name: "unknown key points at profile keys", opts: []string{"sasl.usr=me"}, wantErr: "kcl -X help"},
		{name: "legacy underscore form", opts: []string{"sasl_user=me"}, want: Cfg{SASL: &CfgSASL{User: "me"}}},
		{name: "renamed timeout_ms still explains itself", opts: []string{"timeout_ms=5000"}, wantErr: "renamed to broker_timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := test.start
			err := ApplyCfgOpts(&cfg, test.opts)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cfg, test.want) {
				t.Errorf("got %+v tls=%+v sasl=%+v sr=%+v\nwant %+v", cfg, cfg.TLS, cfg.SASL, cfg.SR, test.want)
			}
		})
	}
}

func TestCfgKeysDescribed(t *testing.T) {
	seen := make(map[string]bool)
	for _, k := range CfgKeys() {
		if k.Desc == "" {
			t.Errorf("key %q has no description", k.Name)
		}
		if seen[normCfgKey(k.Name)] {
			t.Errorf("key %q collides with another after normalization", k.Name)
		}
		seen[normCfgKey(k.Name)] = true
	}
	if !seen["sasl_user"] || !seen["registry_tls_ca_cert_path"] || seen["timeout_ms"] {
		t.Errorf("unexpected key set: %v", seen)
	}
}

// TestEnvSkipsTableKeys pins that KCL_TLS or KCL_SASL in the environment,
// which would only ever be a mistake, is not read as a table removal.
func TestEnvSkipsTableKeys(t *testing.T) {
	t.Setenv("KCL_TLS", "1")
	t.Setenv("KCL_SASL", "1")
	t.Setenv("KCL_SASL_USER", "env")
	c := &Client{envPfx: "KCL_", format: "text", cfg: Cfg{TLS: &CfgTLS{CACert: "/ca"}}}
	c.processOverrides()
	if c.cfg.TLS == nil || c.cfg.SASL == nil || c.cfg.SASL.User != "env" {
		t.Errorf("cfg tls=%+v sasl=%+v", c.cfg.TLS, c.cfg.SASL)
	}
}

func TestExpandEnvRefs(t *testing.T) {
	t.Setenv("KCL_TEST_PASS", "s3cret")
	t.Setenv("KCL_TEST_HOST", "kafka.internal")
	for _, test := range []struct {
		name    string
		cfg     Cfg
		want    Cfg
		wantErr string
	}{
		{
			name: "plain values untouched, including a lone dollar",
			cfg:  Cfg{SASL: &CfgSASL{Pass: "a$b$$c"}},
			want: Cfg{SASL: &CfgSASL{Pass: "a$b$$c"}},
		},
		{
			name: "reference in a nested table",
			cfg:  Cfg{SASL: &CfgSASL{User: "me", Pass: "${KCL_TEST_PASS}"}},
			want: Cfg{SASL: &CfgSASL{User: "me", Pass: "s3cret"}},
		},
		{
			name: "reference inside a list element and text",
			cfg:  Cfg{SeedBrokers: []string{"${KCL_TEST_HOST}:9092", "b:9092"}},
			want: Cfg{SeedBrokers: []string{"kafka.internal:9092", "b:9092"}},
		},
		{
			name: "two references and an escape",
			cfg:  Cfg{SR: &CfgSR{BearerToken: "${KCL_TEST_PASS}-${KCL_TEST_PASS}", Context: "$${KCL_TEST_PASS}"}},
			want: Cfg{SR: &CfgSR{BearerToken: "s3cret-s3cret", Context: "${KCL_TEST_PASS}"}},
		},
		{
			name: "registry tls path",
			cfg:  Cfg{SR: &CfgSR{TLS: &CfgTLS{CACert: "/etc/${KCL_TEST_HOST}/ca.pem"}}},
			want: Cfg{SR: &CfgSR{TLS: &CfgTLS{CACert: "/etc/kafka.internal/ca.pem"}}},
		},
		{
			name: "not an identifier is left alone",
			cfg:  Cfg{SASL: &CfgSASL{Pass: "${not-a-name}"}},
			want: Cfg{SASL: &CfgSASL{Pass: "${not-a-name}"}},
		},
		{
			name:    "missing variable is an error",
			cfg:     Cfg{SASL: &CfgSASL{Pass: "${KCL_TEST_DEFINITELY_UNSET}"}},
			wantErr: "${KCL_TEST_DEFINITELY_UNSET}, which is not set",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := test.cfg
			err := expandEnvRefs(&cfg)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cfg, test.want) {
				t.Errorf("got sasl=%+v sr=%+v brokers=%v", cfg.SASL, cfg.SR, cfg.SeedBrokers)
			}
		})
	}
}

// TestLoadCfgExpandsFileAndFlags pins that references are expanded after
// the file, environment, and flags are combined, so every source is treated
// the same way.
func TestLoadCfgExpandsFileAndFlags(t *testing.T) {
	t.Setenv("KCL_TEST_PASS", "s3cret")
	t.Setenv("KCL_TEST_USER", "alice")
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte("[sasl]\npass = \"${KCL_TEST_PASS}\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := &Client{cfgPath: path, format: "text", envPfx: "KCL_", flagOverrides: []string{"sasl.user=${KCL_TEST_USER}"}, cfg: defaultCfg()}
	c.loadCfg()
	if c.cfg.SASL == nil || c.cfg.SASL.Pass != "s3cret" || c.cfg.SASL.User != "alice" {
		t.Errorf("sasl = %+v", c.cfg.SASL)
	}
}

func TestXListAndHelpCoverEveryKey(t *testing.T) {
	list := XList()
	help := XHelp()
	for _, k := range CfgKeys() {
		line := k.Name + "=" + k.Example
		if !strings.Contains(list, line+"\n") {
			t.Errorf("-X list lacks %q", line)
		}
		if !strings.Contains(help, "\n"+line+"\n  ") {
			t.Errorf("-X help lacks %q followed by an indented description", line)
		}
	}
	if strings.Contains(list, "timeout_ms") || strings.Contains(help, "timeout_ms=") {
		t.Error("the renamed timeout_ms is listed")
	}
	if !strings.Contains(list, "sasl=\n") {
		t.Error("a table key should print as NAME= with nothing after")
	}
	for _, line := range strings.Split(help, "\n") {
		if len(line) > 80 {
			t.Errorf("help line over 80 columns: %q", line)
		}
	}
	if got := wrap("a bb ccc dddd", 8, "  "); got != "  a bb\n  ccc\n  dddd\n" {
		t.Errorf("wrap = %q", got)
	}
}

func TestMaybeXHelp(t *testing.T) {
	capture := func(f func() bool) (string, bool) {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		old := os.Stdout
		os.Stdout = w
		ok := f()
		w.Close()
		os.Stdout = old
		b, _ := io.ReadAll(r)
		return string(b), ok
	}
	for _, test := range []struct {
		name   string
		flags  []string
		format string
		want   string // substring of stdout
		wantOK bool
	}{
		{name: "nothing", flags: []string{"sasl.user=me"}, format: "text"},
		{name: "help", flags: []string{"help"}, format: "text", want: "\nsasl.pass=${KAFKA_PASS}\n  Password.", wantOK: true},
		{name: "list", flags: []string{"sasl.user=me", "list"}, format: "text", want: "sasl.user=alice\n", wantOK: true},
		{name: "help as json", flags: []string{"help"}, format: "json", want: `"key": "tls.insecure"`, wantOK: true},
		{name: "list as awk", flags: []string{"list"}, format: "awk", want: "tls.insecure\tbool\ttrue\t", wantOK: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := &Client{flagOverrides: test.flags, format: test.format}
			got, ok := capture(c.MaybeXHelp)
			if ok != test.wantOK {
				t.Fatalf("ok = %v, want %v", ok, test.wantOK)
			}
			if !strings.Contains(got, test.want) {
				t.Errorf("stdout lacks %q:\n%s", test.want, got)
			}
		})
	}
}

func TestDiskCfgKeepsReferences(t *testing.T) {
	t.Setenv("KCL_TEST_PASS", "s3cret")
	c := &Client{noCfgFile: true, format: "text", envPfx: "KCL_", flagOverrides: []string{"sasl.method=plain", "sasl.pass=${KCL_TEST_PASS}", "seed_brokers=${KCL_TEST_PASS}.example:1"}, cfg: defaultCfg()}
	c.loadCfg()
	if c.cfg.SASL.Pass != "s3cret" || c.cfg.SeedBrokers[0] != "s3cret.example:1" {
		t.Errorf("running cfg not expanded: %+v %v", c.cfg.SASL, c.cfg.SeedBrokers)
	}
	if c.cfgWritten.SASL.Pass != "${KCL_TEST_PASS}" || c.cfgWritten.SeedBrokers[0] != "${KCL_TEST_PASS}.example:1" || c.cfgWritten.SASL.Method != "plain" {
		t.Errorf("written cfg changed: %+v %v", c.cfgWritten.SASL, c.cfgWritten.SeedBrokers)
	}
}

func TestCfgCloneIsDeep(t *testing.T) {
	orig := Cfg{
		SeedBrokers:   []string{"a:1"},
		BrokerTimeout: Dur(time.Second),
		TLS:           &CfgTLS{CACert: "/ca", CipherSuites: []string{"x"}},
		SASL:          &CfgSASL{User: "u"},
		SR:            &CfgSR{URLs: []string{"http://sr"}, TLS: &CfgTLS{ServerName: "sr"}},
	}
	c := orig.clone()
	if !reflect.DeepEqual(c, orig) {
		t.Fatalf("clone differs: %+v vs %+v", c, orig)
	}
	c.SeedBrokers[0] = "changed"
	*c.BrokerTimeout = Duration(2 * time.Second)
	c.TLS.CACert = "changed"
	c.TLS.CipherSuites[0] = "changed"
	c.SASL.User = "changed"
	c.SR.URLs[0] = "changed"
	c.SR.TLS.ServerName = "changed"
	if orig.SeedBrokers[0] != "a:1" || orig.BrokerTimeout.D() != time.Second || orig.TLS.CACert != "/ca" || orig.TLS.CipherSuites[0] != "x" || orig.SASL.User != "u" || orig.SR.URLs[0] != "http://sr" || orig.SR.TLS.ServerName != "sr" {
		t.Errorf("mutating the clone reached the original: %+v tls=%+v sasl=%+v sr=%+v", orig, orig.TLS, orig.SASL, orig.SR)
	}
}
