package myconfig

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
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
				BrokerTimeout: client.Dur(10 * time.Second),
			},
			"local": {
				SeedBrokers:   []string{"localhost:9092"},
				BrokerTimeout: client.Dur(5 * time.Second),
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

func TestCreateProfile(t *testing.T) {
	brokers := func(hosts ...string) client.Cfg { return client.Cfg{SeedBrokers: hosts} }
	for _, test := range []struct {
		name        string
		exists      bool
		existing    string
		profile     string
		cfg         client.Cfg
		wantCurrent bool
		wantErr     string
		wantCode    int
		check       func(t *testing.T, f client.CfgFile)
	}{
		{
			name:        "new file in a missing directory",
			profile:     "local",
			cfg:         brokers("localhost:9092"),
			wantCurrent: true,
			check: func(t *testing.T, f client.CfgFile) {
				if f.CurrentProfile != "local" || len(f.Profiles) != 1 {
					t.Errorf("current=%q profiles=%v", f.CurrentProfile, f.Profiles)
				}
				if got := f.Profiles["local"].SeedBrokers; len(got) != 1 || got[0] != "localhost:9092" {
					t.Errorf("seed_brokers = %v", got)
				}
			},
		},
		{
			name:        "empty existing file",
			exists:      true,
			profile:     "local",
			cfg:         brokers("localhost:9092"),
			wantCurrent: true,
		},
		{
			name:        "comments only",
			exists:      true,
			existing:    "# nothing here yet\n",
			profile:     "local",
			cfg:         brokers("localhost:9092"),
			wantCurrent: true,
		},
		{
			name:   "adds to existing profiles and keeps current",
			exists: true,
			existing: `current_profile = "prod"

[profiles.prod]
seed_brokers = ["p:9092"]
broker_timeout = "10s"
`,
			profile: "staging",
			cfg:     brokers("s:9092"),
			check: func(t *testing.T, f client.CfgFile) {
				if f.CurrentProfile != "prod" || len(f.Profiles) != 2 {
					t.Errorf("current=%q profiles=%v", f.CurrentProfile, f.Profiles)
				}
				if got := f.Profiles["staging"].SeedBrokers; len(got) != 1 || got[0] != "s:9092" {
					t.Errorf("staging seed_brokers = %v", got)
				}
				if prod := f.Profiles["prod"]; prod.BrokerTimeout.D() != 10*time.Second || prod.SeedBrokers[0] != "p:9092" {
					t.Errorf("prod was changed: %+v", prod)
				}
			},
		},
		{
			name:   "profiles without a current makes the new one current",
			exists: true,
			existing: `[profiles.prod]
seed_brokers = ["p:9092"]
`,
			profile:     "staging",
			cfg:         brokers("s:9092"),
			wantCurrent: true,
			check: func(t *testing.T, f client.CfgFile) {
				if f.CurrentProfile != "staging" || len(f.Profiles) != 2 {
					t.Errorf("current=%q profiles=%v", f.CurrentProfile, f.Profiles)
				}
			},
		},
		{
			name:        "dangling current_profile is replaced",
			exists:      true,
			existing:    "current_profile = \"gone\"\n",
			profile:     "new",
			cfg:         brokers("n:9092"),
			wantCurrent: true,
			check: func(t *testing.T, f client.CfgFile) {
				if f.CurrentProfile != "new" {
					t.Errorf("current = %q", f.CurrentProfile)
				}
			},
		},
		{
			name:   "duplicate name",
			exists: true,
			existing: `current_profile = "prod"

[profiles.prod]
seed_brokers = ["p:9092"]
`,
			profile: "prod",
			cfg:     brokers("x:9092"),
			wantErr: "already exists",
		},
		{
			name:     "flat config is refused",
			exists:   true,
			existing: "seed_brokers = [\"x:9092\"]\n",
			profile:  "prod",
			cfg:      brokers("p:9092"),
			wantErr:  "flat",
		},
		{
			name:     "empty name",
			profile:  "",
			cfg:      brokers("p:9092"),
			wantErr:  "cannot be empty",
			wantCode: out.ExitUsage,
		},
		{
			name:    "tls and sasl round trip",
			profile: "secure",
			cfg: client.Cfg{
				SeedBrokers: []string{"k:9093"},
				DialTimeout: client.Dur(2 * time.Second),
				TLS:         &client.CfgTLS{CACert: "/ca.pem"},
				SASL:        &client.CfgSASL{Method: "scram-sha-256", User: "me", Pass: "pw"},
			},
			wantCurrent: true,
			check: func(t *testing.T, f client.CfgFile) {
				p := f.Profiles["secure"]
				if p.DialTimeout.D() != 2*time.Second {
					t.Errorf("dial_timeout = %v", p.DialTimeout.D())
				}
				if p.TLS == nil || p.TLS.CACert != "/ca.pem" {
					t.Errorf("tls = %+v", p.TLS)
				}
				if p.SASL == nil || p.SASL.Method != "scram-sha-256" || p.SASL.User != "me" || p.SASL.Pass != "pw" {
					t.Errorf("sasl = %+v", p.SASL)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "kcl", "config.toml")
			if test.exists {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(test.existing), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			current, err := createProfile(path, test.profile, test.cfg)

			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, test.wantErr)
				}
				if test.wantCode != 0 {
					var ce *out.ExitCodeError
					if !errors.As(err, &ce) || ce.Code != test.wantCode {
						t.Errorf("exit code = %v, want %d", err, test.wantCode)
					}
				}
				// A refused create must leave the file exactly as it was.
				got, rerr := os.ReadFile(path)
				if test.exists {
					if rerr != nil || string(got) != test.existing {
						t.Errorf("file changed on error: %q (%v)", got, rerr)
					}
				} else if !os.IsNotExist(rerr) {
					t.Errorf("file created on error: %q (%v)", got, rerr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if current != test.wantCurrent {
				t.Errorf("current = %v, want %v", current, test.wantCurrent)
			}

			var f client.CfgFile
			if _, err := toml.DecodeFile(path, &f); err != nil {
				t.Fatalf("decode written config: %v", err)
			}
			if _, ok := f.Profiles[test.profile]; !ok {
				t.Fatalf("profile %q missing from %v", test.profile, f.Profiles)
			}
			if test.check != nil {
				test.check(t, f)
			}
		})
	}
}

func TestCreateVisibleSetupHidden(t *testing.T) {
	cl := client.New(&cobra.Command{Use: "kcl"})
	for _, test := range []struct {
		name   string
		hidden bool
	}{
		{"create", false},
		{"set", false},
		{"setup", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd, _, err := Command(cl).Find([]string{test.name})
			if err != nil || cmd.Name() != test.name {
				t.Fatalf("find %q: %v (got %q)", test.name, err, cmd.Name())
			}
			if cmd.Hidden != test.hidden {
				t.Errorf("hidden = %v, want %v", cmd.Hidden, test.hidden)
			}
			if len(cmd.Aliases) != 0 {
				t.Errorf("aliases = %v, want none", cmd.Aliases)
			}
		})
	}
	if cmd, _, _ := Command(cl).Find([]string{"wizard"}); cmd != nil && cmd.Name() == "wizard" {
		t.Error("wizard should no longer resolve")
	}
}

func TestSetProfile(t *testing.T) {
	const profiles = `current_profile = "prod"

[profiles.prod]
seed_brokers = ["p:9092"]
broker_timeout = "10s"

[profiles.staging]
seed_brokers = ["s:9092"]
`
	for _, test := range []struct {
		name      string
		exists    bool
		existing  string
		profile   string
		opts      []string
		wantWhere string
		wantErr   string
		wantCode  int
		check     func(t *testing.T, f client.CfgFile)
	}{
		{
			name:      "current profile, one key, rest untouched",
			exists:    true,
			existing:  profiles,
			opts:      []string{"seed_brokers=a:9092,b:9092"},
			wantWhere: `profile "prod"`,
			check: func(t *testing.T, f client.CfgFile) {
				p := f.Profiles["prod"]
				if len(p.SeedBrokers) != 2 || p.SeedBrokers[1] != "b:9092" || p.BrokerTimeout.D() != 10*time.Second {
					t.Errorf("prod = %+v", p)
				}
				if f.CurrentProfile != "prod" || f.Profiles["staging"].SeedBrokers[0] != "s:9092" {
					t.Errorf("other state changed: current=%q staging=%+v", f.CurrentProfile, f.Profiles["staging"])
				}
			},
		},
		{
			name:      "-C picks another profile, several keys at once",
			exists:    true,
			existing:  profiles,
			profile:   "staging",
			opts:      []string{"sasl.method=scram-sha-256", "sasl_user=me", "dial_timeout=2s"},
			wantWhere: `profile "staging"`,
			check: func(t *testing.T, f client.CfgFile) {
				p := f.Profiles["staging"]
				if p.SASL == nil || p.SASL.Method != "scram-sha-256" || p.SASL.User != "me" || p.DialTimeout.D() != 2*time.Second {
					t.Errorf("staging = %+v sasl=%+v", p, p.SASL)
				}
				if f.Profiles["prod"].SeedBrokers[0] != "p:9092" {
					t.Errorf("prod changed: %+v", f.Profiles["prod"])
				}
			},
		},
		{
			name:     "flat config is edited at the top level",
			exists:   true,
			existing: "seed_brokers = [\"x:9092\"]\n",
			opts:     []string{"retry_timeout=5s"},
			check: func(t *testing.T, f client.CfgFile) {
				if len(f.Profiles) != 0 || f.SeedBrokers[0] != "x:9092" || f.RetryTimeout.D() != 5*time.Second {
					t.Errorf("flat = %+v", f.Cfg)
				}
			},
		},
		{
			name:    "missing file",
			opts:    []string{"seed_brokers=a:9092"},
			wantErr: "kcl profile create",
		},
		{
			name:    "empty file has nothing to set",
			exists:  true,
			opts:    []string{"seed_brokers=a:9092"},
			wantErr: "no profiles",
		},
		{
			name:     "profiles but no current and no -C",
			exists:   true,
			existing: "[profiles.prod]\nseed_brokers = [\"p:9092\"]\n",
			opts:     []string{"seed_brokers=a:9092"},
			wantErr:  "no current profile",
		},
		{
			name:     "-C names a missing profile",
			exists:   true,
			existing: profiles,
			profile:  "nope",
			opts:     []string{"seed_brokers=a:9092"},
			wantErr:  "not found",
		},
		{
			name:     "-C on a flat config",
			exists:   true,
			existing: "seed_brokers = [\"x:9092\"]\n",
			profile:  "prod",
			opts:     []string{"seed_brokers=a:9092"},
			wantErr:  "no profiles",
		},
		{
			name:     "unknown key",
			exists:   true,
			existing: profiles,
			opts:     []string{"seed_brokers=a:9092", "nope=1"},
			wantErr:  "unknown opt key",
			wantCode: out.ExitUsage,
		},
		{
			name:     "bad value",
			exists:   true,
			existing: profiles,
			opts:     []string{"dial_timeout=soon"},
			wantErr:  "invalid duration",
			wantCode: out.ExitUsage,
		},
		{
			name:     "bare non-boolean key",
			exists:   true,
			existing: profiles,
			opts:     []string{"seed_brokers"},
			wantErr:  "needs a value",
			wantCode: out.ExitUsage,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			if test.exists {
				if err := os.WriteFile(path, []byte(test.existing), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			where, err := setProfile(path, test.profile, func(cfg *client.Cfg) error {
				return client.ApplyCfgOpts(cfg, test.opts)
			})

			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, test.wantErr)
				}
				if test.wantCode != 0 {
					var ce *out.ExitCodeError
					if !errors.As(err, &ce) || ce.Code != test.wantCode {
						t.Errorf("exit code = %v, want %d", err, test.wantCode)
					}
				}
				// A refused set must leave the file exactly as it was.
				got, rerr := os.ReadFile(path)
				if test.exists {
					if rerr != nil || string(got) != test.existing {
						t.Errorf("file changed on error: %q (%v)", got, rerr)
					}
				} else if !os.IsNotExist(rerr) {
					t.Errorf("file created on error: %q (%v)", got, rerr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if test.wantWhere == "" {
				test.wantWhere = path
			}
			if where != test.wantWhere {
				t.Errorf("where = %q, want %q", where, test.wantWhere)
			}

			var f client.CfgFile
			if _, err := toml.DecodeFile(path, &f); err != nil {
				t.Fatalf("decode written config: %v", err)
			}
			test.check(t, f)
		})
	}
}

// TestSetCommandFlags drives set through cobra, so the root -X, -B, and -R
// flags reach the profile the way they do from the shell.
func TestSetCommandFlags(t *testing.T) {
	const profiles = `current_profile = "prod"

[profiles.prod]
seed_brokers = ["p:9092"]
broker_timeout = "10s"
`
	for _, test := range []struct {
		name     string
		args     []string
		wantErr  string
		wantCode int
		check    func(t *testing.T, f client.CfgFile)
	}{
		{
			name: "-X and -B together",
			args: []string{"profile", "set", "-X", "sasl.user=me", "-B", "a:9092,b:9092"},
			check: func(t *testing.T, f client.CfgFile) {
				p := f.Profiles["prod"]
				if len(p.SeedBrokers) != 2 || p.SeedBrokers[1] != "b:9092" || p.SASL == nil || p.SASL.User != "me" || p.BrokerTimeout.D() != 10*time.Second {
					t.Errorf("prod = %+v sasl=%+v", p, p.SASL)
				}
			},
		},
		{
			name: "-R sets registry urls",
			args: []string{"profile", "set", "-R", "http://sr:8081"},
			check: func(t *testing.T, f client.CfgFile) {
				if p := f.Profiles["prod"]; p.SR == nil || len(p.SR.URLs) != 1 || p.SR.URLs[0] != "http://sr:8081" || p.SeedBrokers[0] != "p:9092" {
					t.Errorf("prod = %+v sr=%+v", p, p.SR)
				}
			},
		},
		{
			name:     "nothing given",
			args:     []string{"profile", "set"},
			wantErr:  "nothing to set",
			wantCode: out.ExitUsage,
		},
		{
			name:    "positional pairs are not accepted",
			args:    []string{"profile", "set", "sasl.user=me"},
			wantErr: "unknown command",
		},
		{
			name:     "bad -X leaves the file alone",
			args:     []string{"profile", "set", "-X", "nope=1"},
			wantErr:  "unknown opt key",
			wantCode: out.ExitUsage,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(path, []byte(profiles), 0o644); err != nil {
				t.Fatal(err)
			}
			root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
			cl := client.New(root)
			root.AddCommand(Command(cl))
			root.SetArgs(append([]string{"--config-path", path}, test.args...))

			err := root.Execute()

			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, test.wantErr)
				}
				if test.wantCode != 0 {
					var ce *out.ExitCodeError
					if !errors.As(err, &ce) || ce.Code != test.wantCode {
						t.Errorf("exit code = %v, want %d", err, test.wantCode)
					}
				}
				if got, _ := os.ReadFile(path); string(got) != profiles {
					t.Errorf("file changed on error:\n%s", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var f client.CfgFile
			if _, err := toml.DecodeFile(path, &f); err != nil {
				t.Fatalf("decode written config: %v", err)
			}
			test.check(t, f)
		})
	}
}

// TestSetTouchesOnlyNamedKeys pins that set writes back what the file had,
// plus the keys named on the command line, and no defaults for the rest.
func TestSetTouchesOnlyNamedKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	const before = `current_profile = "prod"

[profiles.prod]
seed_brokers = ["p:9092"]

[profiles.other]
seed_brokers = ["o:9092"]
dial_timeout = "2s"
[profiles.other.tls]
ca_cert_path = "/ca.pem"
`
	if err := os.WriteFile(path, []byte(before), 0o644); err != nil {
		t.Fatal(err)
	}
	var want client.CfgFile
	if _, err := toml.DecodeFile(path, &want); err != nil {
		t.Fatal(err)
	}

	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs([]string{"--config-path", path, "profile", "set", "-X", "sasl.user=me"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}

	var got client.CfgFile
	if _, err := toml.DecodeFile(path, &got); err != nil {
		t.Fatal(err)
	}
	prod := got.Profiles["prod"]
	if prod.SASL == nil || prod.SASL.User != "me" {
		t.Fatalf("sasl.user not set: %+v", prod.SASL)
	}
	prod.SASL = nil
	got.Profiles["prod"] = prod
	if !reflect.DeepEqual(got, want) {
		t.Errorf("set changed more than sasl.user:\n got %+v\nwant %+v", got, want)
	}
}
