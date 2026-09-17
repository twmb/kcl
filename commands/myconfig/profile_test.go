package myconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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
				SASL:        &client.CfgSASL{Mechanism: "scram-sha-256", User: "me", Pass: "pw"},
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
				if p.SASL == nil || p.SASL.Mechanism != "scram-sha-256" || p.SASL.User != "me" || p.SASL.Pass != "pw" {
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
		name        string
		exists      bool
		existing    string
		profile     string
		opts        []string
		wantEdited  string
		wantCurrent bool
		wantErr     string
		wantCode    int
		check       func(t *testing.T, f client.CfgFile)
	}{
		{
			name:        "current profile, one key, rest untouched",
			exists:      true,
			existing:    profiles,
			opts:        []string{"seed_brokers=a:9092,b:9092"},
			wantEdited:  "prod",
			wantCurrent: true,
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
			name:       "-C picks another profile, several keys at once",
			exists:     true,
			existing:   profiles,
			profile:    "staging",
			opts:       []string{"sasl.mechanism=scram-sha-256", "sasl_user=me", "dial_timeout=2s"},
			wantEdited: "staging",
			check: func(t *testing.T, f client.CfgFile) {
				p := f.Profiles["staging"]
				if p.SASL == nil || p.SASL.Mechanism != "scram-sha-256" || p.SASL.User != "me" || p.DialTimeout.D() != 2*time.Second {
					t.Errorf("staging = %+v sasl=%+v", p, p.SASL)
				}
				if f.Profiles["prod"].SeedBrokers[0] != "p:9092" {
					t.Errorf("prod changed: %+v", f.Profiles["prod"])
				}
			},
		},
		{
			name:        "flat config is edited at the top level",
			exists:      true,
			existing:    "seed_brokers = [\"x:9092\"]\n",
			opts:        []string{"retry_timeout=5s"},
			wantCurrent: true,
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

			edited, current, err := setProfile(path, test.profile, func(cfg *client.Cfg) error {
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
			if edited != test.wantEdited || current != test.wantCurrent {
				t.Errorf("edited %q current %v, want %q %v", edited, current, test.wantEdited, test.wantCurrent)
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

func TestShellWordAndUniq(t *testing.T) {
	for in, want := range map[string]string{
		"prod":       "prod",
		"with space": "'with space'",
		"it's":       `'it'\''s'`,
		"a/b.c:d":    "a/b.c:d",
		"a$b":        "'a$b'",
	} {
		if got := shellWord(in); got != want {
			t.Errorf("shellWord(%q) = %s, want %s", in, got, want)
		}
	}
	if got := uniq([]string{"seed_brokers", "sasl.user", "seed_brokers"}); len(got) != 2 || got[0] != "seed_brokers" || got[1] != "sasl.user" {
		t.Errorf("uniq = %v", got)
	}
}

func TestSetMessage(t *testing.T) {
	for _, test := range []struct {
		set, unset []string
		want       string
	}{
		{[]string{"seed_brokers", "seed_brokers"}, nil, "Set seed_brokers"},
		{nil, []string{"sasl.pass"}, "Unset sasl.pass"},
		{[]string{"sasl.user"}, []string{"sasl.pass", "dial_timeout"}, "Set sasl.user; unset sasl.pass, dial_timeout"},
	} {
		if got := setMessage(test.set, test.unset); got != test.want {
			t.Errorf("setMessage(%v, %v) = %q, want %q", test.set, test.unset, got, test.want)
		}
	}
}

func TestCurrentHonorsProfileFlag(t *testing.T) {
	const profiles = "current_profile = \"prod\"\n[profiles.prod]\nseed_brokers = [\"p:9092\"]\n[profiles.dev]\nseed_brokers = [\"d:9092\"]\n"
	for _, test := range []struct {
		name    string
		env     string
		args    []string
		want    string
		wantErr string
	}{
		{name: "current_profile", args: []string{"profile", "current"}, want: "prod\n"},
		{name: "-C wins", args: []string{"-C", "dev", "profile", "current"}, want: "dev\n"},
		{name: "-C unknown", args: []string{"-C", "nope", "profile", "current"}, wantErr: "not found"},
		{name: "KCL_PROFILE", env: "dev", args: []string{"profile", "current"}, want: "dev\n"},
		{name: "-C wins over KCL_PROFILE", env: "dev", args: []string{"-C", "prod", "profile", "current"}, want: "prod\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("KCL_PROFILE", test.env)
			path := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(path, []byte(profiles), 0o644); err != nil {
				t.Fatal(err)
			}
			root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
			cl := client.New(root)
			root.AddCommand(Command(cl))
			root.SetArgs(append([]string{"--config-path", path}, test.args...))
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			old := os.Stdout
			os.Stdout = w
			execErr := root.Execute()
			w.Close()
			os.Stdout = old
			outb, _ := io.ReadAll(r)
			if test.wantErr != "" {
				if execErr == nil || !strings.Contains(execErr.Error(), test.wantErr) {
					t.Fatalf("err = %v, want containing %q", execErr, test.wantErr)
				}
				return
			}
			if execErr != nil {
				t.Fatal(execErr)
			}
			if string(outb) != test.want {
				t.Errorf("stdout = %q, want %q", outb, test.want)
			}
		})
	}
}

func TestProfileFormats(t *testing.T) {
	const profiles = "current_profile = \"prod\"\n[profiles.prod]\nseed_brokers = [\"p:9092\", \"q:9092\"]\ndial_timeout = \"2s\"\n[profiles.prod.sasl]\nmethod = \"plain\"\n[profiles.prod.schema_registry]\nurls = [\"http://sr:8081\"]\n[profiles.dev]\nseed_brokers = [\"d:9092\"]\n"
	for _, test := range []struct {
		name string
		args []string
		want string
		json bool
	}{
		{name: "list json", args: []string{"--format", "json", "profile", "list"}, want: `"name":"prod"`, json: true},
		{name: "list awk", args: []string{"--format", "awk", "profile", "list"}, want: "dev\tfalse\nprod\ttrue\n"},
		{name: "current json", args: []string{"--format", "json", "profile", "current"}, want: `"profile":"prod"`, json: true},
		{name: "current awk with -C", args: []string{"--format", "awk", "-C", "dev", "profile", "current"}, want: "dev\n"},
		{name: "dump text is toml", args: []string{"profile", "dump"}, want: "seed_brokers = [\"p:9092\", \"q:9092\"]"},
		{name: "dump json", args: []string{"--format", "json", "profile", "dump"}, want: `"mechanism":"plain"`, json: true},
		{name: "dump awk", args: []string{"--format", "awk", "profile", "dump"}, want: "registry.urls\thttp://sr:8081\nsasl.mechanism\tplain\nseed_brokers\tp:9092,q:9092\n"},
		{name: "dump json names the registry section", args: []string{"--format", "json", "profile", "dump"}, want: `"registry":{"urls":["http://sr:8081"]}`, json: true},
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
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			old := os.Stdout
			os.Stdout = w
			execErr := root.Execute()
			w.Close()
			os.Stdout = old
			outb, _ := io.ReadAll(r)
			if execErr != nil {
				t.Fatal(execErr)
			}
			if !strings.Contains(string(outb), test.want) {
				t.Errorf("stdout lacks %q:\n%s", test.want, outb)
			}
			if test.json && !json.Valid(outb) {
				t.Errorf("not JSON:\n%s", outb)
			}
		})
	}
}

func TestFlattenCfg(t *testing.T) {
	got := flattenCfg("", map[string]any{"b": map[string]any{"y": "1", "x": []any{"p", "q"}}, "a": int64(2)})
	want := [][2]string{{"a", "2"}, {"b.x", "p,q"}, {"b.y", "1"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("flattenCfg = %v, want %v", got, want)
	}
}

// TestSetRewritesOldSectionName pins that a file holding the old
// [schema_registry] section is read, and written back as [registry], the
// section's current name, by the first command that writes the file.
func TestSetRewritesOldSectionName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	const before = `current_profile = "prod"

[profiles.prod]
seed_brokers = ["p:9092"]
[profiles.prod.schema_registry]
urls = ["http://sr:8081"]
`
	if err := os.WriteFile(path, []byte(before), 0o644); err != nil {
		t.Fatal(err)
	}
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs([]string{"--config-path", path, "profile", "set", "-X", "dial_timeout=2s"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "[profiles.prod.registry]") || strings.Contains(string(got), "schema_registry") {
		t.Errorf("file after set:\n%s", got)
	}
	var f client.CfgFile
	if _, err := toml.DecodeFile(path, &f); err != nil {
		t.Fatal(err)
	}
	if p := f.Profiles["prod"]; p.SR == nil || p.SR.URLs[0] != "http://sr:8081" || p.DialTimeout.D() != 2*time.Second {
		t.Errorf("prod = %+v sr=%+v", p, p.SR)
	}
}

// runProfile runs the profile command tree in-process against the config
// file at path and returns what it wrote to stdout.
func runProfile(t *testing.T, path string, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs(append([]string{"--config-path", path}, args...))
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	execErr := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	return string(b), execErr
}

// TestMutatorDocuments pins that every command that writes the config file
// prints a {profile, path, current} document under --format json, one KEY
// and value row per field under awk, and nothing on stdout in text, where
// the line it always printed stays on stderr.
func TestMutatorDocuments(t *testing.T) {
	const profiles = "current_profile = \"prod\"\n[profiles.prod]\nseed_brokers = [\"p:9092\"]\n[profiles.dev]\nseed_brokers = [\"d:9092\"]\n"
	for _, test := range []struct {
		name    string
		args    []string
		profile string
		current bool
	}{
		{"create", []string{"profile", "create", "new", "-B", "n:9092"}, "new", false},
		{"create into an empty file", []string{"profile", "create", "first", "-B", "f:9092"}, "first", true},
		{"use", []string{"profile", "use", "dev"}, "dev", true},
		{"set current", []string{"profile", "set", "-X", "dial_timeout=2s"}, "prod", true},
		{"set -C", []string{"-C", "dev", "profile", "set", "-X", "dial_timeout=2s"}, "dev", false},
		{"rename current", []string{"profile", "rename", "prod", "live"}, "live", true},
		{"rename other", []string{"profile", "rename", "dev", "test"}, "test", false},
		{"delete", []string{"profile", "delete", "dev"}, "dev", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			existing := profiles
			if strings.Contains(test.name, "empty file") {
				existing = ""
			}
			for _, format := range []string{"json", "awk", "text"} {
				if err := os.WriteFile(path, []byte(existing), 0o644); err != nil {
					t.Fatal(err)
				}
				stdout, err := runProfile(t, path, append(test.args, "--format", format)...)
				if err != nil {
					t.Fatalf("%s: %v", format, err)
				}
				switch format {
				case "json":
					var doc map[string]any
					if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
						t.Fatalf("json: %v: %q", err, stdout)
					}
					want := map[string]any{"_command": "profile." + test.args[slices.Index(test.args, "profile")+1], "_version": float64(1), "profile": test.profile, "path": path, "current": test.current}
					if !reflect.DeepEqual(doc, want) {
						t.Errorf("json = %v, want %v", doc, want)
					}
				case "awk":
					if want := fmt.Sprintf("profile\t%s\npath\t%s\ncurrent\t%v\n", test.profile, path, test.current); stdout != want {
						t.Errorf("awk = %q, want %q", stdout, want)
					}
				default:
					if stdout != "" {
						t.Errorf("text stdout = %q, want nothing", stdout)
					}
				}
			}
		})
	}
}

// TestCreateWritesOnlyWhatWasGiven pins the file profile create writes: the
// keys from the flags and nothing else, so no default is frozen into it.
func TestCreateWritesOnlyWhatWasGiven(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "brokers",
			args: []string{"-B", "k:9092"},
			want: "current_profile = \"p1\"\n\n[profiles]\n  [profiles.p1]\n    seed_brokers = [\"k:9092\"]\n",
		},
		{
			name: "no flags",
			args: nil,
			want: "current_profile = \"p1\"\n\n[profiles]\n  [profiles.p1]\n",
		},
		{
			name: "registry and sasl",
			args: []string{"-R", "http://sr:8081", "-X", "sasl.mechanism=plain", "-X", "sasl.user=me"},
			want: "current_profile = \"p1\"\n\n[profiles]\n  [profiles.p1]\n    [profiles.p1.sasl]\n      mechanism = \"plain\"\n      user = \"me\"\n    [profiles.p1.registry]\n      urls = [\"http://sr:8081\"]\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			if _, err := runProfile(t, path, append([]string{"profile", "create", "p1"}, test.args...)...); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != test.want {
				t.Errorf("file:\n%s\nwant:\n%s", got, test.want)
			}
			// The profile loads, and takes the defaults for what it left out.
			root := &cobra.Command{Use: "kcl"}
			c := client.New(root)
			if err := root.ParseFlags([]string{"--config-path", path}); err != nil {
				t.Fatal(err)
			}
			cfg := c.DiskCfg()
			if test.name != "brokers" && (len(cfg.SeedBrokers) != 1 || cfg.SeedBrokers[0] != "localhost:9092") {
				t.Errorf("seed_brokers = %v, want the default", cfg.SeedBrokers)
			}
			if cfg.BrokerTimeout.D() != 5*time.Second {
				t.Errorf("broker_timeout = %v, want the 5s default", cfg.BrokerTimeout.D())
			}
		})
	}
}

// TestCurrentNoneInAWK pins that profile current prints no awk row when no
// profile is set, rather than one empty line, and TestDumpNestsConfig that
// dump's JSON keeps the config under "config", beside the envelope.
func TestCurrentNoneInAWK(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte("[profiles.prod]\nseed_brokers = [\"p:9092\"]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, err := runProfile(t, path, "profile", "current", "--format", "awk")
	if err != nil {
		t.Fatal(err)
	}
	if stdout != "" {
		t.Errorf("awk stdout = %q, want nothing", stdout)
	}
	stdout, err = runProfile(t, path, "profile", "current", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout, `"profile":""`) {
		t.Errorf("json = %q, want an empty profile", stdout)
	}
}

func TestDumpNestsConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte("current_profile = \"prod\"\n[profiles.prod]\nseed_brokers = [\"p:9092\"]\ndial_timeout = \"2s\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, err := runProfile(t, path, "profile", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("json: %v: %q", err, stdout)
	}
	if len(doc) != 3 || doc["_command"] != "profile.dump" {
		t.Errorf("top level = %v, want _command, _version, and config only", doc)
	}
	cfg, _ := doc["config"].(map[string]any)
	if cfg["dial_timeout"] != "2s" || cfg["broker_timeout"] != "5s" {
		t.Errorf("config = %v", cfg)
	}
}
