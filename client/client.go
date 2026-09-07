// Package client is the kcl client, containing global options and
// a kgo.Client builder.
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/twmb/kcl/out"
)

// Requestor can either be a kgo.Client or kgo.Broker.
type Requestor interface {
	Request(context.Context, kmsg.Request) (kmsg.Response, error)
}

type CfgTLS struct {
	CACert         string `toml:"ca_cert_path,omitempty"`
	ClientCertPath string `toml:"client_cert_path,omitempty"`
	ClientKeyPath  string `toml:"client_key_path,omitempty"`
	ServerName     string `toml:"server_name,omitempty"`

	InsecureSkipVerify bool `toml:"insecure,omitempty"`

	MinVersion       string   `toml:"min_version,omitempty"`
	CipherSuites     []string `toml:"cipher_suites"`
	CurvePreferences []string `toml:"curve_preferences"`
}

type CfgSASL struct {
	Method  string `toml:"method,omitempty"`
	Zid     string `toml:"zid,omitempty"`
	User    string `toml:"user,omitempty"`
	Pass    string `toml:"pass,omitempty"`
	IsToken bool   `toml:"is_token,omitempty"`
}

// CfgSR configures the Schema Registry client used by "kcl registry" and by
// schema-aware produce/consume. The registry is a separate HTTP service from
// the Kafka brokers, so it has its own URLs and auth.
type CfgSR struct {
	// URLs are the Schema Registry base URLs (e.g. http://localhost:8081).
	URLs []string `toml:"urls,omitempty"`

	// User and Pass, if set, enable HTTP basic auth.
	User string `toml:"user,omitempty"`
	Pass string `toml:"pass,omitempty"`

	// BearerToken, if set, is sent as an Authorization: Bearer header. It is
	// mutually exclusive with basic auth.
	BearerToken string `toml:"bearer_token,omitempty"`

	// Context, if set, is the schema registry context (namespace) that all
	// requests are scoped to by default.
	Context string `toml:"context,omitempty"`

	// TLS configures TLS for https registry URLs. It reuses the same shape
	// as the Kafka TLS config.
	TLS *CfgTLS `toml:"tls,omitzero"`
}

// duration wraps time.Duration with TOML + -X parsing that accepts Go
// duration strings ("5s", "500ms", "1m").
type Duration time.Duration

func (d *Duration) UnmarshalText(b []byte) error {
	parsed, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

// D returns the underlying time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Cfg contains kcl options that can be defined in a file.
type Cfg struct {
	SeedBrokers []string `toml:"seed_brokers,omitempty"`

	// BrokerTimeout is the wire TimeoutMs value sent to the broker
	// in admin-style requests (e.g. CreateTopics.TimeoutMs). It
	// tells the broker how long to wait before giving up on the
	// server side.
	BrokerTimeout Duration `toml:"broker_timeout,omitzero"`

	// DialTimeout bounds how long kgo waits for a single TCP dial.
	// Zero leaves kgo's default (10s).
	DialTimeout Duration `toml:"dial_timeout,omitzero"`

	// RetryTimeout bounds total time for a client request and its
	// retries. Zero leaves kgo's default (30s for most requests,
	// 45s for group-session requests).
	RetryTimeout Duration `toml:"retry_timeout,omitzero"`

	TLS  *CfgTLS  `toml:"tls,omitzero"`
	SASL *CfgSASL `toml:"sasl,omitempty"`

	// SR configures the Schema Registry client.
	SR *CfgSR `toml:"schema_registry,omitzero"`
}

// CfgFile represents the full config file, which may contain named profiles.
type CfgFile struct {
	// CurrentProfile is the active profile name.
	CurrentProfile string         `toml:"current_profile,omitempty"`
	Profiles       map[string]Cfg `toml:"profiles,omitempty"`

	// Flat fields for backward compat (single-profile config).
	Cfg
}

// Client contains kgo client options and a kgo client.
type Client struct {
	opts    []kgo.Opt
	once    sync.Once
	cfgOnce sync.Once
	client  *kgo.Client
	txnSess *kgo.GroupTransactSession

	logLevel string
	logFile  string

	asVersion string
	asJSON    bool
	format    string

	// config options parsed and filled on load
	defaultCfgPath   string
	cfgPath          string
	noCfgFile        bool
	envNoCfgFile     bool
	envPfx           string
	flagOverrides    []string
	bootstrapServers []string // --bootstrap-servers/-B override
	registryURLs     []string // --registry/-R override
	registryContext  string   // registry --context override
	profileName      string   // --context/-C override
	cfgFile          CfgFile
	cfg              Cfg
}

// Format returns the output format: "text", "json", or "awk".
// If --dump-json is set and --format is not explicitly set, returns "json".
func (c *Client) Format() string {
	switch c.format {
	case "text", "json", "awk":
	default:
		out.Die("invalid --format %q: must be text, json, or awk", c.format)
	}
	if c.format != "text" {
		return c.format
	}
	if c.asJSON {
		return "json"
	}
	return "text"
}

// AsJSON returns whether the output should be dumped as JSON if applicable.
func (c *Client) AsJSON() bool { return c.Format() == "json" }

// TimeoutMillis returns the value to put in wire TimeoutMs fields on
// broker requests (e.g. CreateTopicsRequest.TimeoutMs). The configured
// broker_timeout duration is rounded down to the nearest millisecond.
func (c *Client) TimeoutMillis() int32 {
	c.loadClientOnce()
	return int32(c.cfg.BrokerTimeout.D().Milliseconds())
}

// Version is set by main via SetVersion; used to tag the kgo ClientID.
var clientVersion = "dev"

// SetVersion records the effective kcl version. Called by main once
// it has resolved ldflags / debug.BuildInfo. The version is used as
// part of the kgo ClientID so brokers can identify kcl in audit logs.
func SetVersion(v string) {
	if v != "" {
		clientVersion = v
	}
}

// New returns a new Client with the given config and installs some
// persistent flags and commands to root.
func New(root *cobra.Command) *Client {
	c := &Client{
		opts: []kgo.Opt{
			kgo.MetadataMinAge(time.Second),
			// Tag the ClientID so brokers can see kcl in ACL
			// audit logs and metrics.
			kgo.ClientID("kcl/" + clientVersion),
		},
		cfg: Cfg{
			SeedBrokers:   []string{"localhost:9092"},
			BrokerTimeout: Duration(5 * time.Second),
		},
	}

	cfgDir, err := os.UserConfigDir()
	if err == nil {
		cfgDir = filepath.Join(cfgDir, "kcl")
	}
	if envDir, ok := os.LookupEnv("KCL_CONFIG_DIR"); ok {
		cfgDir = envDir
	}
	cfgFile := "config.toml"
	if envPath, ok := os.LookupEnv("KCL_CONFIG_FILE"); ok {
		cfgFile = envPath
	}
	c.defaultCfgPath = filepath.Join(cfgDir, cfgFile)
	if envFile, ok := os.LookupEnv("KCL_CONFIG_PATH"); ok {
		c.defaultCfgPath = envFile
	}

	c.envNoCfgFile = os.Getenv("KCL_NO_CONFIG_FILE") != ""

	root.PersistentFlags().StringVar(&c.logLevel, "log-level", "none", "log level to use for basic logging (none, error, warn, info, debug)")
	root.PersistentFlags().StringVar(&c.logFile, "log-file", "STDERR", "log to this file (if log-level is not none; file must not exist; STDERR sets to stderr & STDOUT sets to stdout)")
	root.PersistentFlags().StringVar(&c.cfgPath, "config-path", c.defaultCfgPath, "path to confile file (lowest priority)")
	root.PersistentFlags().BoolVar(&c.noCfgFile, "no-config-file", false, "do not load any config file")
	root.PersistentFlags().StringVar(&c.envPfx, "config-env-prefix", "KCL_", "environment variable prefix for config overrides (middle priority)")
	root.PersistentFlags().StringArrayVarP(&c.flagOverrides, "config-opt", "X", nil, "flag provided config option (highest priority)")
	root.PersistentFlags().StringSliceVarP(&c.bootstrapServers, "bootstrap-servers", "B", nil, "comma-separated list of seed brokers (overrides profile/config); shorthand for -X seed_brokers=...")
	root.PersistentFlags().StringSliceVarP(&c.registryURLs, "registry", "R", nil, "comma-separated list of schema registry URLs (overrides profile/config); shorthand for -X registry.urls=...")
	root.PersistentFlags().StringVar(&c.asVersion, "as-version", "", "if nonempty, which version of Kafka versions to use (e.g. '0.8.0', '2.3.0')")
	root.PersistentFlags().StringVar(&c.format, "format", "text", "output format (text, json, awk)")
	root.PersistentFlags().StringVarP(&c.profileName, "profile", "C", "", "use a specific config profile")
	root.PersistentFlags().BoolVarP(&c.asJSON, "dump-json", "j", false, "dump response as json if supported")
	root.PersistentFlags().MarkDeprecated("dump-json", "use --format json instead")

	return c
}

// AddOpt adds an option to be passed to the eventual new kgo.Client.
func (c *Client) AddOpt(opt kgo.Opt) {
	c.opts = append(c.opts, opt)
}

// Client returns a new kgo.Client using all buffered options.
//
// This can only be used once.
func (c *Client) Client() *kgo.Client {
	c.loadClientOnce()
	return c.client
}

// GroupTransactSession returns a new kgo.GroupTransactSession using all
// buffered options.
//
// This can only be used once, and is incompatible with the Client function.
func (c *Client) GroupTransactSession() *kgo.GroupTransactSession {
	c.loadTxnSessOnce()
	return c.txnSess
}

// DiskCfg returns the loaded disk configuration.
func (c *Client) DiskCfg() Cfg {
	c.loadClientOnce()
	return c.cfg
}

// DefaultCfgPath returns the default path that is used to load configs.
func (c *Client) DefaultCfgPath() string {
	return c.defaultCfgPath
}

// RemakeWithOpts remakes the client with additional opts added. The opts are
// not persisted to the overall Client opts, but the created client does
// persist. This is not concurrent safe.
func (c *Client) RemakeWithOpts(opts ...kgo.Opt) *kgo.Client {
	var err error
	c.client.Close()
	c.client, err = kgo.NewClient(append(c.opts, opts...)...)
	out.MaybeDie(err, "unable to load client: %v", err)
	return c.client
}

func (c *Client) loadClientOnce() {
	c.once.Do(func() {
		c.fillOpts()
		var err error
		c.client, err = kgo.NewClient(c.opts...)
		out.MaybeDie(err, "unable to load client: %v", err)
	})
}

func (c *Client) loadTxnSessOnce() {
	c.once.Do(func() {
		c.fillOpts()
		var err error
		c.txnSess, err = kgo.NewGroupTransactSession(c.opts...)
		out.MaybeDie(err, "unable to load group transact session: %v", err)
	})
}

// loadCfg parses the config file and applies overrides exactly once. It is
// safe to call from both the Kafka client path (fillOpts) and the Schema
// Registry client path, which lets "kcl registry" commands run without ever
// constructing a kgo.Client.
func (c *Client) loadCfg() {
	c.cfgOnce.Do(func() {
		c.parseCfgFile()     // loads config file if needed
		c.processOverrides() // overrides config values just loaded
	})
}

func (c *Client) fillOpts() {
	c.loadCfg()             // loads config file + overrides (once)
	c.maybeAddMaxVersions() // fills MaxVersions if necessary
	c.parseLogLevel()       // adds basic logger if necessary

	if err := c.maybeAddSASL(); err != nil {
		out.Die("sasl error: %v", err)
	}

	tlscfg, err := c.loadTLS()
	if err != nil {
		out.Die("%s", err)
	} else if tlscfg != nil {
		dialer := &net.Dialer{Timeout: 10 * time.Second}
		c.AddOpt(kgo.Dialer(func(_ context.Context, _, host string) (net.Conn, error) {
			cloned := tlscfg.Clone()
			if c.cfg.TLS.ServerName != "" {
				cloned.ServerName = c.cfg.TLS.ServerName
			} else if h, _, err := net.SplitHostPort(host); err == nil {
				cloned.ServerName = h
			}
			return tls.DialWithDialer(dialer, "tcp", host, cloned)
		}))
	}

	c.AddOpt(kgo.SeedBrokers(c.cfg.SeedBrokers...))
	if d := c.cfg.DialTimeout.D(); d > 0 {
		c.AddOpt(kgo.DialTimeout(d))
	}
	if d := c.cfg.RetryTimeout.D(); d > 0 {
		c.AddOpt(kgo.RetryTimeout(d))
	}
}

func (c *Client) parseCfgFile() {
	if c.noCfgFile || c.envNoCfgFile {
		return
	}

	// First try decoding as a context-aware config file.
	md, err := toml.DecodeFile(c.cfgPath, &c.cfgFile)
	if os.IsNotExist(err) {
		// A missing file is the same as --no-config-file.
		return
	}
	if err != nil {
		out.Die("unable to decode config file %q: %v", c.cfgPath, err)
	}
	// Warn on unknown top-level keys so typos and stale names from
	// old configs don't get silently dropped. This catches "timeout_ms"
	// after the rename, "tls_xxx" typos, etc.
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		for _, k := range undecoded {
			fmt.Fprintf(os.Stderr, "kcl: warning: unknown config key %q in %s\n", k, c.cfgPath)
		}
	}

	// If the file has named profiles, select the appropriate one.
	if len(c.cfgFile.Profiles) > 0 {
		name := c.cfgFile.CurrentProfile
		if c.profileName != "" {
			name = c.profileName
		}
		if name == "" {
			out.Die("config has profiles but no current_profile set; use --profile or set current_profile in config")
		}
		p, ok := c.cfgFile.Profiles[name]
		if !ok {
			out.Die("profile %q not found in config file", name)
		}
		c.cfg = p
		return
	}

	// No profiles: use the flat config (backward compatible).
	c.cfg = c.cfgFile.Cfg
}

// CfgFilePath returns the path to the config file.
func (c *Client) CfgFilePath() string {
	return c.cfgPath
}

// LoadedCfgFile returns the full loaded config file (may include contexts).
func (c *Client) LoadedCfgFile() CfgFile {
	c.loadClientOnce()
	return c.cfgFile
}

// cfgSetters maps every config key, in its normalized underscore form, to
// the function that sets it. See normCfgKey.
var cfgSetters = func() map[string]func(*Cfg, string) error {
	intoStrSlice := func(in string, dst *[]string) error {
		*dst = nil
		split := strings.Split(in, ",")
		for _, on := range split {
			on = strings.TrimSpace(on)
			if len(on) == 0 {
				return fmt.Errorf("invalid empty value in %q", in)
			}
			*dst = append(*dst, on)
		}
		return nil
	}

	mktls := func(c *Cfg) {
		if c.TLS == nil {
			c.TLS = new(CfgTLS)
		}
	}

	mksasl := func(c *Cfg) {
		if c.SASL == nil {
			c.SASL = new(CfgSASL)
		}
	}

	mksr := func(c *Cfg) {
		if c.SR == nil {
			c.SR = new(CfgSR)
		}
	}

	mksrtls := func(c *Cfg) {
		mksr(c)
		if c.SR.TLS == nil {
			c.SR.TLS = new(CfgTLS)
		}
	}

	intoDuration := func(v string, dst *Duration) error {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %v", v, err)
		}
		*dst = Duration(d)
		return nil
	}

	fns := map[string]func(*Cfg, string) error{
		"seed_brokers":   func(c *Cfg, v string) error { return intoStrSlice(v, &c.SeedBrokers) },
		"broker_timeout": func(c *Cfg, v string) error { return intoDuration(v, &c.BrokerTimeout) },
		"dial_timeout":   func(c *Cfg, v string) error { return intoDuration(v, &c.DialTimeout) },
		"retry_timeout":  func(c *Cfg, v string) error { return intoDuration(v, &c.RetryTimeout) },
		// Removed in favor of duration-based names above.
		"timeout_ms": func(c *Cfg, v string) error {
			return fmt.Errorf("timeout_ms was renamed to broker_timeout and now takes a Go duration (e.g. -X broker_timeout=5s); please update your config or -X flags")
		},
		"use_tls":               func(c *Cfg, _ string) error { mktls(c); return nil },
		"tls.ca_cert_path":      func(c *Cfg, v string) error { mktls(c); c.TLS.CACert = v; return nil },
		"tls.client_cert_path":  func(c *Cfg, v string) error { mktls(c); c.TLS.ClientCertPath = v; return nil },
		"tls.client_key_path":   func(c *Cfg, v string) error { mktls(c); c.TLS.ClientKeyPath = v; return nil },
		"tls.insecure":          func(c *Cfg, _ string) error { mktls(c); c.TLS.InsecureSkipVerify = true; return nil },
		"tls.server_name":       func(c *Cfg, v string) error { mktls(c); c.TLS.ServerName = v; return nil },
		"tls.min_version":       func(c *Cfg, v string) error { mktls(c); c.TLS.MinVersion = v; return nil },
		"tls.cipher_suites":     func(c *Cfg, v string) error { mktls(c); return intoStrSlice(v, &c.TLS.CipherSuites) },
		"tls.curve_preferences": func(c *Cfg, v string) error { mktls(c); return intoStrSlice(v, &c.TLS.CurvePreferences) },
		"sasl.method":           func(c *Cfg, v string) error { mksasl(c); c.SASL.Method = v; return nil },
		"sasl.zid":              func(c *Cfg, v string) error { mksasl(c); c.SASL.Zid = v; return nil },
		"sasl.user":             func(c *Cfg, v string) error { mksasl(c); c.SASL.User = v; return nil },
		"sasl.pass":             func(c *Cfg, v string) error { mksasl(c); c.SASL.Pass = v; return nil },
		"sasl.is_token":         func(c *Cfg, _ string) error { mksasl(c); c.SASL.IsToken = true; return nil }, // accepts any val

		"registry.urls":                  func(c *Cfg, v string) error { mksr(c); return intoStrSlice(v, &c.SR.URLs) },
		"registry.user":                  func(c *Cfg, v string) error { mksr(c); c.SR.User = v; return nil },
		"registry.pass":                  func(c *Cfg, v string) error { mksr(c); c.SR.Pass = v; return nil },
		"registry.bearer_token":          func(c *Cfg, v string) error { mksr(c); c.SR.BearerToken = v; return nil },
		"registry.context":               func(c *Cfg, v string) error { mksr(c); c.SR.Context = v; return nil },
		"registry.tls.ca_cert_path":      func(c *Cfg, v string) error { mksrtls(c); c.SR.TLS.CACert = v; return nil },
		"registry.tls.client_cert_path":  func(c *Cfg, v string) error { mksrtls(c); c.SR.TLS.ClientCertPath = v; return nil },
		"registry.tls.client_key_path":   func(c *Cfg, v string) error { mksrtls(c); c.SR.TLS.ClientKeyPath = v; return nil },
		"registry.tls.insecure":          func(c *Cfg, _ string) error { mksrtls(c); c.SR.TLS.InsecureSkipVerify = true; return nil },
		"registry.tls.server_name":       func(c *Cfg, v string) error { mksrtls(c); c.SR.TLS.ServerName = v; return nil },
		"registry.tls.min_version":       func(c *Cfg, v string) error { mksrtls(c); c.SR.TLS.MinVersion = v; return nil },
		"registry.tls.cipher_suites":     func(c *Cfg, v string) error { mksrtls(c); return intoStrSlice(v, &c.SR.TLS.CipherSuites) },
		"registry.tls.curve_preferences": func(c *Cfg, v string) error { mksrtls(c); return intoStrSlice(v, &c.SR.TLS.CurvePreferences) },
	}

	// The canonical keys above are dot-separated by field. We index by the
	// flattened (dot->underscore) form so that both the dotted form and the
	// legacy pure-underscore form (e.g. "sasl_user") resolve to the same
	// handler. See normCfgKey.
	flat := make(map[string]func(*Cfg, string) error, len(fns))
	for k, fn := range fns {
		flat[normCfgKey(k)] = fn
	}
	return flat
}()

// applyCfgOpts applies key=value overrides to cfg in order.
func applyCfgOpts(cfg *Cfg, opts []string) error {
	for _, opt := range opts {
		k, v, ok := strings.Cut(opt, "=")
		if !ok {
			return fmt.Errorf("opt %q not a key=value", opt)
		}
		fn, exists := cfgSetters[normCfgKey(k)]
		if !exists {
			return fmt.Errorf("unknown opt key %q", k)
		}
		if err := fn(cfg, v); err != nil {
			return err
		}
	}
	return nil
}

// applyShorthandFlags applies -B and -R, which win over any other setting
// of seed_brokers and registry.urls.
func (c *Client) applyShorthandFlags(cfg *Cfg) {
	if len(c.bootstrapServers) > 0 {
		cfg.SeedBrokers = c.bootstrapServers
	}
	if len(c.registryURLs) > 0 {
		if cfg.SR == nil {
			cfg.SR = new(CfgSR)
		}
		cfg.SR.URLs = c.registryURLs
	}
}

func (c *Client) processOverrides() {
	// Environment variables use the flattened (underscore) form, uppercased,
	// since env var names cannot contain dots: KCL_REGISTRY_TLS_SERVER_NAME.
	var envOverrides []string
	for k := range cfgSetters {
		if v, exists := os.LookupEnv(c.envPfx + strings.ToUpper(k)); exists {
			envOverrides = append(envOverrides, k+"="+v)
		}
	}
	if err := applyCfgOpts(&c.cfg, envOverrides); err != nil {
		out.Die("%s", err)
	}
	if err := applyCfgOpts(&c.cfg, c.flagOverrides); err != nil {
		out.Die("%s", err)
	}
	c.applyShorthandFlags(&c.cfg)
}

// FlagCfg returns the configuration given on the command line through -X,
// -B, and -R alone. The config file, environment variables, and defaults are
// not consulted. This is what "kcl profile create" saves.
func (c *Client) FlagCfg() (Cfg, error) {
	var cfg Cfg
	if err := applyCfgOpts(&cfg, c.flagOverrides); err != nil {
		return Cfg{}, err
	}
	c.applyShorthandFlags(&cfg)
	return cfg, nil
}

func (c *Client) maybeAddMaxVersions() {
	if c.asVersion != "" {
		versions := kversion.FromString(c.asVersion)
		if versions == nil {
			out.Die("unknown Kafka version %s", c.asVersion)
		}
		c.AddOpt(kgo.MaxVersions(versions))
	}
}

func (c *Client) maybeAddSASL() error {
	if c.cfg.SASL == nil {
		return nil
	}

	method := Strnorm(c.cfg.SASL.Method)

	switch method {
	case "":
	case "plain":
		c.AddOpt(kgo.SASL(plain.Plain(func(context.Context) (plain.Auth, error) {
			return plain.Auth{
				Zid:  c.cfg.SASL.Zid,
				User: c.cfg.SASL.User,
				Pass: c.cfg.SASL.Pass,
			}, nil
		})))
	case "scramsha256":
		c.AddOpt(kgo.SASL(scram.Auth{
			Zid:     c.cfg.SASL.Zid,
			User:    c.cfg.SASL.User,
			Pass:    c.cfg.SASL.Pass,
			IsToken: c.cfg.SASL.IsToken,
		}.AsSha256Mechanism()))
	case "scramsha512":
		c.AddOpt(kgo.SASL(scram.Auth{
			Zid:     c.cfg.SASL.Zid,
			User:    c.cfg.SASL.User,
			Pass:    c.cfg.SASL.Pass,
			IsToken: c.cfg.SASL.IsToken,
		}.AsSha512Mechanism()))
	case "awsmskiam":
		awscfg, err := config.LoadDefaultConfig(context.Background())
		out.MaybeDie(err, "unable to create aws session: %v", err)

		c.AddOpt(kgo.SASL(aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
			creds, err := awscfg.Credentials.Retrieve(ctx)
			if err != nil {
				return aws.Auth{}, err
			}
			return aws.Auth{
				AccessKey:    creds.AccessKeyID,
				SecretKey:    creds.SecretAccessKey,
				SessionToken: creds.SessionToken,
			}, nil
		})))

	default:
		return fmt.Errorf("unrecognized / unhandled sasl method %q", c.cfg.SASL.Method)
	}
	return nil
}

func (c *Client) loadTLS() (*tls.Config, error) {
	return buildTLS(c.cfg.TLS)
}

// buildTLS builds a *tls.Config from a CfgTLS. It returns (nil, nil) if the
// config is nil, meaning TLS is not requested. This is shared by the Kafka
// client and the Schema Registry client.
func buildTLS(cfg *CfgTLS) (*tls.Config, error) {
	if cfg == nil {
		return nil, nil
	}

	tc := new(tls.Config)

	tc.InsecureSkipVerify = cfg.InsecureSkipVerify
	switch strings.ToLower(cfg.MinVersion) {
	case "", "v1.2", "1.2":
		tc.MinVersion = tls.VersionTLS12 // the default
	case "v1.3", "1.3":
		tc.MinVersion = tls.VersionTLS13
	case "v1.1", "1.1":
		tc.MinVersion = tls.VersionTLS11
	case "v1.0", "1.0":
		tc.MinVersion = tls.VersionTLS10
	default:
		return nil, fmt.Errorf("unrecognized tls min version %s", cfg.MinVersion)
	}

	if suites := cfg.CipherSuites; len(suites) > 0 {
		potentials := make(map[string]uint16)
		for _, suite := range append(tls.CipherSuites(), tls.InsecureCipherSuites()...) {
			potentials[Strnorm(suite.Name)] = suite.ID
			potentials[Strnorm(strings.TrimPrefix("TLS_", suite.Name))] = suite.ID
		}

		for _, suite := range cfg.CipherSuites {
			id, exists := potentials[Strnorm(suite)]
			if !exists {
				return nil, fmt.Errorf("unknown cipher suite %s", suite)
			}
			tc.CipherSuites = append(tc.CipherSuites, id)
		}
	}

	if curves := cfg.CurvePreferences; len(curves) > 0 {
		potentials := map[string]tls.CurveID{
			"curvep256": tls.CurveP256,
			"curvep384": tls.CurveP384,
			"curvep521": tls.CurveP521,
			"x25519":    tls.X25519,
		}
		for _, curve := range cfg.CurvePreferences {
			id, exists := potentials[Strnorm(curve)]
			if !exists {
				return nil, fmt.Errorf("unknown curve preference %s", curve)
			}
			tc.CurvePreferences = append(tc.CurvePreferences, id)
		}
	}

	if cfg.CACert != "" {
		ca, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("unable to read CA file %q: %v",
				cfg.CACert, err)
		}

		tc.RootCAs = x509.NewCertPool()
		tc.RootCAs.AppendCertsFromPEM(ca)
	}

	if cfg.ClientCertPath != "" ||
		cfg.ClientKeyPath != "" {

		if cfg.ClientCertPath == "" ||
			cfg.ClientKeyPath == "" {
			return nil, errors.New("both client and key cert paths must be specified, but saw only one")
		}

		cert, err := os.ReadFile(cfg.ClientCertPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read client cert file %q: %v",
				cfg.ClientCertPath, err)
		}
		key, err := os.ReadFile(cfg.ClientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read client key file %q: %v",
				cfg.ClientKeyPath, err)
		}

		pair, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("unable to create key pair: %v", err)
		}

		tc.Certificates = append(tc.Certificates, pair)

	}

	return tc, nil
}

func (c *Client) parseLogLevel() {
	var level kgo.LogLevel
	switch ll := strings.ToLower(c.logLevel); ll {
	default:
		out.Die("unknown log level %q", ll)
	case "none":
		return // no opt added
	case "error":
		level = kgo.LogLevelError
	case "warn":
		level = kgo.LogLevelWarn
	case "info":
		level = kgo.LogLevelInfo
	case "debug":
		level = kgo.LogLevelDebug
	}
	var of *os.File
	switch c.logFile {
	case "STDOUT":
		of = os.Stdout
	case "STDERR":
		of = os.Stderr
	default:
		f, err := os.OpenFile(c.logFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o666)
		out.MaybeDie(err, "unable to open log-file %q: %v", c.logFile, err)
		of = f
	}
	c.opts = append(c.opts, kgo.WithLogger(kgo.BasicLogger(of, level, nil)))
}

// normCfgKey flattens a -X / env config key to a canonical match form. The
// documented, canonical key form is dot-separated by struct field (e.g.
// "sasl.user", "registry.tls.server_name"), which keeps any underscores
// unambiguously within a single field name. Matching is done on the flattened
// (dot->underscore, lowercased) form so that the legacy pure-underscore form
// ("sasl_user", "registry_tls_server_name") from older configs and scripts
// still resolves to the same handler.
func normCfgKey(k string) string {
	return strings.ReplaceAll(strings.ToLower(k), ".", "_")
}

func Strnorm(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	return s
}
