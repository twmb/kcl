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
	"strconv"
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
// D returns the duration, or zero when d is nil, which is what an unset
// timeout means.
func (d *Duration) D() time.Duration {
	if d == nil {
		return 0
	}
	return time.Duration(*d)
}

// Dur returns d as a *Duration, for the timeout fields of Cfg.
func Dur(d time.Duration) *Duration {
	dd := Duration(d)
	return &dd
}

// Cfg contains kcl options that can be defined in a file.
type Cfg struct {
	SeedBrokers []string `toml:"seed_brokers,omitempty"`

	// BrokerTimeout is the wire TimeoutMs value sent to the broker
	// in admin-style requests (e.g. CreateTopics.TimeoutMs). It
	// tells the broker how long to wait before giving up on the
	// server side. The timeouts are pointers so that a key written
	// as zero is kept apart from a key that is not set at all.
	BrokerTimeout *Duration `toml:"broker_timeout,omitempty"`

	// DialTimeout bounds how long kgo waits for a single TCP dial.
	// Zero leaves kgo's default (10s).
	DialTimeout *Duration `toml:"dial_timeout,omitempty"`

	// RetryTimeout bounds total time for a client request and its
	// retries. Zero leaves kgo's default (30s for most requests,
	// 45s for group-session requests).
	RetryTimeout *Duration `toml:"retry_timeout,omitempty"`

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

// defaultCfg is what kcl runs with when nothing sets a key. The config file,
// environment, and flags are laid over it.
func defaultCfg() Cfg {
	return Cfg{
		SeedBrokers:   []string{"localhost:9092"},
		BrokerTimeout: Dur(5 * time.Second),
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
		cfg: defaultCfg(),
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

	// Profiles decode as primitives so that the selected one can be laid
	// over the defaults already in c.cfg: only keys present in the file
	// change anything, and a key written as zero stays zero.
	var raw struct {
		CurrentProfile string                    `toml:"current_profile"`
		Profiles       map[string]toml.Primitive `toml:"profiles"`
		Cfg
	}
	md, err := toml.DecodeFile(c.cfgPath, &raw)
	if os.IsNotExist(err) {
		// A missing file is the same as --no-config-file.
		return
	}
	if err != nil {
		out.Die("unable to decode config file %q: %v", c.cfgPath, err)
	}

	c.cfgFile = CfgFile{CurrentProfile: raw.CurrentProfile, Cfg: raw.Cfg}
	if len(raw.Profiles) > 0 {
		c.cfgFile.Profiles = make(map[string]Cfg, len(raw.Profiles))
	}
	for name, prim := range raw.Profiles {
		var p Cfg
		if err := md.PrimitiveDecode(prim, &p); err != nil {
			out.Die("unable to decode profile %q in %s: %v", name, c.cfgPath, err)
		}
		c.cfgFile.Profiles[name] = p
	}

	// Warn on unknown keys so typos and stale names from old configs do
	// not get silently dropped. This catches "timeout_ms" after the
	// rename, "tls_xxx" typos, etc. Keys inside profiles count as
	// undecoded until PrimitiveDecode above has seen them.
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		for _, k := range undecoded {
			fmt.Fprintf(os.Stderr, "kcl: warning: unknown config key %q in %s\n", k, c.cfgPath)
		}
	}

	if len(raw.Profiles) > 0 {
		name := raw.CurrentProfile
		if c.profileName != "" {
			name = c.profileName
		}
		if name == "" {
			out.Die("config has profiles but no current_profile set; use --profile or set current_profile in config")
		}
		prim, ok := raw.Profiles[name]
		if !ok {
			out.Die("profile %q not found in config file", name)
		}
		if err := md.PrimitiveDecode(prim, &c.cfg); err != nil {
			out.Die("unable to decode profile %q in %s: %v", name, c.cfgPath, err)
		}
		return
	}

	// No profiles: the flat layout. Decoding the file straight into c.cfg
	// lays its keys over the defaults the same way.
	if _, err := toml.DecodeFile(c.cfgPath, &c.cfg); err != nil {
		out.Die("unable to decode config file %q: %v", c.cfgPath, err)
	}
}

// CfgFilePath returns the path to the config file.
func (c *Client) CfgFilePath() string {
	return c.cfgPath
}

// ProfileName returns the profile named by -C, or "" if none was given.
func (c *Client) ProfileName() string {
	return c.profileName
}

// LoadedCfgFile returns the full loaded config file (may include contexts).
func (c *Client) LoadedCfgFile() CfgFile {
	c.loadClientOnce()
	return c.cfgFile
}

// CfgKey is one -X key: its dotted name, what it does, and its Type, one of
// string, list, duration, bool, or table. A bool may be given bare
// (-X tls.insecure) to mean true. A table takes only the empty value, which
// removes the whole table. An empty value (-X sasl.pass=) unsets any key.
type CfgKey struct {
	Name string
	Desc string
	Type string

	set    func(*Cfg, string) error
	hidden bool // an old name that only errors
}

// CfgKeys returns every -X key kcl accepts, in display order.
func CfgKeys() []CfgKey {
	var keys []CfgKey
	for _, k := range cfgKeys {
		if !k.hidden {
			keys = append(keys, k)
		}
	}
	return keys
}

// A cfgTable knows how to find, create, and remove one optional table of
// Cfg. Unsetting a key in a table that does not exist leaves the table
// absent, since an empty [tls] table is itself a setting.
type cfgTable struct {
	has func(*Cfg) bool
	mk  func(*Cfg)
	rm  func(*Cfg)
}

var (
	topTable = cfgTable{has: func(*Cfg) bool { return true }, mk: func(*Cfg) {}}

	tlsTable = cfgTable{
		has: func(c *Cfg) bool { return c.TLS != nil },
		mk: func(c *Cfg) {
			if c.TLS == nil {
				c.TLS = new(CfgTLS)
			}
		},
		rm: func(c *Cfg) { c.TLS = nil },
	}

	saslTable = cfgTable{
		has: func(c *Cfg) bool { return c.SASL != nil },
		mk: func(c *Cfg) {
			if c.SASL == nil {
				c.SASL = new(CfgSASL)
			}
		},
		rm: func(c *Cfg) { c.SASL = nil },
	}

	srTable = cfgTable{
		has: func(c *Cfg) bool { return c.SR != nil },
		mk: func(c *Cfg) {
			if c.SR == nil {
				c.SR = new(CfgSR)
			}
		},
		rm: func(c *Cfg) { c.SR = nil },
	}

	srTLSTable = cfgTable{
		has: func(c *Cfg) bool { return c.SR != nil && c.SR.TLS != nil },
		mk: func(c *Cfg) {
			srTable.mk(c)
			if c.SR.TLS == nil {
				c.SR.TLS = new(CfgTLS)
			}
		},
		rm: func(c *Cfg) {
			if c.SR != nil {
				c.SR.TLS = nil
			}
		},
	}
)

func intoStrSlice(in string, dst *[]string) error {
	*dst = nil
	for _, on := range strings.Split(in, ",") {
		on = strings.TrimSpace(on)
		if len(on) == 0 {
			return fmt.Errorf("invalid empty value in %q", in)
		}
		*dst = append(*dst, on)
	}
	return nil
}

func str(name, desc string, t cfgTable, f func(*Cfg) *string) CfgKey {
	return CfgKey{Name: name, Desc: desc, Type: "string", set: func(c *Cfg, v string) error {
		if v == "" && !t.has(c) {
			return nil
		}
		t.mk(c)
		*f(c) = v
		return nil
	}}
}

func boolean(name, desc string, t cfgTable, f func(*Cfg) *bool) CfgKey {
	return CfgKey{Name: name, Desc: desc, Type: "bool", set: func(c *Cfg, v string) error {
		b, err := parseBoolOpt(v)
		if err != nil {
			return err
		}
		if !b && !t.has(c) {
			return nil
		}
		t.mk(c)
		*f(c) = b
		return nil
	}}
}

func list(name, desc string, t cfgTable, f func(*Cfg) *[]string) CfgKey {
	return CfgKey{Name: name, Desc: desc, Type: "list", set: func(c *Cfg, v string) error {
		if v == "" {
			if t.has(c) {
				*f(c) = nil
			}
			return nil
		}
		t.mk(c)
		return intoStrSlice(v, f(c))
	}}
}

func duration(name, desc string, f func(*Cfg) **Duration) CfgKey {
	return CfgKey{Name: name, Desc: desc, Type: "duration", set: func(c *Cfg, v string) error {
		if v == "" {
			*f(c) = nil
			return nil
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %v", v, err)
		}
		*f(c) = Dur(d)
		return nil
	}}
}

func table(name, desc string, t cfgTable) CfgKey {
	return CfgKey{Name: name, Desc: desc, Type: "table", set: func(c *Cfg, v string) error {
		if v != "" {
			return fmt.Errorf("%s is a table: set its keys, or %s= to remove it", name, name)
		}
		t.rm(c)
		return nil
	}}
}

// parseBoolOpt reads a boolean -X value; empty is false, which unsets.
func parseBoolOpt(v string) (bool, error) {
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("invalid boolean %q", v)
	}
	return b, nil
}

func tlsKeys(prefix string, t cfgTable, tls func(*Cfg) *CfgTLS, suffix string) []CfgKey {
	return []CfgKey{
		table(prefix, "The TLS table. "+prefix+"= removes it, turning TLS off"+suffix+".", t),
		str(prefix+".ca_cert_path", "PEM file holding the CA that signed the server certificates"+suffix+".", t, func(c *Cfg) *string { return &tls(c).CACert }),
		str(prefix+".client_cert_path", "PEM client certificate, for mutual TLS"+suffix+".", t, func(c *Cfg) *string { return &tls(c).ClientCertPath }),
		str(prefix+".client_key_path", "PEM client key, for mutual TLS"+suffix+".", t, func(c *Cfg) *string { return &tls(c).ClientKeyPath }),
		str(prefix+".server_name", "Name to verify the server certificate against, when it is not the host dialed"+suffix+".", t, func(c *Cfg) *string { return &tls(c).ServerName }),
		boolean(prefix+".insecure", "Skip certificate verification"+suffix+".", t, func(c *Cfg) *bool { return &tls(c).InsecureSkipVerify }),
		str(prefix+".min_version", "Lowest TLS version accepted: 1.0, 1.1, 1.2, or 1.3. Default 1.2"+suffix+".", t, func(c *Cfg) *string { return &tls(c).MinVersion }),
		list(prefix+".cipher_suites", "Cipher suites allowed, by Go name, comma separated"+suffix+".", t, func(c *Cfg) *[]string { return &tls(c).CipherSuites }),
		list(prefix+".curve_preferences", "Curves allowed for key exchange, comma separated"+suffix+".", t, func(c *Cfg) *[]string { return &tls(c).CurvePreferences }),
	}
}

// cfgKeys is every -X key, in the order kcl profile keys lists them.
var cfgKeys = func() []CfgKey {
	keys := []CfgKey{
		list("seed_brokers", "Brokers to connect to, host:port, comma separated. Default localhost:9092.", topTable, func(c *Cfg) *[]string { return &c.SeedBrokers }),
		duration("broker_timeout", "How long the broker may spend on an admin request, sent as the wire TimeoutMs. Default 5s.", func(c *Cfg) **Duration { return &c.BrokerTimeout }),
		duration("dial_timeout", "Bound on one TCP dial. Unset uses kgo's 10s.", func(c *Cfg) **Duration { return &c.DialTimeout }),
		duration("retry_timeout", "Bound on a request and its retries. Unset uses kgo's 30s, 45s for group requests.", func(c *Cfg) **Duration { return &c.RetryTimeout }),
		{Name: "timeout_ms", hidden: true, set: func(*Cfg, string) error {
			return fmt.Errorf("timeout_ms was renamed to broker_timeout and now takes a Go duration (e.g. -X broker_timeout=5s); please update your config or -X flags")
		}},
		{Name: "use_tls", Type: "bool", Desc: "true turns TLS on with the system roots; false removes the tls table.", set: func(c *Cfg, v string) error {
			b, err := parseBoolOpt(v)
			if err != nil {
				return err
			}
			if b {
				tlsTable.mk(c)
			} else {
				tlsTable.rm(c)
			}
			return nil
		}},
	}
	keys = append(keys, tlsKeys("tls", tlsTable, func(c *Cfg) *CfgTLS { return c.TLS }, "")...)
	keys = append(keys,
		table("sasl", "The SASL table. sasl= removes it.", saslTable),
		str("sasl.method", "plain, scram-sha-256, scram-sha-512, or aws_msk_iam.", saslTable, func(c *Cfg) *string { return &c.SASL.Method }),
		str("sasl.zid", "Authorization id, when it differs from the user.", saslTable, func(c *Cfg) *string { return &c.SASL.Zid }),
		str("sasl.user", "User name.", saslTable, func(c *Cfg) *string { return &c.SASL.User }),
		str("sasl.pass", "Password.", saslTable, func(c *Cfg) *string { return &c.SASL.Pass }),
		boolean("sasl.is_token", "The password is a delegation token.", saslTable, func(c *Cfg) *bool { return &c.SASL.IsToken }),
		table("registry", "The schema registry table. registry= removes it.", srTable),
		list("registry.urls", "Schema registry URLs, comma separated. Default http://localhost:8081.", srTable, func(c *Cfg) *[]string { return &c.SR.URLs }),
		str("registry.user", "Basic auth user name.", srTable, func(c *Cfg) *string { return &c.SR.User }),
		str("registry.pass", "Basic auth password.", srTable, func(c *Cfg) *string { return &c.SR.Pass }),
		str("registry.bearer_token", "Bearer token, in place of basic auth.", srTable, func(c *Cfg) *string { return &c.SR.BearerToken }),
		str("registry.context", "Registry context that scopes every request.", srTable, func(c *Cfg) *string { return &c.SR.Context }),
	)
	keys = append(keys, tlsKeys("registry.tls", srTLSTable, func(c *Cfg) *CfgTLS { return c.SR.TLS }, " for the registry")...)
	return keys
}()

// cfgSetters indexes cfgKeys by the flattened (dot->underscore) name, so
// both the dotted form and the legacy pure-underscore form (e.g.
// "sasl_user") resolve to the same key. See normCfgKey.
var cfgSetters = func() map[string]CfgKey {
	m := make(map[string]CfgKey, len(cfgKeys))
	for _, k := range cfgKeys {
		m[normCfgKey(k.Name)] = k
	}
	return m
}()

// ApplyCfgOpts applies -X style options to cfg in order. Each is KEY=VALUE,
// an empty VALUE unsets the key, and a boolean key may be given bare. The
// first bad option stops it with an error.
func ApplyCfgOpts(cfg *Cfg, opts []string) error {
	for _, opt := range opts {
		k, v, hasEq := strings.Cut(opt, "=")
		key, exists := cfgSetters[normCfgKey(k)]
		if !exists {
			return fmt.Errorf("unknown opt key %q; kcl profile keys lists them", k)
		}
		if !hasEq {
			if key.Type != "bool" {
				return fmt.Errorf("%s needs a value; %s= unsets it", k, k)
			}
			v = "true"
		}
		if err := key.set(cfg, v); err != nil {
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
	for k, key := range cfgSetters {
		if key.Type == "table" {
			continue
		}
		if v, exists := os.LookupEnv(c.envPfx + strings.ToUpper(k)); exists {
			envOverrides = append(envOverrides, k+"="+v)
		}
	}
	if err := ApplyCfgOpts(&c.cfg, envOverrides); err != nil {
		out.Die("%s", err)
	}
	if err := ApplyCfgOpts(&c.cfg, c.flagOverrides); err != nil {
		out.Die("%s", err)
	}
	c.applyShorthandFlags(&c.cfg)
}

// ApplyFlags applies the -X, -B, and -R flags to cfg, in that order so the
// shorthands win, and returns the keys they set. The config file,
// environment variables, and defaults are not consulted.
func (c *Client) ApplyFlags(cfg *Cfg) ([]string, error) {
	if err := ApplyCfgOpts(cfg, c.flagOverrides); err != nil {
		return nil, err
	}
	var keys []string
	for _, opt := range c.flagOverrides {
		k, _, _ := strings.Cut(opt, "=")
		keys = append(keys, k)
	}
	if len(c.bootstrapServers) > 0 {
		keys = append(keys, "seed_brokers")
	}
	if len(c.registryURLs) > 0 {
		keys = append(keys, "registry.urls")
	}
	c.applyShorthandFlags(cfg)
	return keys, nil
}

// FlagCfg returns the defaults with the -X, -B, and -R flags laid over them.
// This is what "kcl profile create" saves, so a new profile spells out what
// it runs with.
func (c *Client) FlagCfg() (Cfg, error) {
	cfg := defaultCfg()
	if _, err := c.ApplyFlags(&cfg); err != nil {
		return Cfg{}, err
	}
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
