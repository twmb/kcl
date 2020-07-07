// Package client is the kcl client, containing global options and
// a kgo.Client builder.
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"

	"github.com/twmb/kafka-go/pkg/kgo"
	"github.com/twmb/kafka-go/pkg/kmsg"
	"github.com/twmb/kafka-go/pkg/kversion"
	"github.com/twmb/kafka-go/pkg/sasl/plain"
	"github.com/twmb/kafka-go/pkg/sasl/scram"

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
}

type CfgSASL struct {
	Method  string `toml:"method,omitempty"`
	Zid     string `toml:"zid,omitempty"`
	User    string `toml:"user,omitempty"`
	Pass    string `toml:"pass,omitempty"`
	IsToken bool   `toml:"is_token,omitempty"`
}

// cfg contains kcl options that can be defined in a file.
type Cfg struct {
	SeedBrokers []string `toml:"seed_brokers,omitempty"`

	TimeoutMillis int32 `toml:"timeout_ms,omitempty"`

	TLS  *CfgTLS `toml:"tls,omitempty"`
	SASL CfgSASL `toml:"sasl,omitempty"`
}

// Client contains kgo client options and a kgo client.
type Client struct {
	opts   []kgo.Opt
	once   sync.Once
	client *kgo.Client

	logLevel string
	logFile  string

	asVersion string
	asJSON    bool

	// config options parsed and filled on load
	defaultCfgPath string
	cfgPath        string
	noCfgFile      bool
	envPfx         string
	flagOverrides  []string
	cfg            Cfg
}

// AsJSON returns whether the output should be dumped as JSON if applicable.
func (c *Client) AsJSON() bool { return c.asJSON }

// TimeoutMillis is what requests that have timeouts should use.
func (c *Client) TimeoutMillis() int32 { return c.cfg.TimeoutMillis }

// New returns a new Client with the given config and installs some
// persistent flags and commands to root.
func New(root *cobra.Command) *Client {
	c := &Client{
		opts: []kgo.Opt{
			kgo.MetadataMinAge(time.Second),
		},
		cfg: Cfg{
			SeedBrokers:   []string{"localhost:9092"},
			TimeoutMillis: 5000,
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

	root.PersistentFlags().StringVar(&c.logLevel, "log-level", "none", "log level to use for basic logging (none, error, warn, info, debug)")
	root.PersistentFlags().StringVar(&c.logFile, "log-file", "", "log to this file rather than STDERR (if log-level is not none; file must not exist)")
	root.PersistentFlags().StringVar(&c.cfgPath, "config-path", c.defaultCfgPath, "path to confile file (lowest priority)")
	root.PersistentFlags().BoolVar(&c.noCfgFile, "no-config", false, "do not load any config file")
	root.PersistentFlags().StringVar(&c.envPfx, "config-env-prefix", "KCL_", "environment variable prefix for config overrides (middle priority)")
	root.PersistentFlags().StringArrayVarP(&c.flagOverrides, "config-opt", "X", nil, "flag provided config option (highest priority)")
	root.PersistentFlags().StringVar(&c.asVersion, "as-version", "", "if nonempty, which version of Kafka versions to use (e.g. '0.8.0', '2.3.0')")
	root.PersistentFlags().BoolVarP(&c.asJSON, "dump-json", "j", false, "dump response as json if supported")

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

// DiskCfg returns the loaded disk configuration.
func (c *Client) DiskCfg() Cfg {
	c.loadClientOnce()
	return c.cfg
}

// DefaultCfgPath returns the default path that is used to load configs.
func (c *Client) DefaultCfgPath() string {
	return c.defaultCfgPath
}

func (c *Client) loadClientOnce() {
	c.once.Do(func() {
		c.fillOpts()
		var err error
		c.client, err = kgo.NewClient(c.opts...)
		out.MaybeDie(err, "unable to load client: %v", err)
	})
}

func (c *Client) fillOpts() {
	c.parseCfgFile()        // loads config file if needed
	c.processOverrides()    // overrides config values just loaded
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
		c.AddOpt(kgo.Dialer(func(_ context.Context, host string) (net.Conn, error) {
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
}

func (c *Client) parseCfgFile() {
	if !c.noCfgFile {
		md, err := toml.DecodeFile(c.cfgPath, &c.cfg)
		if !os.IsNotExist(err) {
			if err != nil {
				out.Die("unable to decode config file %q: %v", c.cfgPath, err)
			}
			if len(md.Undecoded()) > 0 {
				out.Die("unknown keys in toml cfg: %v", md.Undecoded())
			}
		}
	}
}

func (c *Client) processOverrides() {
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

	intoInt32 := func(in string, dst *int32) error {
		i, err := strconv.Atoi(in)
		if err != nil {
			return err
		}
		if i > math.MaxInt32 || i < 0 {
			return fmt.Errorf("invalid int32 value %s", in)
		}
		*dst = int32(i)
		return nil
	}

	mktls := func(c *Cfg) {
		if c.TLS == nil {
			c.TLS = new(CfgTLS)
		}
	}

	fns := map[string]func(*Cfg, string) error{
		"seed_brokers":         func(c *Cfg, v string) error { return intoStrSlice(v, &c.SeedBrokers) },
		"timeout_ms":           func(c *Cfg, v string) error { return intoInt32(v, &c.TimeoutMillis) },
		"use_tls":              func(c *Cfg, _ string) error { mktls(c); return nil },
		"tls_ca_cert_path":     func(c *Cfg, v string) error { mktls(c); c.TLS.CACert = v; return nil },
		"tls_client_cert_path": func(c *Cfg, v string) error { mktls(c); c.TLS.ClientCertPath = v; return nil },
		"tls_client_key_path":  func(c *Cfg, v string) error { mktls(c); c.TLS.ClientKeyPath = v; return nil },
		"tls_server_name":      func(c *Cfg, v string) error { mktls(c); c.TLS.ServerName = v; return nil },
		"sasl_method":          func(c *Cfg, v string) error { c.SASL.Method = v; return nil },
		"sasl_zid":             func(c *Cfg, v string) error { c.SASL.Zid = v; return nil },
		"sasl_user":            func(c *Cfg, v string) error { c.SASL.User = v; return nil },
		"sasl_pass":            func(c *Cfg, v string) error { c.SASL.Pass = v; return nil },
		"sasl_is_token":        func(c *Cfg, _ string) error { c.SASL.IsToken = true; return nil }, // accepts any val
	}

	parse := func(kvs []string) {
		for _, opt := range kvs {
			kv := strings.Split(opt, "=")
			if len(kv) != 2 {
				out.Die("opt %q not a key=value", opt)
			}
			k, v := kv[0], kv[1]

			fn, exists := fns[strings.ToLower(k)]
			if !exists {
				out.Die("unknown opt key %q", k)
			}
			if err := fn(&c.cfg, v); err != nil {
				out.Die("%s", err)
			}
		}
	}

	var envOverrides []string
	for k := range fns {
		if v, exists := os.LookupEnv(c.envPfx + strings.ToUpper(k)); exists {
			envOverrides = append(envOverrides, k+"="+v)
		}
	}

	parse(envOverrides)
	parse(c.flagOverrides)
}

func (c *Client) maybeAddMaxVersions() {
	if c.asVersion != "" {
		var versions kversion.Versions
		switch c.asVersion {
		case "0.8.0":
			versions = kversion.V0_8_0()
		case "0.8.1":
			versions = kversion.V0_8_1()
		case "0.8.2":
			versions = kversion.V0_8_2()
		case "0.9.0":
			versions = kversion.V0_9_0()
		case "0.10.0":
			versions = kversion.V0_10_0()
		case "0.10.1":
			versions = kversion.V0_10_1()
		case "0.10.2":
			versions = kversion.V0_10_2()
		case "0.11.0":
			versions = kversion.V0_11_0()
		case "1.0.0":
			versions = kversion.V1_0_0()
		case "1.1.0":
			versions = kversion.V1_1_0()
		case "2.0.0":
			versions = kversion.V2_0_0()
		case "2.1.0":
			versions = kversion.V2_1_0()
		case "2.2.0":
			versions = kversion.V2_2_0()
		case "2.3.0":
			versions = kversion.V2_3_0()
		case "2.4.0":
			versions = kversion.V2_4_0()
		case "2.5.0":
			versions = kversion.V2_5_0()
		default:
			out.Die("unknown Kafka version %s", c.asVersion)
		}
		c.AddOpt(kgo.MaxVersions(versions))
	}
}

func (c *Client) maybeAddSASL() error {
	method := strings.ToLower(strings.TrimSpace(c.cfg.SASL.Method))
	method = strings.Replace(method, "-", "", -1)
	method = strings.Replace(method, "_", "", -1)

	switch method {
	case "":
	case "plaintext":
		c.AddOpt(kgo.SASL(plain.Plain(func(context.Context) (plain.Auth, error) {
			return plain.Auth{
				Zid:  c.cfg.SASL.Zid,
				User: c.cfg.SASL.User,
				Pass: c.cfg.SASL.Pass,
			}, nil
		})))
	case "scramsha256":
		c.AddOpt(kgo.SASL(scram.Sha256(func(context.Context) (scram.Auth, error) {
			return scram.Auth{
				Zid:     c.cfg.SASL.Zid,
				User:    c.cfg.SASL.User,
				Pass:    c.cfg.SASL.Pass,
				IsToken: c.cfg.SASL.IsToken,
			}, nil
		})))
	case "scramsha512":
		c.AddOpt(kgo.SASL(scram.Sha512(func(context.Context) (scram.Auth, error) {
			return scram.Auth{
				Zid:     c.cfg.SASL.Zid,
				User:    c.cfg.SASL.User,
				Pass:    c.cfg.SASL.Pass,
				IsToken: c.cfg.SASL.IsToken,
			}, nil
		})))
	default:
		return fmt.Errorf("unrecognized / unhandled sasl method %q", c.cfg.SASL.Method)
	}
	return nil
}

func (c *Client) loadTLS() (*tls.Config, error) {
	if c.cfg.TLS == nil {
		return nil, nil
	}

	tlscfg := &tls.Config{
		MinVersion: tls.VersionTLS12,

		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			// ECDHE-{ECDSA,RSA}-AES256-SHA384 unavailable
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256, // for Kafka
		},

		CurvePreferences: []tls.CurveID{
			tls.X25519,
		},
	}

	if c.cfg.TLS.CACert != "" {
		ca, err := ioutil.ReadFile(c.cfg.TLS.CACert)
		if err != nil {
			return nil, fmt.Errorf("unable to read CA file %q: %v",
				c.cfg.TLS.CACert, err)
		}

		tlscfg.RootCAs = x509.NewCertPool()
		tlscfg.RootCAs.AppendCertsFromPEM(ca)
	}

	if c.cfg.TLS.ClientCertPath != "" ||
		c.cfg.TLS.ClientKeyPath != "" {

		if c.cfg.TLS.ClientCertPath == "" ||
			c.cfg.TLS.ClientKeyPath == "" {
			return nil, errors.New("both client and key cert paths must be specified, but saw only one")
		}

		cert, err := ioutil.ReadFile(c.cfg.TLS.ClientCertPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read client cert file %q: %v",
				c.cfg.TLS.ClientCertPath, err)
		}
		key, err := ioutil.ReadFile(c.cfg.TLS.ClientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read client key file %q: %v",
				c.cfg.TLS.ClientKeyPath, err)
		}

		pair, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("unable to create key pair: %v", err)
		}

		tlscfg.Certificates = append(tlscfg.Certificates, pair)

	}

	return tlscfg, nil
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
	of := os.Stderr
	if c.logFile != "" {
		f, err := os.OpenFile(c.logFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
		out.MaybeDie(err, "unable to open log-file %q: %v", c.logFile, err)
		of = f
	}
	c.opts = append(c.opts, kgo.WithLogger(kgo.BasicLogger(of, level, nil)))
}
