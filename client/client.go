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

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"
	"github.com/twmb/kgo"
	"github.com/twmb/kgo/kmsg"
	"github.com/twmb/kgo/kversion"

	"github.com/twmb/kcl/out"
)

// Requestor can either be a kgo.Client or kgo.Broker.
type Requestor interface {
	Request(context.Context, kmsg.Request) (kmsg.Response, error)
}

// cfg contains kcl options that can be defined in a file.
type Cfg struct {
	SeedBrokers []string `toml:"seed_brokers"`

	TimeoutMillis int32 `toml:"timeout_ms"`

	TLSCACert         string `toml:"tls_ca_cert_path"`
	TLSClientCertPath string `toml:"tls_client_cert_path"`
	TLSClientKeyPath  string `toml:"tls_client_key_path"`
	TLSServerName     string `toml:"tls_server_name"`
}

// Client contains kgo client options and a kgo client.
type Client struct {
	opts   []kgo.Opt
	once   sync.Once
	client *kgo.Client

	asVersion string
	asJSON    bool

	// config options parsed and filled on load
	defaultCfgPath string
	cfgPath        string
	noCfgFile      bool
	cfgOverrides   []string
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
		cfg: Cfg{
			SeedBrokers:   []string{"localhost"},
			TimeoutMillis: 5000,
		},
	}

	cfgDir, err := os.UserConfigDir()
	if err == nil {
		c.defaultCfgPath = filepath.Join(cfgDir, "kcl", "config.toml")
	}

	root.PersistentFlags().StringVar(&c.cfgPath, "config-path", c.defaultCfgPath, "path to confile file (lowest priority)")
	root.PersistentFlags().BoolVarP(&c.noCfgFile, "no-config", "Z", false, "do not load any config file")
	root.PersistentFlags().StringArrayVarP(&c.cfgOverrides, "config-opt", "X", nil, "flag provided config option (highest priority)")
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
	c.maybeAddMaxVersions() // fills WithMaxVersions if necessary

	tlscfg, err := c.loadTLS()
	if err != nil {
		out.Die("%s", err)
	} else if tlscfg != nil {
		c.AddOpt(kgo.WithDialFn(func(host string) (net.Conn, error) {
			cloned := tlscfg.Clone()
			if c.cfg.TLSServerName != "" {
				cloned.ServerName = c.cfg.TLSServerName
			} else if h, _, err := net.SplitHostPort(host); err == nil {
				cloned.ServerName = h
			}
			return tls.Dial("tcp", host, cloned)
		}))
	}
	c.AddOpt(kgo.WithSeedBrokers(c.cfg.SeedBrokers...))
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

	var err error
	for _, opt := range c.cfgOverrides {
		kv := strings.Split(opt, "=")
		if len(kv) != 2 {
			out.Die("opt %q not a key=value", opt)
		}
		k, v := kv[0], kv[1]

		switch k {
		default:
			err = fmt.Errorf("unknown opt key %q", k)
		case "seed_brokers":
			err = intoStrSlice(v, &c.cfg.SeedBrokers)
		case "timeout_ms":
			err = intoInt32(v, &c.cfg.TimeoutMillis)
		case "tls_ca_cert_path":
			c.cfg.TLSCACert = v
		case "tls_client_cert_path":
			c.cfg.TLSClientCertPath = v
		case "tls_client_key_path":
			c.cfg.TLSClientKeyPath = v
		case "tls_server_name":
			c.cfg.TLSServerName = v
		}
		if err != nil {
			out.Die("%s", err)
		}
	}
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
		default:
			out.Die("unknown Kafka version %s", c.asVersion)
		}
		c.AddOpt(kgo.WithMaxVersions(versions))
	}
}

func (c *Client) loadTLS() (*tls.Config, error) {
	if c.cfg.TLSCACert == "" &&
		c.cfg.TLSClientCertPath == "" &&
		c.cfg.TLSClientKeyPath == "" {
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

	if c.cfg.TLSCACert != "" {
		ca, err := ioutil.ReadFile(c.cfg.TLSCACert)
		if err != nil {
			return nil, fmt.Errorf("unable to read CA file %q: %v",
				c.cfg.TLSCACert, err)
		}

		tlscfg.RootCAs = x509.NewCertPool()
		tlscfg.RootCAs.AppendCertsFromPEM(ca)
	}

	if c.cfg.TLSClientCertPath != "" ||
		c.cfg.TLSClientKeyPath != "" {

		if c.cfg.TLSClientCertPath == "" ||
			c.cfg.TLSClientKeyPath == "" {
			return nil, errors.New("both client and key cert paths must be specified, but saw only one")
		}

		cert, err := ioutil.ReadFile(c.cfg.TLSClientCertPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read client cert file %q: %v",
				c.cfg.TLSClientCertPath, err)
		}
		key, err := ioutil.ReadFile(c.cfg.TLSClientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read client key file %q: %v",
				c.cfg.TLSClientKeyPath, err)
		}

		pair, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("unable to create key pair: %v", err)
		}

		tlscfg.Certificates = append(tlscfg.Certificates, pair)

	}

	return tlscfg, nil
}
