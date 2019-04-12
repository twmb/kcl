package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"

	"github.com/twmb/kgo"
)

var (
	cfgPath      string
	noCfgFile    bool
	cfgOverrides []string

	asJSON bool // dump responses as JSON for commands that support it
)

var cfg struct {
	SeedBrokers []string `toml:"seed_brokers"`

	TimeoutMillis int32 `toml:"timeout_ms"`

	TLSCACert         string `toml:"tls_ca_cert_path"`
	TLSClientCertPath string `toml:"tls_client_cert_path"`
	TLSClientKeyPath  string `toml:"tls_client_key_path"`
	TLSServerName     string `toml:"tls_server_name"`
}

func init() {
	defaultPath := ""
	home, err := os.UserHomeDir()
	if err == nil {
		defaultPath = home + "/.config/kcl/config.toml"
	}

	root.PersistentFlags().StringVar(&cfgPath, "config-path", defaultPath, "path to confile file (lowest priority)")
	root.PersistentFlags().BoolVarP(&noCfgFile, "no-config", "Z", false, "do not load any config file")
	root.PersistentFlags().StringArrayVarP(&cfgOverrides, "config-opt", "X", nil, "flag provided config option (highest priority)")

	root.PersistentFlags().BoolVarP(&asJSON, "dump-json", "j", false, "dump response as json if supported")

	// Set config defaults here.
	cfg.SeedBrokers = []string{"localhost"}
	cfg.TimeoutMillis = 1000
}

const configHelp = `kcl takes configuration options by default from $HOME/.config/kcl/config.toml.
The config path can be set with --config-path, and --no-config (-Z) can be used
to disable loading a config file entirely.

To show the configuration that kcl is running with, run 'kcl misc dump-config'.

The repeatable -X flag allows for specifying config options directly. Any flag
set option has higher precedence over config file options.

Options are described below, with examples being how they would look in a
config.toml. Overrides generally look the same, but quotes can be dropped and
arrays do not use brackets (-X foo=bar,baz).

OPTIONS

  seed_brokers=["localhost", "127.0.0.1:9092"]
     An inital set of brokers to use for connecting to your Kafka cluster.

  timeout_ms=1000
     Timeout to use for any command that takes a timeout.

  tls_ca_cert_path="/path/to/my/ca.cert"
     Path to a CA cert to load and use for connecting to brokers over TLS.

  tls_client_cert_path="/path/to/my/ca.cert"
     Path to a client cert to load and use for connecting to brokers over TLS.
     This must be paired with tls_client_key_path.

  tls_client_key_path="/path/to/my/ca.cert"
     Path to a client key to load and use for connecting to brokers over TLS.
     This must be paired with tls_client_cert_path.

  tls_server_name="127.0.0.1"
     Server name to use for connecting to brokers over TLS.

`

func load() error {
	if !noCfgFile {
		md, err := toml.DecodeFile(cfgPath, &cfg)
		if !os.IsNotExist(err) {
			if err != nil {
				return fmt.Errorf("unable to decode config file %q: %v", cfgPath, err)
			}
			if len(md.Undecoded()) > 0 {
				return fmt.Errorf("unknown keys in toml cfg: %v", md.Undecoded())
			}
		}
	}

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

	for _, opt := range cfgOverrides {
		kv := strings.Split(opt, "=")
		if len(kv) != 2 {
			return fmt.Errorf("opt %q not a key=value", opt)
		}
		k, v := kv[0], kv[1]

		switch k {
		default:
			err = fmt.Errorf("unknown opt key %q", k)
		case "seed_brokers":
			err = intoStrSlice(v, &cfg.SeedBrokers)
		case "timeout_ms":
			err = intoInt32(v, &cfg.TimeoutMillis)
		case "tls_ca_cert_path":
			cfg.TLSCACert = v
		case "tls_client_cert_path":
			cfg.TLSClientCertPath = v
		case "tls_client_key_path":
			cfg.TLSClientKeyPath = v
		case "tls_server_name":
			cfg.TLSServerName = v
		}

		if err != nil {
			return err
		}
	}

	var opts []kgo.Opt

	if tlsCfg, err := loadTLSCfg(); err != nil {
		return err
	} else if tlsCfg != nil {
		opts = append(opts,
			kgo.WithDialFn(func(host string) (net.Conn, error) {
				cloned := tlsCfg.Clone()
				if cfg.TLSServerName != "" {
					cloned.ServerName = cfg.TLSServerName
				} else if h, _, err := net.SplitHostPort(host); err == nil {
					cloned.ServerName = h
				}
				return tls.Dial("tcp", host, cloned)
			}))
	}

	client, err = kgo.NewClient(cfg.SeedBrokers, opts...)
	if err != nil {
		return fmt.Errorf("unable to create client: %v", err)
	}
	return nil
}

func loadTLSCfg() (*tls.Config, error) {
	if cfg.TLSCACert == "" &&
		cfg.TLSClientCertPath == "" &&
		cfg.TLSClientKeyPath == "" {
		return nil, nil
	}

	tlsCfg := &tls.Config{
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

	if cfg.TLSCACert != "" {
		ca, err := ioutil.ReadFile(cfg.TLSCACert)
		if err != nil {
			return nil, fmt.Errorf("unable to read CA file %q: %v",
				cfg.TLSCACert, err)
		}

		tlsCfg.RootCAs = x509.NewCertPool()
		tlsCfg.RootCAs.AppendCertsFromPEM(ca)
	}

	if cfg.TLSClientCertPath != "" ||
		cfg.TLSClientKeyPath != "" {

		if cfg.TLSClientCertPath == "" ||
			cfg.TLSClientKeyPath == "" {
			return nil, errors.New("both client and key cert paths must be specified, but saw only one")
		}

		cert, err := ioutil.ReadFile(cfg.TLSClientCertPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read client cert file %q: %v",
				cfg.TLSClientCertPath, err)
		}
		key, err := ioutil.ReadFile(cfg.TLSClientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read client key file %q: %v",
				cfg.TLSClientKeyPath, err)
		}

		pair, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("unable to create key pair: %v", err)
		}

		tlsCfg.Certificates = append(tlsCfg.Certificates, pair)

	}

	return tlsCfg, nil
}
