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
	"path/filepath"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"

	"github.com/twmb/kgo"
	"github.com/twmb/kgo/kversion"
)

// The top half of this file is loading config.
// The bottom half is config commands.

var (
	// clientOpts is used when initializing a client. Commands can stuff
	// opts into this slice before creating the client for the first time
	// as necessary.
	clientOpts []kgo.Opt

	cfgPath      string
	noCfgFile    bool
	cfgOverrides []string

	asVersion string // which version of Kafka api versions to use

	asJSON bool // dump responses as JSON for commands that support it

	defaultCfgPath = ""
	configDirErr   error
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
	var configDir string
	configDir, configDirErr = os.UserConfigDir()
	if configDirErr == nil {
		defaultCfgPath = filepath.Join(configDir, "kcl", "config.toml")
	}

	root.PersistentFlags().StringVar(&cfgPath, "config-path", defaultCfgPath, "path to confile file (lowest priority)")
	root.PersistentFlags().BoolVarP(&noCfgFile, "no-config", "Z", false, "do not load any config file")
	root.PersistentFlags().StringArrayVarP(&cfgOverrides, "config-opt", "X", nil, "flag provided config option (highest priority)")
	root.PersistentFlags().StringVar(&asVersion, "as-version", "", "if nonempty, which version of Kafka versions to use (e.g. '0.8.0', '0.10.0', '1.0.0'; dots optional)")

	root.PersistentFlags().BoolVarP(&asJSON, "dump-json", "j", false, "dump response as json if supported")

	// Set config defaults here.
	cfg.SeedBrokers = []string{"localhost"}
	cfg.TimeoutMillis = 1000
}

func load() (*kgo.Client, error) {
	if asVersion != "" {
		var versions kversion.Versions
		switch asVersion {
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
			return nil, fmt.Errorf("unknown Kafka version %s", asVersion)
		}
		clientOpts = append(clientOpts, kgo.WithMaxVersions(versions))
	}

	if !noCfgFile {
		md, err := toml.DecodeFile(cfgPath, &cfg)
		if !os.IsNotExist(err) {
			if err != nil {
				return nil, fmt.Errorf("unable to decode config file %q: %v", cfgPath, err)
			}
			if len(md.Undecoded()) > 0 {
				return nil, fmt.Errorf("unknown keys in toml cfg: %v", md.Undecoded())
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
			return nil, fmt.Errorf("opt %q not a key=value", opt)
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
			return nil, err
		}
	}

	if tlsCfg, err := loadTLSCfg(); err != nil {
		return nil, err
	} else if tlsCfg != nil {
		clientOpts = append(clientOpts,
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

	clientOpts = append(clientOpts,
		kgo.WithSeedBrokers(cfg.SeedBrokers...),
	)
	c, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("unable to create client: %v", err)
	}
	return c, nil
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

func init() {
	root.AddCommand(configCmd())
}

func configCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "kcl configuration commands",
	}

	cmd.AddCommand(cfgDumpCmd())
	cmd.AddCommand(cfgHelpCmd())
	cmd.AddCommand(cfgUseCmd())
	cmd.AddCommand(cfgClearCmd())
	cmd.AddCommand(cfgListCmd())

	return cmd
}

func cfgDumpCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "dump",
		Short: "dump the loaded configuration",
		Run: func(_ *cobra.Command, _ []string) {
			client() // force a load of the config
			toml.NewEncoder(os.Stdout).Encode(cfg)
		},
	}
}

func cfgHelpCmd() *cobra.Command {
	var configHelp = `kcl takes configuration options by default from ` + defaultCfgPath + `.
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
	return &cobra.Command{
		Use:   "help",
		Short: "describe kcl config semantics",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println(configHelp)
		},
	}
}

func cfgUseCmd() *cobra.Command {
	dir := filepath.Dir(defaultCfgPath)

	return &cobra.Command{
		Use:   "use NAME",
		Short: "Link a config in " + dir + " to " + defaultCfgPath,
		Long: `Link a config in ` + dir + ` to ` + defaultCfgPath + `.

This command allows you to easily swap kcl configs. It will look for
any file NAME or NAME.toml (favoring .toml) in the config directory
and link it to the default config path.

This dies if NAME does not yet exist or on any other os errors.

If asked to use config "none", this simply remove an existing symlink.
`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if defaultCfgPath == "" {
				die("cannot use a config; unable to determine home dir: %v", configDirErr)
			}

			existing, err := os.Lstat(defaultCfgPath)
			if err != nil {
				if !os.IsNotExist(err) {
					die("stat err for existing config path %q: %v", defaultCfgPath, err)
				}
				// not exists: we can create symlink
			} else {
				if existing.Mode()&os.ModeSymlink == 0 {
					die("stat shows that existing config at %q is not a symlink", defaultCfgPath)
				}
			}

			dirents, err := ioutil.ReadDir(dir)
			maybeDie(err, "unable to read config dir %q: %v", dir, err)

			use := args[0]
			exact := strings.HasSuffix(use, ".toml")
			found := false
			for _, dirent := range dirents {
				if exact && dirent.Name() == use {
					found = true
					break
				}

				// With inexact matching, we favor
				// foo.toml over foo if both exist.
				noExt := strings.TrimSuffix(dirent.Name(), ".toml")
				if noExt == use {
					found = true
					if len(dirent.Name()) > len(use) {
						use = dirent.Name()
					}
				}
			}

			if !found {
				die("could not find requested config %q", args[0])
			}

			if existing != nil {
				if err := os.Remove(defaultCfgPath); err != nil {
					die("unable to remove old symlink at %q: %v", defaultCfgPath, err)
				}
			}

			src := filepath.Join(dir, use)
			if err := os.Symlink(src, defaultCfgPath); err != nil {
				die("unable to create symlink from %q to %q: %v", src, defaultCfgPath, err)
			}

			fmt.Printf("linked %q to %q\n", src, defaultCfgPath)
		},
	}
}

func cfgClearCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clear",
		Short: "Remove " + defaultCfgPath + " if it is a symlink",
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			if defaultCfgPath == "" {
				die("cannot use a config; unable to determine config dir: %v", configDirErr)
			}

			existing, err := os.Lstat(defaultCfgPath)
			if err != nil {
				if !os.IsNotExist(err) {
					die("stat err for existing config path %q: %v", defaultCfgPath, err)
				}
				fmt.Printf("no symlink found at %q\n", defaultCfgPath)
				return
			}

			if existing.Mode()&os.ModeSymlink == 0 {
				die("stat shows that existing config at %q is not a symlink", defaultCfgPath)
			}

			if err := os.Remove(defaultCfgPath); err != nil {
				die("unable to remove symlink at %q: %v", defaultCfgPath, err)
			}
			fmt.Printf("cleared config symlink %q\n", defaultCfgPath)
		},
	}
}

func cfgListCmd() *cobra.Command {
	dir := filepath.Dir(defaultCfgPath)

	return &cobra.Command{
		Use:   "lsdir",
		Short: "List all files in configuration directory" + defaultCfgPath,
		Args:  cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			dirents, err := ioutil.ReadDir(dir)
			maybeDie(err, "unable to read config dir %q: %v", dir, err)
			for _, dirent := range dirents {
				fmt.Println(dirent.Name())
			}
		},
	}
}
