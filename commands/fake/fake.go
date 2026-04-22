// Package fake implements the `kcl fake` subcommand: start an in-process
// kfake cluster for testing, probing, and experimentation.
package fake

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/twmb/kcl/out"
)

// Command returns the `kcl fake` cobra command.
func Command() *cobra.Command {
	var (
		ports      []int
		logLevel   string
		dataDir    string
		syncWrites bool
		asVersion  string
		brokerCfgs []string
		seedTopics []string
		allowAuto  bool
		acls       bool
		saslUsers  []string
		pprofAddr  string
	)

	cmd := &cobra.Command{
		Use:   "fake",
		Short: "Start an in-process kfake cluster for testing.",
		Long: `Start an in-process kfake cluster for testing.

kcl fake runs a kfake cluster in-process and prints the listen addresses
to stdout. By default it starts three brokers on 127.0.0.1 ports
9092,9093,9094. Point any Kafka client at the printed addresses; SIGINT
or SIGTERM exits cleanly.

This is NOT a production broker. kfake implements the user-facing Kafka
protocol surface (produce, fetch, groups, transactions, ACLs, share
groups) but intentionally omits broker-to-broker / KRaft-internal
requests and is not performance-tuned.

State is in-memory by default. Pass -d/--data-dir PATH to persist
topics, records, consumer group offsets, and transactional producer
state across restarts. Pass --sync to fsync every write (slower but
safest).

EXAMPLES

Default three-broker cluster:

  kcl fake

Pick specific ports (the number of ports determines broker count):

  kcl fake --ports 19092,19093,19094
  kcl fake --ports 9092                # single broker

Persistent cluster:

  kcl fake -d /tmp/kfake --sync

Pretend to be Kafka 3.9 (caps advertised API versions):

  kcl fake --as-version 3.9

Seed topics at startup:

  kcl fake --seed-topic foo:10 --seed-topic bar:3

Custom broker config (repeatable):

  kcl fake -c group.consumer.heartbeat.interval.ms=500 \
           -c transactional.id.expiration.ms=60000

Test SASL + ACLs:

  kcl fake --acls --sasl plain:admin:pw --sasl scram-sha-256:alice:pw2

Tune log verbosity for debugging:

  kcl fake -l debug
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			level, err := parseLogLevel(logLevel)
			if err != nil {
				return out.Errf(out.ExitUsage, "%v", err)
			}
			bcfgs, err := parseBrokerConfigs(brokerCfgs)
			if err != nil {
				return out.Errf(out.ExitUsage, "%v", err)
			}
			seeds, err := parseSeedTopics(seedTopics)
			if err != nil {
				return out.Errf(out.ExitUsage, "%v", err)
			}
			supers, err := parseSASLUsers(saslUsers)
			if err != nil {
				return out.Errf(out.ExitUsage, "%v", err)
			}

			numBrokers := len(ports)

			// Default auto-created / seeded partition count scales with
			// broker count so the cluster distributes work meaningfully:
			//   1 broker  -> 1 partition
			//   2 brokers -> 2 partitions
			//   3+ brokers-> 3 partitions
			defaultParts := numBrokers
			if defaultParts > 3 {
				defaultParts = 3
			}

			opts := []kfake.Opt{
				kfake.NumBrokers(numBrokers),
				kfake.DefaultNumPartitions(defaultParts),
				kfake.WithLogger(kfake.BasicLogger(os.Stderr, level)),
				kfake.Ports(ports...),
			}
			if dataDir != "" {
				opts = append(opts, kfake.DataDir(dataDir))
			}
			if syncWrites {
				opts = append(opts, kfake.SyncWrites())
			}
			if allowAuto {
				opts = append(opts, kfake.AllowAutoTopicCreation())
			}
			if acls {
				opts = append(opts, kfake.EnableACLs())
			}
			if len(supers) > 0 {
				// Enabling SASL is implied by adding superusers.
				opts = append(opts, kfake.EnableSASL())
				for _, s := range supers {
					opts = append(opts, kfake.Superuser(s.mechanism, s.user, s.pass))
				}
			}
			if asVersion != "" {
				v := kversion.FromString(asVersion)
				if v == nil {
					return out.Errf(out.ExitUsage, "unknown Kafka version %q", asVersion)
				}
				opts = append(opts, kfake.MaxVersions(v))
			}
			if len(bcfgs) > 0 {
				opts = append(opts, kfake.BrokerConfigs(bcfgs))
			}
			for _, s := range seeds {
				opts = append(opts, kfake.SeedTopics(s.partitions, s.topic))
			}

			if pprofAddr != "" {
				addr := pprofAddr
				if !strings.Contains(addr, ":") {
					addr = "127.0.0.1:" + addr
				}
				go func() {
					fmt.Fprintf(os.Stderr, "pprof listening on %s\n", addr)
					if err := http.ListenAndServe(addr, nil); err != nil {
						fmt.Fprintf(os.Stderr, "pprof failed: %v\n", err)
					}
				}()
			}

			c, err := kfake.NewCluster(opts...)
			if err != nil {
				return fmt.Errorf("unable to start fake cluster: %v", err)
			}

			for _, addr := range c.ListenAddrs() {
				fmt.Println(addr)
			}

			sigs := make(chan os.Signal, 2)
			signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
			<-sigs
			if dataDir != "" {
				// Give persistent shutdown a chance to drain; a second
				// signal forces immediate exit.
				fmt.Fprintln(os.Stderr, "shutting down (Ctrl+C again to force)...")
				done := make(chan struct{})
				go func() {
					c.Close()
					close(done)
				}()
				select {
				case <-done:
				case <-sigs:
					fmt.Fprintln(os.Stderr, "forced shutdown")
					os.Exit(1)
				}
			} else {
				c.Close()
			}
			return nil
		},
	}

	cmd.Flags().IntSliceVar(&ports, "ports", []int{9092, 9093, 9094}, "ports for brokers (comma-separated; broker count = number of ports)")
	cmd.Flags().StringVarP(&logLevel, "log-level", "l", "none", "kfake log level: none, error, warn, info, debug")
	cmd.Flags().StringVarP(&dataDir, "data-dir", "d", "", "persist state under this directory across restarts (default: in-memory only)")
	cmd.Flags().BoolVar(&syncWrites, "sync", false, "fsync every write for immediate durability (slower)")
	cmd.Flags().StringVar(&asVersion, "as-version", "", "pretend to be this Kafka version (e.g. 3.9, 4.0); caps advertised API versions")
	cmd.Flags().StringArrayVarP(&brokerCfgs, "broker-config", "c", nil, "broker config key=value (repeatable; applied at startup)")
	cmd.Flags().StringSliceVar(&seedTopics, "seed-topic", nil, "seed topics at startup as NAME:PARTITIONS (repeatable and/or comma-separated)")
	cmd.Flags().BoolVar(&allowAuto, "allow-auto-topic-creation", false, "allow producers/consumers to auto-create topics")
	cmd.Flags().BoolVar(&acls, "acls", false, "enable ACL enforcement (requires --sasl superusers to get through the deny-by-default)")
	cmd.Flags().StringArrayVar(&saslUsers, "sasl", nil, "add a SASL superuser as MECHANISM:USER:PASS (repeatable; enables SASL). Mechanisms: plain, scram-sha-256, scram-sha-512")
	cmd.Flags().StringVar(&pprofAddr, "pprof", "", "if set, serve pprof on this addr (e.g. :6060 or 127.0.0.1:6060)")

	return cmd
}

func parseLogLevel(s string) (kfake.LogLevel, error) {
	switch strings.ToLower(s) {
	case "none", "":
		return kfake.LogLevelNone, nil
	case "error":
		return kfake.LogLevelError, nil
	case "warn":
		return kfake.LogLevelWarn, nil
	case "info":
		return kfake.LogLevelInfo, nil
	case "debug":
		return kfake.LogLevelDebug, nil
	default:
		return 0, fmt.Errorf("unknown log level %q: want none, error, warn, info, debug", s)
	}
}

func parseBrokerConfigs(list []string) (map[string]string, error) {
	if len(list) == 0 {
		return nil, nil
	}
	m := make(map[string]string, len(list))
	for _, kv := range list {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("broker config %q must be key=value", kv)
		}
		m[k] = v
	}
	return m, nil
}

type seedTopic struct {
	topic      string
	partitions int32
}

func parseSeedTopics(list []string) ([]seedTopic, error) {
	var out []seedTopic
	for _, s := range list {
		name, parts, ok := strings.Cut(s, ":")
		if !ok {
			out = append(out, seedTopic{topic: s, partitions: -1})
			continue
		}
		n, err := strconv.Atoi(parts)
		if err != nil {
			return nil, fmt.Errorf("invalid --seed-topic %q: partition count %q is not an int", s, parts)
		}
		if n < 1 {
			return nil, fmt.Errorf("invalid --seed-topic %q: partition count must be >= 1", s)
		}
		out = append(out, seedTopic{topic: name, partitions: int32(n)})
	}
	return out, nil
}

type saslUser struct {
	mechanism string
	user      string
	pass      string
}

// parseSASLUsers parses "MECHANISM:USER:PASS" entries. USER and PASS
// are passed through os.ExpandEnv so callers can reference environment
// variables without exposing secrets on the command line, e.g.
//   --sasl "plain:$KAFKA_USER:$KAFKA_PASS"
// Quote the argument in the shell so the shell doesn't expand first.
func parseSASLUsers(list []string) ([]saslUser, error) {
	var out []saslUser
	for _, s := range list {
		parts := strings.SplitN(s, ":", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid --sasl %q: want MECHANISM:USER:PASS", s)
		}
		// kfake expects canonical uppercase Kafka mechanism names
		// ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"), but accept
		// any case from the user for convenience.
		var mech string
		switch strings.ToLower(parts[0]) {
		case "plain":
			mech = "PLAIN"
		case "scram-sha-256":
			mech = "SCRAM-SHA-256"
		case "scram-sha-512":
			mech = "SCRAM-SHA-512"
		default:
			return nil, fmt.Errorf("invalid --sasl %q: mechanism %q must be plain, scram-sha-256, or scram-sha-512", s, parts[0])
		}
		user := os.ExpandEnv(parts[1])
		pass := os.ExpandEnv(parts[2])
		if user == "" || pass == "" {
			return nil, fmt.Errorf("invalid --sasl %q: user and password must be non-empty after env expansion", s)
		}
		out = append(out, saslUser{mechanism: mech, user: user, pass: pass})
	}
	return out, nil
}
