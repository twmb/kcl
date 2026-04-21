// Package fake implements the `kcl fake` subcommand: start an in-process
// fake Kafka cluster backed by github.com/twmb/franz-go/pkg/kfake for
// testing, probing, and experimentation.
package fake

import (
	"fmt"
	"net"
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
		ports        []int
		numBrokers   int
		logLevel     string
		dataDir      string
		syncWrites   bool
		asVersion    string
		brokerCfgs   []string
		seedTopics   []string
		allowAuto    bool
		clusterID    string
		pprofAddr    string
	)

	cmd := &cobra.Command{
		Use:   "fake",
		Short: "Start an in-process fake Kafka cluster for testing.",
		Long: `Start an in-process fake Kafka cluster for testing.

kcl fake runs github.com/twmb/franz-go/pkg/kfake in-process and prints
the listen addresses to stdout. By default it listens on 127.0.0.1:9092,
:9093, :9094 (three brokers). Point another kcl or any Kafka client at
those addresses -- SIGINT or SIGTERM exits cleanly.

This is NOT a production broker. kfake is a protocol-level fake used
for integration tests: it implements the public user-facing Kafka
protocol surface (produce, fetch, groups, transactions, ACLs, share
groups) but intentionally omits broker-to-broker / KRaft-internal
requests and is not performance-tuned.

State is in-memory by default. Pass --data-dir PATH to persist topics,
records, consumer group offsets, and transactional producer state
across restarts. Pass --sync to fsync every write (slower but safest).

EXAMPLES

Default three-broker cluster on 9092/9093/9094:

  kcl fake

Single broker on a specific port:

  kcl fake --ports 9092 --num-brokers 1

Persistent cluster:

  kcl fake -d /tmp/kfake --sync

Pretend to be Kafka 3.9 (restricts advertised API versions):

  kcl fake --as-version 3.9

Seed topics at startup:

  kcl fake --seed-topic foo:10 --seed-topic bar:3

Custom broker config (repeatable):

  kcl fake -c group.consumer.heartbeat.interval.ms=500 \
           -c transactional.id.expiration.ms=60000

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

			if numBrokers <= 0 {
				numBrokers = len(ports)
				if numBrokers == 0 {
					numBrokers = 3
				}
			}

			opts := []kfake.Opt{
				kfake.NumBrokers(numBrokers),
				kfake.WithLogger(kfake.BasicLogger(os.Stderr, level)),
			}
			if len(ports) > 0 {
				opts = append(opts, kfake.Ports(ports...))
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
			if clusterID != "" {
				opts = append(opts, kfake.ClusterID(clusterID))
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
				// Give persistent shutdown a chance to drain; a
				// second signal forces immediate exit.
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

	cmd.Flags().IntSliceVar(&ports, "ports", nil, "ports for brokers (comma-separated; default is kfake-chosen when unset)")
	cmd.Flags().IntVar(&numBrokers, "num-brokers", 0, "number of brokers (defaults to 3, or to len(--ports) if set)")
	cmd.Flags().StringVarP(&logLevel, "log-level", "l", "none", "kfake log level: none, error, warn, info, debug")
	cmd.Flags().StringVarP(&dataDir, "data-dir", "d", "", "persist state under this directory across restarts (default: in-memory only)")
	cmd.Flags().BoolVar(&syncWrites, "sync", false, "fsync every write for immediate durability (slower)")
	cmd.Flags().StringVar(&asVersion, "as-version", "", "pretend to be this Kafka version (e.g. 3.9, 4.0); caps advertised API versions")
	cmd.Flags().StringArrayVarP(&brokerCfgs, "broker-config", "c", nil, "broker config key=value (repeatable; applied at startup)")
	cmd.Flags().StringArrayVar(&seedTopics, "seed-topic", nil, "seed a topic at startup as NAME:PARTITIONS (repeatable)")
	cmd.Flags().BoolVar(&allowAuto, "allow-auto-topic-creation", false, "allow producers/consumers to auto-create topics")
	cmd.Flags().StringVar(&clusterID, "cluster-id", "", "override the cluster ID (default 'kfake')")
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

// Unused by the command flow but kept for future expansion (e.g., a
// --listen flag that binds to non-loopback; kfake currently bakes
// 127.0.0.1 into its ListenFn default, so we note that here).
var _ = net.IPv4zero
