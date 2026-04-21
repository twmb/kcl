kcl
===

## Contents

- [Introduction](#introduction)
- [Getting Started / Installation](#getting-started)
- [Stability Status](#stability-status)
- [Configuration](#configuration)
- [Autocompletion](#autocompletion)
- [Group Consuming](#group-consuming)
- [Share Groups](#share-groups)
- [API at a Glance](#api-at-a-glance)
- [Examples](#examples)

## Introduction

kcl is a complete, pure Go command line Kafka client. Think of it as your
one stop shop to do anything you want to do with Kafka -- producing, consuming,
administering, transactions, ACLs, share groups, and so on.

Unlike the small size of [kafkacat][1], this binary is ~15M compiled. It is,
however, still fast, has rich consuming and producing formatting options, and
a complete Kafka administration interface that tracks the upstream protocol
closely.

[1]: https://github.com/edenhill/kafkacat

## Stability Status

The command surface is now relatively stable. Recent releases have made
targeted breaking changes (see the CHANGELOG) to clean up flag/config names
before a 1.x tag. Once a broader set of users confirm they are happy with
the surface, a 1.x will follow.

I've spent significant time integration testing my [franz-go][2] client that
this program uses. It is worth reading the stability status in the franz-go
repo as well if using this client.

[2]: https://github.com/twmb/franz-go/

## Getting Started

If you have a Go installation:

```
go install github.com/twmb/kcl@latest
```

This installs kcl from the latest release. You can optionally suffix with
`@v#.#.#` to install a specific version. When installed this way, kcl
automatically reports itself to brokers as `kcl/<version>` via the Kafka
protocol's client ID (useful for ACL audit logs and broker-side metrics).

Otherwise, download a release from the
[releases](https://github.com/twmb/kcl/releases) page.

## Configuration

kcl is usable out of the box against `localhost:9092`; no config is required
for the common case of probing a local cluster. For real clusters you can
either use flags, environment variables, or a config file. The config file
supports multiple named profiles so that switching between clusters is easy.

Priority (highest wins):

1. `-B/--bootstrap-servers` (seed brokers only)
2. `-X key=value` flags (repeatable; any config key)
3. `KCL_<KEY>` environment variables
4. Active profile in the config file (`--profile/-C` or `current_profile`)
5. Top-level config file keys (flat layout)
6. Built-in defaults

By default, kcl reads its config from your OS user-config directory,
typically `~/.config/kcl/config.toml`. The default path can be overridden
with `--config-path` or `KCL_CONFIG_PATH`.

The configuration supports TLS, SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512,
AWS_MSK_IAM), seed brokers, and client/server timeouts. Timeouts accept Go
duration strings (`500ms`, `5s`, `2m30s`).

For a full reference with examples, run `kcl profile --help`.

### Quick example config

```toml
# ~/.config/kcl/config.toml
current_profile = "prod"

[profiles.prod]
seed_brokers   = ["kafka-prod-1:9092", "kafka-prod-2:9092"]
broker_timeout = "10s"

[profiles.cicd]
seed_brokers   = ["kafka-staging:9092"]
dial_timeout   = "2s"       # fail fast in CI
broker_timeout = "5s"
retry_timeout  = "5s"

[profiles.local]
seed_brokers = ["localhost:9092"]
```

Then:

```
kcl topic list                  # uses "prod"
kcl -C cicd topic list          # switches to "cicd" for one command
kcl -B other-host:9092 topic list   # one-off override of seed brokers
```

## Autocompletion

Thanks to [cobra][3], autocompletion exists for bash, zsh, and powershell.

[3]: https://github.com/spf13/cobra

Bash example to put in `.bashrc`:

```bash
if [ -f /etc/bash_completion ] && ! shopt -oq posix; then
    . /etc/bash_completion
    . <(kcl misc gen-autocomplete -kbash)
fi
```

## Group Consuming

Group consuming is supported with the `-g/--group` flag on `kcl consume`. The
default balancer is `cooperative-sticky` (incremental rebalancing, Kafka 2.4+),
which is incompatible with the older eager balancers (roundrobin, range,
sticky). If your existing group has members using eager balancing, pass
`--balancer` explicitly.

`kcl group describe` shows per-partition committed offsets, lag, and member
assignments. `kcl group seek` resets committed offsets via AlterOffsets;
`kcl group offset-delete` deletes specific partitions' committed offsets.

## Share Groups

Share groups (KIP-932, Kafka 4.0+) are supported via `kcl consume --share-group
NAME`. The `--share-ack-type` flag controls how each fetched record is
acknowledged:

- `accept` (default) -- mark the record as successfully processed.
- `release` -- put the record back into the pool for redelivery (bumps
  delivery count). Useful for peeking at records without consuming them.
- `reject` -- archive the record as unprocessable (bumps delivery count,
  no redelivery). Useful for force-draining or exercising DLQ-style flows.

`kcl share-group` has its own `list`, `describe`, `seek`, `delete`, and
`offset-delete` subcommands.

## API at a glance

The best way to explore kcl is `kcl --help` and then `kcl <cmd> --help`.
The top-level commands are:

```
kcl
 acl            -- list/create/delete ACLs
 client-metrics -- manage client telemetry subscriptions (KIP-714)
 cluster        -- cluster info/quorum/feature flags/leader elections
 config         -- alter/describe topic, broker, group, client-metrics configs
 consume        -- consume records (classic group, share group, or direct)
 dtoken         -- delegation token commands
 group          -- classic / KIP-848 consumer group operations
 logdirs        -- per-partition log directory operations
 misc           -- api-versions, list-offsets, raw-req, error lookups, completion
 produce        -- produce records
 profile        -- manage connection profiles / config
 quota          -- alter/describe/resolve client quotas
 reassign       -- alter/list partition reassignments
 share-group    -- share group operations (KIP-932)
 topic          -- list/create/describe/delete/add-partitions/trim-prefix
 txn            -- describe active transactions / producers
 user           -- SCRAM user credential management
```

Output format for every command is controlled by the global `--format` flag
(`text`, `json`, or `awk`). JSON output is stable (`{_command, _version, ...}`
envelope) and suitable for piping into `jq`. Text output is tab-aligned;
column names are hyphen-delimited (`GROUP-ID`, `LEADER-EPOCH`, etc.) so awk
pipelines are straightforward.

## Examples

### Consuming

Consume topic `foo`, print values:

```
kcl consume foo
```

Advanced formatting -- key, value, and headers:

```
kcl consume foo -f "KEY=%k, VALUE=%v, HEADERS=%{%h{ '%k'='%v' }}\n"
```

Group consuming from topics `foo` and `bar`:

```
kcl consume -g mygroup foo bar
```

Share group consuming (Kafka 4.0+), peeking at records without consuming:

```
kcl consume --share-group sg1 --share-ack-type release foo
```

From a specific timestamp:

```
kcl consume foo -o @2024-01-15
kcl consume foo -o @-1h            # 1 hour ago
kcl consume foo -o @-30m:@now      # 30 minutes ago to now
```

### Producing

Newline-delimited value to topic `foo`:

```
echo fubar | kcl produce foo
```

Values from a file:

```
kcl produce foo < baz
```

Produce key `k` and value `v` from a single line:

```
echo "key: k, value: v" | kcl produce foo -f 'key: %k, value: %v\n'
```

Produce with headers:

```
echo "k v 2 h1 v1 h2 v2" | kcl produce foo -f '%k %v %H %h{%k %v }\n'
```

### Administering

```
kcl topic create foo                              # uses cluster default partitions/replication
kcl topic create foo -p 6 -r 3                    # 6 partitions, 3 replicas
kcl topic describe foo                            # partitions, configs, health
kcl topic describe --topic-id <uuid>              # lookup by UUID (KIP-516)
kcl cluster info                                  # broker list, controller
kcl cluster features describe                     # feature flags (KIP-584)
kcl cluster features update share.version=1 --upgrade-type safe-downgrade
kcl group list                                    # classic + KIP-848 + share groups
kcl group describe mygroup
kcl group seek mygroup --to end --yes
kcl acl list
```

### Error and exit codes

Commands exit non-zero on any per-item failure (e.g. deleting one topic
out of three, where one doesn't exist, exits 1). `--format json` output
on stdout is always valid JSON; all errors go to stderr. This makes kcl
safe to script against.
