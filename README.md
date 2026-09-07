kcl
===

## Contents

- [Introduction](#introduction)
- [Getting Started / Installation](#getting-started)
- [Configuration](#configuration)
- [Autocompletion](#autocompletion)
- [Group Consuming](#group-consuming)
- [Share Groups](#share-groups)
- [Schema Registry](#schema-registry)
- [Local Fake Cluster](#local-fake-cluster)
- [API at a Glance](#api-at-a-glance)
- [Examples](#examples)

## Introduction

kcl is a complete, pure Go command line Kafka client. Think of it as your
one stop shop to do anything you want to do with Kafka -- producing, consuming,
administering, transactions, ACLs, share groups, and so on.

Unlike the small size of [kcat][1] (formerly kafkacat), this binary is
~15M compiled. It is, however, still fast, has rich consuming and
producing formatting options, and a complete Kafka administration
interface that tracks the upstream protocol closely.

[1]: https://github.com/confluentinc/kcat

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

`kcl profile create` writes a profile from the same flags a one-off command
takes:

```
kcl profile create prod -B kafka-prod-1:9092,kafka-prod-2:9092
kcl profile create cicd -B kafka-staging:9092 -X dial_timeout=2s -X sasl.method=plain -X sasl.user=ci -X sasl.pass=secret
```

If nothing is current yet, the new profile is; otherwise `kcl profile use NAME`.

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

## Schema Registry

The Schema Registry is a separate HTTP service from the Kafka brokers. Point
kcl at it with `-R/--registry` (comma-separated URLs), `-X registry.urls=...`,
or a `schema_registry` config section; it defaults to `http://localhost:8081`
(mirroring the `localhost:9092` broker default). Auth is optional: basic auth
(`registry.user` / `registry.pass`), a bearer token (`registry.bearer_token`),
and TLS for `https` URLs (`registry.tls.*`, mirroring the Kafka `tls.*` keys).

`kcl registry` administers the registry as a thin layer over franz-go's typed
`pkg/sr` API:

```
kcl registry subjects                          # list subjects
kcl registry versions mytopic-value            # list a subject's versions
kcl registry schema list                       # all schemas across all subjects
kcl registry schema create mytopic-value -s schema.avsc   # register (avro by default)
kcl registry schema get -S mytopic-value       # latest schema for a subject
kcl registry schema get --id 5                 # schema by global id
kcl registry references mytopic-value          # schemas that reference this one
kcl registry delete mytopic-value -v 3         # soft-delete a version (--permanent to hard)
kcl registry compat set BACKWARD mytopic-value # set per-subject compatibility
kcl registry compat get                        # global compatibility
kcl registry compat test mytopic-value -s new.avsc --verbose   # check a candidate schema
kcl registry mode get                          # global mode
kcl registry context list                      # list contexts (namespaces)
kcl registry --context myctx subjects          # scope to a context
```

(`schema create` is also available as `schema register`.)

`kcl produce` and `kcl consume` can transcode between JSON and the Schema
Registry binary wire format (the magic byte + 4-byte schema id, plus the
Protobuf message-index). **Avro**, **JSON Schema**, and **Protobuf** are
supported. Encoding is the inverse of decoding: produce reads JSON and writes
schema-encoded bytes; consume reads schema-encoded bytes and writes JSON, which
then flows through the normal `%v`/`%k` format verbs.

On produce, `--schema` (value) and `--key-schema` (key) take a small spec that
resolves an *existing* schema (producing never registers — use `kcl registry
schema create` for that):

```
topic[@VERSION]          schema for <topic>-value / <topic>-key (latest)
NAME[@VERSION]           a subject (bare), e.g. orders-value, orders-value@3
subject:NAME[@VERSION]   an explicit subject (escape hatch for odd names)
id:N                     a registered schema id
```

`VERSION` is a number or `latest` (default); any form may add a trailing
`#MESSAGE` to pick the protobuf message in a multi-message schema.

```
# Encode values with the latest registered <topic>-value schema:
echo '{"id":"a","n":1}' | kcl produce orders --schema topic

# By explicit subject/version, or by id; encode the key too:
kcl produce orders --schema orders-value@3
kcl produce orders -f '%k %v\n' --key-schema id:7 --schema id:8

# Protobuf, selecting the message:
kcl produce orders --schema topic#com.acme.Order
```

On consume, opt in with `--decode` (both key and value), or `--decode=value` /
`--decode=key` for one. The schema id is read from each record and the schema is
fetched and cached automatically; records not in the wire format are printed
unchanged:

```
kcl consume orders --decode
kcl consume orders --decode=value -f '%k -> %v\n'
```

The subject defaults to the TopicNameStrategy (`<topic>-value` / `<topic>-key`)
when not given explicitly. For an end-to-end playground with no external
dependencies, `kcl fake --seed-demo` stands up a registry and seeds
schema-encoded topics (see below).

To test against a real registry instead of the fake, run one in Docker, e.g.
Redpanda (registry on 8081):

```
docker run -d --name redpanda -p 9092:9092 -p 8081:8081 \
  redpandadata/redpanda redpanda start --schema-registry-addr 0.0.0.0:8081 \
  --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr localhost:9092
kcl -R http://localhost:8081 registry subjects
```

## Local Fake Cluster

`kcl fake` runs a [kfake][4] cluster in-process and prints the listen
addresses. Point any Kafka client (including another kcl invocation) at
those addresses; SIGINT or SIGTERM exits cleanly. State is in-memory by
default; pass `--data-dir PATH` to persist, and `--sync` for fsync-on-write
durability.

[4]: https://github.com/twmb/franz-go/tree/master/pkg/kfake

This is NOT a production broker. kfake implements the user-facing Kafka
protocol surface (produce, fetch, groups, transactions, ACLs, share
groups) but intentionally omits broker-to-broker / KRaft-internal
requests and is not performance-tuned. It's great for probing, learning,
integration tests, CI pipelines, and demos without Docker.

```
kcl fake                                 # 3 brokers on kfake-picked ports
kcl fake --ports 9092,9093,9094          # 3 brokers on specific ports
kcl fake --ports 9092                    # single-broker cluster
kcl fake -d /tmp/kfake --sync            # persistent, durable
kcl fake --seed-topic foo:10,bar:3       # pre-create topics
kcl fake --as-version 3.9                # cap advertised API versions
kcl fake --acls --sasl 'plain:$USER:$PW' # SASL superuser from env vars
kcl fake -c group.consumer.heartbeat.interval.ms=500  # broker config
kcl fake -l debug                        # verbose kfake logs
kcl fake --registry=false                # disable the bundled schema registry
kcl fake --seed-demo                     # seed schema-encoded + plain demo topics
```

`kcl fake` also serves an in-memory Schema Registry ([srfake][5]) on port 8081
by default, so the same process backs schema-aware produce/consume; disable it
with `--registry=false` or move it with `--registry-port`. If the port is busy
(e.g. a real registry is already running) kcl warns and continues without it,
unless `--registry`/`--registry-port` was passed explicitly.

`--seed-demo` creates `demo-avro`, `demo-proto`, and `demo-json` (each with a
registered schema of that type) plus `demo-plain` (no schema), all sharing the
same `{id, count}` shape, and produces a few records to each -- a one-command,
Docker-free playground for the whole schema-registry flow:

```
kcl fake --seed-demo
kcl consume demo-avro -o start --decode   # decodes back to JSON
kcl consume demo-plain -o start           # plain JSON, no schema
```

[5]: https://github.com/twmb/franz-go/tree/master/pkg/sr/srfake

The `--sasl` flag accepts `MECHANISM:USER:PASS` (repeatable). Supported
mechanisms: `plain`, `scram-sha-256`, `scram-sha-512`. User and password
go through `os.ExpandEnv`, so quoting the argument keeps the shell from
expanding first and lets the broker name env vars pick up the secrets.

## API at a glance

The best way to explore kcl is `kcl --help` and then `kcl <cmd> --help`.
The top-level commands are:

```
kcl
 acl            -- list/create/delete ACLs
 client-metrics -- manage client telemetry subscriptions (KIP-714)
 cluster        -- metadata, quorum, feature flags, leader elections, KRaft voters
 config         -- alter/describe topic, broker, group, client-metrics configs
 consume        -- consume records (classic group, share group, or direct)
 dtoken         -- delegation token commands
 fake           -- start a local in-process kfake cluster for testing
 group          -- classic / KIP-848 consumer group operations
 logdirs        -- per-partition log directory operations
 misc           -- api-versions, list-offsets, raw-req, error lookups, completion
 produce        -- produce records
 profile        -- manage connection profiles / config
 quota          -- alter/describe/resolve client quotas
 reassign       -- alter/list partition reassignments
 registry       -- schema registry: schemas, subjects, compatibility, mode
 share-group    -- share group operations (KIP-932)
 topic          -- list/create/describe/delete/add-partitions/trim-prefix
 txn            -- describe active transactions / producers
 user           -- SCRAM user credential management
```

Output format for every command is controlled by the global `--format` flag
(`text`, `json`, or `awk`). JSON output is stable (`{_command, _version, ...}`
envelope) and suitable for piping into `jq`. Text output is tab-aligned;
column names are hyphen-delimited (`GROUP-ID`, `LEADER-EPOCH`, etc.) so awk
pipelines are straightforward. Note that `consume` and `produce`
deliberately repurpose `--format` as the per-record format string
(records don't fit the table envelope).

For tooling and agents that want to introspect kcl's entire command tree
programmatically (names, flags, examples, help text), use the global
`--help-json` flag at the root:

```
kcl --help-json | jq '.commands[] | .name'
```

Interactive confirmation prompts on destructive commands (`group seek`,
`share-group seek`, `topic trim-prefix`, `config alter`, `acl delete`)
are skipped with `--yes/-y`.

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
kcl cluster metadata                              # broker list, controller
kcl cluster describe-cluster                      # admin view with fenced brokers
kcl cluster features describe                     # feature flags (KIP-584)
kcl cluster features update share.version=1 --upgrade-type safe-downgrade
kcl group list                                    # classic + KIP-848 + share groups
kcl group describe mygroup
kcl group seek mygroup --to end --yes
kcl acl list
```

### Schema Registry

```
# One-command playground (brokers + registry + seeded schema topics):
kcl fake --seed-demo
kcl consume demo-avro -o start --decode

# Register a schema and round-trip JSON <-> Avro binary:
kcl registry schema register foo-value -s user.avsc
echo '{"id":"a","n":1}' | kcl produce foo --schema topic
kcl consume foo -o start --decode

# Inspect the registry:
kcl registry subjects
kcl registry schema get -S foo-value
kcl registry compat get foo-value
```

### Probing against a local fake cluster

Start a fake in one shell, use it from another:

```
# shell 1
kcl fake --seed-topic foo:3

# shell 2 (fake prints 127.0.0.1:<port> -- pick any)
kcl -B 127.0.0.1:<port> topic list
seq 1 5 | kcl -B 127.0.0.1:<port> produce foo
kcl -B 127.0.0.1:<port> consume foo -n 5 -o start
```

Or set a persistent profile for the fake so `-B` isn't needed on each
invocation:

```
kcl profile create fake -B 127.0.0.1:<port>
kcl -C fake topic list
```

### Error and exit codes

Commands exit non-zero on any per-item failure (e.g. deleting one topic
out of three, where one doesn't exist, exits 1). `--format json` output
on stdout is always valid JSON; all errors go to stderr. This makes kcl
safe to script against.
