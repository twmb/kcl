# kcl migration notes

Output changes that could break a script parsing kcl, newest first. `text`
output is for people and may change between releases without an entry here;
`--format awk` is the stable scripting contract. Entries cover `--format
json` and any `text` change worth calling out.

## `config describe` moves the read only marker out of the key

Text marks a read only key by suffixing the key with a star, and JSON and awk
carried the star inside the key, so `"key":"broker.id*"` did not match a
script looking for `broker.id`. The key is now the key, and a `READ-ONLY`
column, `read_only` in JSON, says whether it is read only. The column is last,
so an awk script keeps the indexes it had, and `--with-types` still slots
`TYPE` second. `text` is unchanged and keeps the star.

Before:

```
$ kcl config describe 0 -tb --format json
{"_command":"config.describe","_version":1,"configs":[{"key":"broker.id*","source":"STATIC_BROKER_CONFIG","value":"0"},...]}
$ kcl config describe 0 -tb --format awk | head -1
broker.id*	STATIC_BROKER_CONFIG	0
```

After:

```
$ kcl config describe 0 -tb --format json
{"_command":"config.describe","_version":1,"configs":[{"key":"broker.id","read_only":true,"source":"STATIC_BROKER_CONFIG","value":"0"},...]}
$ kcl config describe 0 -tb --format awk | head -1
broker.id	0	STATIC_BROKER_CONFIG	true
```

A script that stripped the star reads the key straight now; one that tested
for it reads `.read_only`.

## `topic describe` names a config source the way `config describe` does

The two commands disagreed about the source of the same key on the same
topic: `cleanup.policy` was `DEFAULT` under `topic describe` and
`DEFAULT_CONFIG` under `config describe`. `topic describe` had its own list of
names. Both now print what `kmsg.ConfigSource` calls the source, in text, json
and awk.

| was | is |
|---|---|
| `DYNAMIC_TOPIC` | `DYNAMIC_TOPIC_CONFIG` |
| `DYNAMIC_BROKER` | `DYNAMIC_BROKER_CONFIG` |
| `DYNAMIC_DEFAULT_BROKER` | `DYNAMIC_DEFAULT_BROKER_CONFIG` |
| `STATIC_BROKER` | `STATIC_BROKER_CONFIG` |
| `DEFAULT` | `DEFAULT_CONFIG` |
| `DYNAMIC_BROKER_LOGGER` | `DYNAMIC_BROKER_LOGGER_CONFIG` |
| `SOURCE(7)`, `SOURCE(8)` | `CLIENT_METRICS_CONFIG`, `GROUP_CONFIG` |

Before:

```
$ kcl topic describe demo-avro --format json
{...,"configs":[{"key":"cleanup.policy","value":"delete","source":"DEFAULT","sensitive":false},...]}
```

After:

```
$ kcl topic describe demo-avro --format json
{...,"configs":[{"key":"cleanup.policy","value":"delete","source":"DEFAULT_CONFIG","sensitive":false},...]}
```

A script matching `DEFAULT` exactly matches `DEFAULT_CONFIG` now. There is no
way back to the old names.

## `--format awk` is one shape per command

Three commands answered `--format awk` with something other than TSV rows.

`topic describe` ended every partition row with a tab, because the ERROR
column was empty and last. It now writes a dash there, and a dash in the
`--stable` column for a partition with no stable offset, which is what the
text column already showed. The column count is unchanged: 8, or 9 with
`--stable`.

Before:

```
$ kcl topic describe demo-avro --format awk | cat -A
demo-avro^I0^I0^I0^I[0]^I[0]^I[]^I$
```

After:

```
$ kcl topic describe demo-avro --format awk | cat -A
demo-avro^I0^I0^I0^I[0]^I[0]^I[]^I-$
```

`misc list-offsets` ended every awk row in a tab for the same reason, and
writes the same dash in its empty `ERROR` column. `text` and `json` keep the
empty string.

`registry compatibility test` printed `compatible: true`, a label and a
value. awk is the word alone, so that `[ "$(kcl ... --format awk)" = true ]`
reads. `text` still prints the labeled line.

Before:

```
$ kcl registry compatibility test demo-avro-value -s new.avsc --format awk
compatible: true
```

After:

```
$ kcl registry compatibility test demo-avro-value -s new.avsc --format awk
true
```

`registry schema get` printed the schema text alone, the same bytes `text`
prints. awk is one row: id, version, type, schema text. A schema spanning
lines, a .proto for instance, has its newlines, tabs, and backslashes written
as `\n`, `\t` and `\\`, so the row stays one line. Version is a dash when you
fetched by `--id`. Piping the schema itself is what `text` is for.

Before:

```
$ kcl registry schema get demo-avro-value --format awk
{"type":"record","name":"Demo","fields":[...]}
```

After:

```
$ kcl registry schema get demo-avro-value --format awk
1	1	AVRO	{"type":"record","name":"Demo","fields":[...]}
$ kcl registry schema get demo-avro-value            # unchanged
{"type":"record","name":"Demo","fields":[...]}
```

## Numbers in `--format json` are JSON numbers

Some columns reached JSON as the string of a number, so one document carried
`"start":"0"` beside `"stable":5`. The fields that changed type:

| command | fields |
|---|---|
| `misc list-offsets` | `start`, `end` |
| `logdirs describe` | `size`, `total`, `usable` |
| `group describe` | `current_offset`, `lag` in the `lag` rows |
| `share-group describe` | `lag` in the `offsets` rows |

Before:

```
$ kcl misc list-offsets demo-avro --format json
{"_command":"misc.list-offsets","_version":1,"offsets":[{"broker":0,"end":"5","error":"","partition":0,"stable":5,"start":"0","topic":"demo-avro"}]}
$ kcl logdirs describe --format json
{"_command":"logdirs.describe",...,"size":"395",...,"total":"1738","usable":"34359738368"}
```

After:

```
$ kcl misc list-offsets demo-avro --format json
{"_command":"misc.list-offsets","_version":1,"offsets":[{"broker":0,"end":5,"error":"","partition":0,"stable":5,"start":0,"topic":"demo-avro"}]}
$ kcl logdirs describe --format json
{"_command":"logdirs.describe",...,"size":395,...,"total":1738,"usable":34359738368}
```

A field the cluster did not report is `null` rather than the string `"-"`:
`logdirs describe` against a broker below Kafka 3.3, which sends -1 for the
volume size, and the `current_offset` and `lag` of a partition a group has
not committed. `text` and `awk` still print the dash.

Two cases stay strings, both because the column holds more than a number.
`-H` formats sizes as `1.7KB`, which is the display you asked for.
`misc list-offsets --with-epochs` writes `START` and `END` as `offset/epoch`.

A script comparing `.size` to a string now compares it to a number; `jq`
reads `.size | tostring` for the old shape.

## `cluster metadata` sorts topics in every format

`--format json` and `--format awk` printed topics in whatever order the
broker answered. Kafka walks a map, so two runs of the same command gave two
orders. `text` already sorted.

Before:

```
$ kcl cluster metadata --format awk | head -2
demo-proto	43448cad52d6affcd28bc8ca9b8873ce	1	1
demo-json	ac356d10b3b3952034fd3403cc0c449c	1	1
$ kcl cluster metadata --format awk | head -2
demo-avro	98e20e0557ac8939793d53ad4409ccbc	1	1
demo-proto	43448cad52d6affcd28bc8ca9b8873ce	1	1
```

After:

```
$ kcl cluster metadata --format awk | head -2
demo-avro	98e20e0557ac8939793d53ad4409ccbc	1	1
demo-json	ac356d10b3b3952034fd3403cc0c449c	1	1
```

Topics sort by name, each topic's partitions by partition number, and
brokers by node ID, in all three formats. The old order was not an order, so
there is nothing to get back.

## `txn describe` types its JSON fields

`producer_id`, `producer_epoch`, `timeout_ms` and `start_timestamp` were
strings, and `error` was a Go struct with exported field names. The command
also exited 0 when a transactional ID was not found.

Before:

```
$ kcl txn describe nosuchtxn --format json
{"_command":"txn.describe","_version":1,"transactions":[{"error":{"Message":"TRANSACTIONAL_ID_NOT_FOUND","Code":105,"Retriable":false,"Description":"The transactionalId could not be found."},"producer_epoch":"","producer_id":"","start_timestamp":"","state":"","timeout_ms":"","topics":"","transactional_id":"nosuchtxn"}]}
[exit 0]
```

After:

```
$ kcl txn describe nosuchtxn --format json
{"_command":"txn.describe","_version":1,"transactions":[{"error":"TRANSACTIONAL_ID_NOT_FOUND: The transactionalId could not be found.","producer_epoch":null,"producer_id":null,"start_timestamp":null,"state":"","timeout_ms":null,"topics":"","transactional_id":"nosuchtxn"}]}
[exit 1]
```

The four fields are JSON numbers when the transaction exists and `null` when
it does not, `start_timestamp` in unix milliseconds rather than a formatted
date, and `error` is the string every other command puts there. The old
shapes are gone: a script reading `.error.Message` reads `.error` now, and
one comparing `.producer_id` to `""` compares it to `null`.

## `--format json` prints one line

Applies to every command that emits JSON, including `--format json` errors.

Before:

```
$ kcl topic list --format json
{
  "_command": "metadata.topics",
  "_version": 1,
  "topics": [
    {
      "id": "e8bb8b5c6d6ec65756872c88c6e76a42",
      "name": "foo",
      "partitions": 2,
      "replicas": 1
    }
  ]
}
```

After:

```
$ kcl topic list --format json
{"_command":"metadata.topics","_version":1,"topics":[{"id":"e8bb8b5c6d6ec65756872c88c6e76a42","name":"foo","partitions":2,"replicas":1}]}
```

Nothing about the structure changed, so any JSON parser reads both. What
breaks is a script treating the output as lines, such as `kcl ... --format
json | grep '"name"'`, which now matches the whole document instead of one
field.

To get the old shape, pipe through jq:

```
$ kcl topic list --format json | jq
```

The reason for the change: one line pipes into jq, greps as a record, and
captures into a shell variable. A pretty printed value does none of those,
and jq is right there when you want it wide.
