# kcl migration notes

Output changes that could break a script parsing kcl, newest first. `text`
output is for people and may change between releases without an entry here;
`--format awk` is the stable scripting contract. Entries cover `--format
json` and any `text` change worth calling out.

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
