# kcl migration notes

Output changes that could break a script parsing kcl, newest first. `text`
output is for people and may change between releases without an entry here;
`--format awk` is the stable scripting contract. Entries cover `--format
json` and any `text` change worth calling out.

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
