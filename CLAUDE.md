# kcl development context

## Audit every change against this list

Most drift in this repo comes from a new command quietly skipping one of
these rather than from anyone deciding against it. Walk the list before
calling a change done, and say which items do not apply and why.

- [ ] Does the command honor `--format text|json|awk`?
- [ ] Do its errors respect the format, and use the right exit code?
- [ ] Does its help follow the `EXAMPLES:` / `SEE ALSO:` shape?
- [ ] Does it change output? MIGRATION.md gets an entry, with before and
      after and how to get the old shape back.
- [ ] Are there sibling commands that should have gotten the same change?
- [ ] Do the tests cover the paths you only exercised by hand?

That last one is the one that bites. Twice in the fake control work a
smoke test found a bug the unit tests missed, because the tests covered
the HTTP layer and the bug was in argument handling above it.

## Output

`--format` is a root persistent flag (`client/client.go:251`), not part of
`Cfg`, so it is unrelated to the TOML file and `-X`. A command holding a
`*client.Client` reads it with `cl.Format()`; a command without one reads
`cmd.Flags().GetString("format")`, since cobra hands down root's persistent
flags.

Tabular output goes through `out.NewFormattedTable`, which renders all
three formats from one set of rows. It models one flat table, so a command
whose output has sections or nesting (`topic describe`, `cluster metadata`)
branches on `cl.Format()` by hand and calls `out.MarshalJSON` instead. Both
are fine; a command with a single table that hand rolls it is not.

Do not confuse the two mechanisms with a coverage gap. Every command emits
valid JSON under `--format json` today, verified by running the read-only
commands against `kcl fake` and parsing their stdout. If you change that,
you have broken something.

`out.MarshalJSON` covers non-tabular JSON, and both it and the table put
`_command` and `_version` at the top level. All JSON output is one line;
see MIGRATION.md. Pipe to jq when you want it wide.

`text` is for people and may change between releases. `awk` is the stable
scripting contract: TSV, no headers, stable column order.

## Commands

Every command but `kcl fake` takes a `*client.Client`. `fake` does not,
deliberately: it does not connect to a cluster, it is one, so it has no
business reading seed brokers, TLS, or SASL. Flags only. That is also why
it must not grow config keys or read `KCL_*` variables of its own, which
would collide with the names `client.go:539` derives from config keys.

`Short` is a sentence ending in a period (82 of 114 do). `Long` opens by
repeating the short description as its own line, then explains, then:

```
EXAMPLES:
  kcl foo bar                      # what it does

SEE ALSO:
  kcl related        one line
```

15 files use `EXAMPLES:` and 7 use `SEE ALSO:`, and nothing uses a bare
heading without the colon any more. Keep it that way.

Renames keep the old name working as a `Hidden`/`Deprecated` cobra command
or flag, so no script breaks. `--help-json` (`main.go:202`) dumps the whole
tree, and `main_test.go` pins that hidden commands stay marked.

## Errors and exit codes

`out.ExitOK` 0, `out.ExitError` 1 for a Kafka-level failure, `out.ExitUsage`
2 for bad flags, arguments, or parse failures. Return `out.Errf(code, ...)`
rather than a bare `fmt.Errorf` when the exit code matters. `out.DieJSON`
emits a structured error when the format is JSON.

## Tests

Table-driven with an anonymous struct slice and `t.Run` per case, matching
`commands/fake/fake_test.go`. Integration tests stand up a real `kfake`
cluster and a real `kgo` client rather than mocking. Send test output to a
file and grep the file; piping `go test` through grep throws away the
stack traces you will want.

## Environment

Go 1.27. That gets us the stdlib `uuid` package (import `"uuid"`, not
`crypto/uuid`), which parses bare hex, dashed, uppercase, `urn:uuid:` and
braced forms, and `encoding/json/v2`. In v2, `omitempty` does not drop a
`false` or a `0`; `omitzero` does, and is what you almost always want.
`RejectUnknownMembers(true)` turns a misspelled field into an error instead
of a silent drop, and v2 matches field names case sensitively.

`kerr` names error 6 `NOT_LEADER_FOR_PARTITION`, the name Kafka used before
2.6. There is no name to code lookup in `kerr` or `kmsg`; build the reverse
map by walking codes, and skip codes that do not answer to themselves,
since an unknown code answers `UNKNOWN_SERVER_ERROR` rather than nil.

## Never

Do not refer to Redpanda or rpk in code, comments, commit messages, or
documentation. See the licensing note at the top of IMPROVEMENTS.md.
