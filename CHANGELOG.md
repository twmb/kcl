v0.17.0
===

This is a large release with many breaking changes, intentional, to clean
up the flag and config surface before a 1.x tag. If you use kcl via shell
scripts or automation, read this list carefully before upgrading.

### BREAKING

Configuration
* `timeout_ms` (integer milliseconds) is replaced by `broker_timeout`
  (Go duration string: "5s", "500ms", "1m"). Using the old key produces
  a loud migration error.
* Unknown config keys in the TOML file now produce a warning on stderr
  (via the BurntSushi `MetaData.Undecoded` path). This surfaces typos
  and stale names that were previously silently dropped.
* Two new duration keys: `dial_timeout` (bounds a single TCP dial) and
  `retry_timeout` (bounds total client request + retries). Both default
  to kgo's defaults (10s / 30s) when unset.

Flags
* Confirmation prompts are standardized on `--yes`/`-y`.
  - `group seek`, `share-group seek`: `--execute` renamed to `--yes`/`-y`.
  - `topic trim-prefix`, `config alter`, `acl delete`: `--no-confirm`
    renamed to `--yes`/`-y`.
* `--if-exists` / `--if-not-exists` removed from create/delete commands.
  Reliable non-zero exit codes plus structured JSON output replace them.
* `-b` shorthand removed from `consume --balancer` (long form still
  works). `-b` is now free for per-command broker-ID flags where used,
  and `-B/--bootstrap-servers` is available globally to override the
  seed brokers without editing a profile.

Commands
* Removed `kcl txn unstick-lso` (obsolete pre-KIP-890 workaround). If
  you need to force-close a zombie transaction, use `WriteTxnMarkers`
  via the kadm library.
* Removed `kcl profile config-help` subcommand. Its content is now the
  Long description of `kcl profile --help`, so running that prints the
  full configuration reference in one place.
* `kcl topic create` `-p/--num-partitions` and `-r/--replication-factor`
  default to `-1`, which the wire protocol interprets as "use cluster
  default" (`num.partitions`, `default.replication.factor`). This
  matches `kafka-topics.sh --create` behavior. If you relied on the old
  `-p 20 -r 1` defaults, pass them explicitly.
* Column headers no longer contain spaces. `GROUP ID` is now `GROUP-ID`,
  `LAST SEQUENCE` is now `LAST-SEQUENCE`, etc. awk pipelines that
  indexed columns are unaffected (awk output has no headers); only
  text-mode consumers parsing headers need to update.

### NEW

* Global `-B/--bootstrap-servers`: seed brokers override, comma-separated.
* Global ClientID tagging: every request advertises `kcl/<version>` so
  brokers can identify kcl in ACL audit logs and metrics. Version is
  resolved from ldflags, falling back to `runtime/debug.BuildInfo` so
  `go install github.com/twmb/kcl@vX.Y.Z` automatically tags the binary.
* `kcl topic describe --topic-id UUID`: look up by topic ID (KIP-516).
  Accepts 32 hex chars or the dashed 8-4-4-4-12 form.
* `kcl consume --share-ack-type [accept|release|reject]`: control how
  share-group records are acked. `release` peeks (returns records to
  the pool with incremented delivery count); `reject` archives
  permanently (exercises KIP-932 delivery count limits / DLQ flows).
* `kcl group seek --topics foo:0,1,2` and the same on
  `kcl share-group seek`: scope the seek to specific partitions, not
  just topics. Matches kafka-share-groups.sh --topic format.
* `kcl misc raw-req -v N`: pin the wire version for the request.
  Empty stdin is now accepted for requests with no body.
* `kcl cluster features update --upgrade-type [upgrade|safe-downgrade|
  unsafe-downgrade]`: proper support for downgrading finalized feature
  levels.
* `kcl client-metrics list` now uses `ListConfigResources` (KIP-1142)
  and falls back to `DescribeConfigs` for pre-4.1 brokers.

### FIXES / UX

* Commands return non-zero exit on any per-item failure. Previously,
  deleting N topics where some didn't exist returned 0.
* `--format json` output is guaranteed valid JSON on stdout; errors
  and diagnostics are written to stderr.
* Root-level `SilenceUsage: true`: runtime errors no longer dump cobra
  usage boilerplate. Argument/flag parse errors still show usage.
* `group describe` / `share-group describe` skip empty state fields on
  group-level errors (e.g. `GROUP_ID_NOT_FOUND`) instead of showing
  empty `STATE:` / `EPOCH: 0` around the error.
* On dev/dirty builds, `kcl --version` prints `dev+abc1234[-dirty]`
  instead of the full Go pseudo-version.
* `flagutil.ParseTopicPartitions`: duplicate topic entries now merge
  (e.g. `--topics foo:0 --topics foo:3` yields `foo: [0, 3]` instead
  of losing the first entry); a bare-topic entry wins over any
  per-partition scope for the same topic.

### UPSTREAM

* Bumps `franz-go` to v1.21.0, `kadm` to v1.18.0, `kmsg` to v1.13.1,
  and `kfake` to the latest pseudo-version (which carries the new
  handlers: WriteTxnMarkers, UpdateFeatures, ListConfigResources,
  DescribeTopicPartitions).
* Bumps aws-sdk-go-v2 family, cobra, pflag, protoreflect, crypto,
  sync, compression libraries.
* Drops the local `go.work` / `go.work.sum` and the replace directives
  that pointed at a sibling `../franz-go` checkout, so a bare
  `go install github.com/twmb/kcl@latest` resolves cleanly.

v0.16.0
===

* Bumps all deps
* Builds with go1.24.2
* Enables fish command completion

v0.15.0
===

* Bumps all deps
* Builds with go1.22.0
* Fixes kcl misc raw-req to obey a version in the input json, if a version is specified

v0.14.0
===

* Bumps all deps
* Fixes kadm admin user-scram alter typo
* Supports --as-version for all Kafka versions, and kcl misc api-versions --version for all Kafka versions -- and makes it so no code changes are needed in kcl when franz-go adds support for newer Kafka versions
* Builds with go1.21.6

v0.13.0
===

This release bumps all deps, adds support for detecting Kafka 3.5 and detecting
KRaft based Kafka, fixes some internal client bugs (mostly unrelated to what is
used in kcl), and adds the `tls_insecure` config option (thank you
[@dbudworth](https://github.com/dbudworth)!).

This also fixes a bug from v0.12.0 where `kcl admin topic create` broke in a way
that did not support `--config-path`.

v0.12.0
===

This release bumped all deps, built with 1.20, and set `CGO_ENABLED=0`.

v0.11.0
===

This release fixes one minor issues, one feature improvement, and now can
detect Kafka 3.4.

Issue: previously, an empty tls struct would be omitted when marshaled, now it
is not (meaning tls with no custom certs is preserved). Feature: the myconfig
command now can autocomplete available configuration files (thanks
[@robsonpeixoto](https://github.com/robsonpeixoto), as well for the bug fix!).
Kafka 3.4: all deps have been bumped, and the latest franz-go can detect Kafka
3.4 from `kcl misc probe-version`.

v0.10.0
===

This release adds `--proto-file` and `--proto-message` to `kcl consume`,
allowing consume to deserialize protobuf encoded values before printing the
value (thanks [@moming00](https://github.com/moming00))!. This release also
recognizes 3.3 in `kcl misc probe-version`.

v0.9.0
===

This release contains some nice improvements to `kcl produce` and `kcl consume`
from [@Zach-Johnson](https://github.com/Zach-Johnson):

* `kcl produce -Z` now produces tombstones
* `kcl produce -p` can now produce to a specific partition
* `kcl consume`'s offset flag is now more intelligent and has consume-until semantics

As well, kcl now detects (through `kcl misc probe-versions`) Kafka 3.0, 3.1,
and 3.2. Lastly, modules have been updated, most significantly bumping franz-go
from v1.2.3 to v1.6.0, which contains many bug fixes and improvements.


v0.8.0
===

This release contains a bugfix for franz-go and allows directing raw requests
to specific brokers.

v0.7.0
===

This release bumps franz-go to v1.0.0, and drops the -t flag from partas.

v0.6.0
===

This release introduces a new command, `kcl misc offset-for-leader-epoch`,
which can be useful in some debugging scenarios, as well as allows leaving off
any groups to `kcl group describe`, and any topics to `kcl misc list-offsets`.

This also fixes the previously broken `kcl transact`, and adds a mirror mode.

As always, this pulls in the latest franz-go, which includes important fixes.

- [`4786fc2`](https://github.com/twmb/kcl/commit/4786fc2) kcl transact: add mirror mode
- [`1eb1b67`](https://github.com/twmb/kcl/commit/1eb1b67) group describe: rewrite, allow no groups to describe all groups
- [`0902b98`](https://github.com/twmb/kcl/commit/0902b98) misc: allow list-offsets and offset-for-leader-epoch to dump all topics
- [`3aa3602`](https://github.com/twmb/kcl/commit/3aa3602) misc: add offset-for-leader-epoch
- [`edc4ebe`](https://github.com/twmb/kcl/commit/edc4ebe) add `topic list` as an alias for `metadata -t` (thanks @SteadBytes!)

v0.5.0
===

This small release mirrors the franz-go ACL resource pattern type bugfix from
v0.8.1 to kcl's string parsing. This also bumps all deps, which includes a few
minor franz-go bugfixes from v0.8.3 to v0.8.7.


v0.4.0
===

- [`1618283`](https://github.com/twmb/kcl/commit/1618283) duplicate topic / group into top level
- [`784adc1`](https://github.com/twmb/kcl/commit/784adc1) update deps & fix api breakage from franz-go 0.8.0
- [`78d50a5`](https://github.com/twmb/kcl/commit/78d50a5) change how `AWS_MSK_IAM` is supported

This is a small release that is notable for (a) raising topic & group into top
level commands, and (b) updating the franz-go dep to what is hopefully its
final API before stabilization.

The `AWS_MSK_IAM` change now makes it such that kcl loads credentials
directly from your `~/.aws/credentials` file, so that you do not need to
duplicate those credentials into a kcl config.

v0.3.0
===

The most notable changes are as follows:

- [`a20ea56`](https://github.com/twmb/franz-go/commit/a20ea56) **feature** admin txn: add unstick-lso
- [`9c461af`](https://github.com/twmb/franz-go/commit/9c461af) **feature** add support for `AWS_MSK_IAM` in config
- [`da57aa0`](https://github.com/twmb/franz-go/commit/da57aa0) **bugfix** fixup no-config{,-path} to no-config-file
- [`be842c4`](https://github.com/twmb/franz-go/commit/be842c4) **feature** consumer: allow printing producer ID, producer epoch, high watermark, low watermark
- [`930a290`](https://github.com/twmb/franz-go/commit/930a290) **feature** myconfig: add interactive config creation under "create"
- [`ea1857a`](https://github.com/twmb/franz-go/commit/ea1857a) **feature** consuming: support reading uncommitted

The bulk of goodness in this release comes from the updated franz-go dep, going
from v0.6.6 to v0.7.3. See the
[CHANGELOG](https://github.com/twmb/franz-go/blob/master/CHANGELOG.md) in that
repo for more details.

This release includes support for topic IDs from Kafka 2.8.0, but currently,
Kafka does not actually support topic IDs in MetadataRequest (see
[KAFKA-12701](https://issues.apache.org/jira/browse/KAFKA-12701) for more
details).

One major feature of this release is a new command, `kcl admin txn unhang-lso`,
that supports for un-sticking a stuck `LastStableOffset`. This may occur from
buggy clients or in some odd edge case scenarios.

Some minor features are left out of the list above, as well as some other minor
changes.

v0.2.2
===

- [`2a2d65e35`](https://github.com/twmb/kcl/commit/2a2d65e352ea58c8472564e1666a9460d88715d2): (breaking) sasl: switch from plaintext to plain
- other various commits

This release contains many bugfixes and improvements from the franz-go library,
and uses `RequestSharded` where relevant so as to print the broker ID in the
output.

Most commits revolve around changes to keep up with changes in the franz-go
library, so commits here are not much annotated. There is no significant change
related to the kcl api itself.

v0.2.1
======

- [`d95f48cd0`](https://github.com/twmb/kcl/commit/d95f48cd06fa0f595f20f4a99b2549022cefcb40): reorganizes admin commands again

This is a quick release right after v0.2.1 for a more long-term stable organization of commands.


v0.2.0
======

- [`64aea537c`](https://github.com/twmb/kcl/commit/64aea537c27e10925f92b76e8b2f3bb391ca458a): adds alter & describe user-scram (will not work until 2.7.0 is released)
- [`7843319fa`](https://github.com/twmb/kcl/commit/7843319fa9c52c60b15df3924bc93b969a267169): changes global flag --no-config to --no-config-path
- [`b004217a0`](https://github.com/twmb/kcl/commit/b004217a0087ca82e4c4fa38df19913b3c4edb0e): move all admin commands under 'admin'
- [`b0bd7baf5`](https://github.com/twmb/kcl/commit/b0bd7baf52d51c07f6bbf287e3f46a18fc163035): enhanced describe-log-dirs
- [`05451336d`](https://github.com/twmb/kcl/commit/05451336d982b4e537cf8d70c340d1fdebd2f799): add misc errtext command
- [`f72facd71`](https://github.com/twmb/kcl/commit/f72facd715de0e9919a3234b7ca9f074a9791e59): fix output on unrecognized prefix

Minor other changes (formatting, documentation, kafka-go bumps).

This is a "major" bump while on the 0 version due to all admin related commands moving under admin.

v0.1.1
======

- [`f05d384b6`](https://github.com/twmb/kcl/commit/f05d384b60f26b773175d725ad854fa5e5cdfc5b): add `admin {describe,alter}-client-quotas`
- [`48dc25f04`](https://github.com/twmb/kcl/commit/48dc25f0409874f47ffa60a5a442ffec464a80df): adds short flag for `--format` in `kcl group list`
- [`5392b0f05`](https://github.com/twmb/kcl/commit/5392b0f0510f1a27be1e3bd6398f68cdb168c94b): adds `--with-types` and `--with-docs` flags to `kcl configs describe`
- [`5b516e8b5`](https://github.com/twmb/kcl/commit/5b516e8b516d113908a796f3c0a1063024962b55): adds 2.6.0 to `kcl misc probe-version`
- [`9cc5fc0a1`](https://github.com/twmb/kcl/commit/9cc5fc0a152455a12fd128565a261967d55bd5f0): tls configuration: allow min version, cipher suites, curves
- [`e19b21485`](https://github.com/twmb/kcl/commit/e19b2148554db1f158a9add25740b40cb1856e16): fix probe-versions for old kafka's
- [`5b877591d`](https://github.com/twmb/kcl/commit/5b877591df12a16f7506739c94476061616f68ab): add `KCL_CONFIG_{DIR,FILE,PATH}`
- [`4c5af690b`](https://github.com/twmb/kcl/commit/4c5af690ba8ebf164eec10e3319000eeea4d4e71): add ascii number parsing for reading messages

Minor other changes (formatting, documentation, kafka-go bumps).
