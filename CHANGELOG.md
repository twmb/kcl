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
