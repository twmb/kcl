TBD
===

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
