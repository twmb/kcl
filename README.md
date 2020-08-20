kcl
===

## Contents

- [Introduction](#introduction)
- [Getting Started / Installation](#getting-started)
- [Stability Status](#stability-status)
- [Configuration](#configuration)
- [Autocompletion](#autocompletion)
- [Transactions](#transactions)
- [Group Consuming](#group-consuming)
- [API at a Glance](#api-at-a-glance)

## Introduction

kcl is a complete, pure Go command line Kafka client. Think of it as your
one stop shop to do anything you want to do with Kafka. Producing, consuming,
transacting, administrating, and so on.

Unlike the small size of [kafkacat][1], this binary is ~12M compiled.
It is, however, still fast, has rich consuming and producing formatting
options, and a complete Kafka administration interface.

[1]: https://github.com/edenhill/kafkacat

## Stability Status

I consider the current API **relatively** stable. Once this hits a 1.x release,
the API will be even more stable. I would like to get some definitive
broader usage of the client before deeming things unchanging.

I've spent a good amount of time integration testing my [kafka-go][2] client
that this program uses. The main thing I have currently been unable to test is
closest replica fetching, which is only theoretically supported. It is worth it
to read the stability status in the kafka-go repo as well if using this client.

[2]: https://github.com/twmb/kafka-go/

I would love confirmation that this program has been used more broadly, and
would love to start a "Users" section below. With this confirmation, I will
push a 1.x release.

## Getting Started

If you have a go installation, you can simply

```
go install github.com/twmb/kcl
```

This will install kcl from the latest release. You can optionally suffix the
`go get` with `@v#.#.#` to install a specific version.

Otherwise, you can download a release from the
[releases](https://github.com/twmb/kcl/releases) page.

## Configuration

kcl supports configuration through a config file, environment variables, and
config flag overrides, with the config values being defined in that order.
By default, kcl searches your OS's user config dir for a `kcl` directory and
a `config.toml` file in that directory. The default path can be overridden.
As well, multiple configs can easily swapped between with `kcl myconfig`.

The configuration supports TLS, SASL (currently PLAIN and SCRAM), seed brokers,
and a timeout for requests that take timeouts.

To learn more about configuration, use `kcl myconfig help`.

## Autocompletion

Thanks to [cobra][2], autocompletion exists for bash, zsh, and powershell.

[3]: https://github.com/spf13/cobra

As an example of what to put in your .bashrc,

```bash
if [ -f /etc/bash_completion ] && ! shopt -oq posix; then
    . /etc/bash_completion
    . <(kcl misc gen-autocomplete -kbash)
fi
```

## Transactions

Transactions are supported by consuming a batch of records, executing a command
and passing the records to the command's STDIN, reading the modified records
via the command's STDOUT, and publishing those records.

Input to the program and output from the program is controlled through the same
syntax as consuming and producing, and the `--rw` flag is a shortcut to say that
the input and output will use the same format.

As an example, the following command:

```
kcl transact --rw '%V{b4}%v' -dtxn -g group -t foo -x mytxn -v ./command
```

reads topic `foo` in group `group`, executes `./command`, writes all record
values to it prefixed with the four byte big endian value length, reads
back records in the same format, and produces to topic `txn` all using the
transactional id `mytxn`.

## Group Consuming

Group consuming is supported with the `-g` or `--group` flag to `kcl consume`
or `kcl transact`. The default balancer is the cooperative-sticky balancer,
which was introduced with incremental rebalancing in Kafka 2.4.0. This balancer
is incompatible with the previous eager balancers (roundrobin, range, sticky),
thus if you are using kcl with existing groups that have members using eager
balancing strategies, be sure to specify a different balancer.

## API at a glance

Be sure to `help` any command before using it to understand the full syntax.

```
kcl
 consume
 produce
 transact
 misc
   api-versions
   probe-version
   gen-autocomplete
   errcode
   raw-req
   list-offsets
 admin
   elect-leaders
   delete-records
   alter-partition-assignments
   list-partition-reassignments
   alter-replica-log-dirs
   describe-log-dirs
 dtoken
   create
   renew
   describe
   expire
 acl
   create
   describe
   delete
 group
   list
   describe
   delete
   offset-delete
 topic
   create
   delete
   add-partitions
 metadata
 configs
   alter
   describe
 myconfig
   unlink
   link
   dump
   help
   ls
```
