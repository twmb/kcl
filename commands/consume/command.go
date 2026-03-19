package consume

import (
	"time"

	"github.com/spf13/cobra"
)

func (c *consumption) command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "consume TOPICS...",
		Short: "Consume topic records",
		Long:  help,
		Args:  cobra.MinimumNArgs(1), // topic
		Run: func(_ *cobra.Command, args []string) {
			c.run(args)
		},
	}
	cmd.Flags().StringVarP(&c.group, "group", "g", "", "consumer group to assign")
	cmd.Flags().StringVar(&c.shareGroup, "share-group", "", "share group to consume from (Kafka 4.0+, mutually exclusive with --group)")
	cmd.Flags().StringVarP(&c.groupAlg, "balancer", "b", "cooperative-sticky", "group balancer to use if group consuming (range, roundrobin, sticky, cooperative-sticky)")
	cmd.Flags().StringVarP(&c.instanceID, "instance-id", "i", "", "group instance ID to use for consuming; empty means none (implies static membership, Kafka 2.3.0+)")
	cmd.Flags().Int32SliceVarP(&c.partitions, "partitions", "p", nil, "comma delimited list of specific partitions to consume")
	cmd.Flags().StringVarP(&c.offset, "offset", "o", "start", "offset to consume from (start, end, +N, -N, N, N:M, :end, @timestamp, @T1:T2)")
	cmd.Flags().IntVarP(&c.num, "num", "n", 0, "quit after consuming this number of records; 0 is unbounded")
	cmd.Flags().IntVar(&c.numPerPartition, "num-per-partition", 0, "stop printing individual partitions after this many records; 0 is unbounded")
	cmd.Flags().StringVarP(&c.format, "format", "f", `%v\n`, "output format")
	cmd.Flags().BoolVarP(&c.regex, "regex", "r", false, "parse topics as regex; consume any topic that matches any expression")
	cmd.Flags().Int32Var(&c.fetchMaxBytes, "fetch-max-bytes", 1<<20, "maximum amount of bytes per fetch request per broker")
	cmd.Flags().DurationVar(&c.fetchMaxWait, "fetch-max-wait", 5*time.Second, "maximum amount of time to wait when fetching from a broker before the broker replies")
	cmd.Flags().StringVar(&c.rack, "rack", "", "the rack to use for fetch requests; setting this opts in to nearest replica fetching (Kafka 2.2.0+)")
	cmd.Flags().BoolVar(&c.readUncommitted, "read-uncommitted", false, "opt in to reading uncommitted offsets")
	cmd.Flags().BoolVar(&c.printControlRecords, "print-control-records", false, "include control records (transaction markers) in output")
	cmd.Flags().Int32Var(&c.fetchMaxPartitionBytes, "fetch-max-partition-bytes", 0, "per-partition byte limit for fetch requests (0 uses broker default)")
	cmd.Flags().DurationVar(&c.timeout, "timeout", 0, "exit if no message received for this duration (0 is no timeout)")
	cmd.Flags().StringArrayVarP(&c.grepPatterns, "grep", "G", nil, "filter records (k:, v:, hk:, hv:, h:NAME=, t: with optional ! negation; repeatable, AND'd)")
	cmd.Flags().StringVar(&c.protoFile, "proto-file", "", "an optional proto source file or protoset file to decode protobuf messages, requires --proto-message")
	cmd.Flags().StringVar(&c.protoMessage, "proto-message", "", "the proto.message structure in --proto-file to use for decoding, requires --proto-file")
	cmd.MarkFlagsRequiredTogether("proto-file", "proto-message")
	return cmd
}

const help = `Consume topic records and print them.

This function consumes Kafka topics and prints the records with a configurable
format. The output format takes similar arguments as kafkacat, with the default
being to newline delimit record values.

The input topics can be regular expressions with the --regex (-r) flag.

Format options:
  %t    topic name
  %T    topic name length
  %k    record key
  %K    record key length
  %v    record value
  %V    record value length
  %h    begin the header specification
  %H    number of headers
  %p    record partition
  %o    record offset
  %e    record leader epoch
  %d    record timestamp (date)
  %a    record attributes (formatting required, see below)
  %x    record producer id
  %y    record producer epoch
  %D    share group delivery count (0 if not share)
  %A    share group acquisition deadline (timestamp, like %d)

  %[    partition log start offset
  %|    partition last stable offset
  %]    partition high watermark

  %i    format iteration number (starts at 1)
  %%    percent sign
  %{    left brace
  %}    right brace
  \n    newline
  \r    carriage return
  \t    tab
  \\    slash
  \xNN  any ASCII character

Headers have their own internal format:
  %k    header key
  %K    header key length
  %v    header value
  %V    header value length

For example, "%H %h{%k %v }" prints the header count, then each header's
key and value with a trailing space.


TEXT FORMATTING

Topics, keys, and values support encoding modifiers in braces:
  %v{base64}         base64 encode
  %v{base64raw}      base64 encode without padding
  %k{hex}            hex encode
  %v{unpack{>iIqQ}}  unpack binary data (see below)

Unpack uses Python struct-like syntax inside nested delimiters:

  Endianness:
    >    big endian (default)
    <    little endian

  Integers:
    b    int8                  B    uint8
    h    int16                 H    uint16
    i    int32                 I    uint32
    q    int64                 Q    uint64

  Other:
    x    pad byte (skip)
    c    any single byte       .    alias for c
    s    rest of input as string
    $    assert end of input

Endianness switches can appear anywhere and affect everything after.


NUMBER FORMATTING

All number verbs (%T, %K, %V, %H, %p, %o, %e, %i, %x, %y, %[, %|, %])
accept a brace modifier controlling how the number is printed. Without braces,
numbers are printed as decimal text.

  Decimal:
    ascii      decimal text, e.g. "12345" (the default)
    number     alias for ascii

  Hex:
    hex        as many hex characters as needed
    hex64      16 hex characters (zero-padded)
    hex32      8 hex characters
    hex16      4 hex characters
    hex8       2 hex characters
    hex4       1 hex character

  Big endian binary:
    big64      8 bytes
    big32      4 bytes
    big16      2 bytes
    big8       1 byte

  Little endian binary:
    little64   8 bytes
    little32   4 bytes
    little16   2 bytes
    little8    1 byte

  Other:
    byte       single byte (alias for big8)
    bool       "true" if non-zero, "false" if zero

For example, %T{big64} prints the topic name length as an 8-byte big endian.


TIME FORMATTING

%d without braces prints the millisecond timestamp as a number.
%d with braces supports:
  %d{strftime[%F %T]}     strftime formatting
  %d{go#2006-01-02#}      Go time formatting

The inner delimiter can be repeated for nesting (e.g., [[%F]]).


ATTRIBUTES

%a requires braces specifying which attribute:
  %a{compression}              "none", "gzip", "snappy", "lz4", "zstd"
  %a{compression;number}       compression as a number
  %a{timestamp-type}           -1, 0 (client), or 1 (broker)
  %a{transactional-bit}        1 if transactional, else 0
  %a{transactional-bit;bool}   "true" / "false"
  %a{control-bit}              1 if control record, else 0
  %a{control-bit;bool}         "true" / "false"


EXAMPLES

A basic example:
  -f 'Topic %t [%p] at offset %o @%d{strftime[%F %T]}: key %k: %v\n'

To mirror a topic (consume with this format, pipe to produce with the same):
  -f '%K{big32}%k%V{big32}%v%H{big32}%h{%K{big32}%k%V{big32}%v}'


REMARKS

Note that this command allows you to consume the Kafka special internal topics
__consumer_offsets and __transaction_state. To do so, either of these topics
must be the only topic specified.

For __consumer_offsets, to dump information about a specific group, use the -G
flag. Doing so will also hide transaction markers. For __transaction_state, you
can use -G to dump information about a specific transactional ID.

Combined with producing, these two commands allow you to easily mirror a topic.
`
