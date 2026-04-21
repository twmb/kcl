package consume

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/out"
)

func (c *consumption) command() *cobra.Command {
	var topicFlags []string
	cmd := &cobra.Command{
		Use:   "consume [TOPICS...]",
		Short: "Consume topic records",
		Long:  help,
		RunE: func(_ *cobra.Command, args []string) error {
			topics := append(args, topicFlags...)
			if len(topics) == 0 {
				return out.Errf(out.ExitUsage, "at least one topic is required (positional or --topic/-t)")
			}
			return c.run(topics)
		},
	}
	cmd.Flags().StringArrayVarP(&topicFlags, "topic", "t", nil, "topic to consume (repeatable; alternative to positional args)")
	cmd.Flags().StringVarP(&c.group, "group", "g", "", "consumer group to assign")
	cmd.Flags().StringVar(&c.shareGroup, "share-group", "", "share group to consume from (Kafka 4.0+, mutually exclusive with --group)")
	cmd.Flags().StringVar(&c.shareAckType, "share-ack-type", "accept", "share group ack type: accept (mark processed), release (peek, return to pool for redelivery), reject (drain permanently, bumps delivery count); only with --share-group")
	cmd.Flags().StringVar(&c.groupAlg, "balancer", "cooperative-sticky", "group balancer to use if group consuming (range, roundrobin, sticky, cooperative-sticky)")
	cmd.Flags().StringVarP(&c.instanceID, "instance-id", "i", "", "group instance ID to use for consuming; empty means none (implies static membership, Kafka 2.3.0+)")
	cmd.Flags().Int32SliceVarP(&c.partitions, "partitions", "p", nil, "comma delimited list of specific partitions to consume")
	cmd.Flags().StringVarP(&c.offset, "offset", "o", "start", "offset to consume from (start, end, +N, -N, N, N:M, :end, @timestamp, @T1:T2)")
	cmd.Flags().IntVarP(&c.num, "num", "n", 0, "quit after consuming this number of records; 0 is unbounded")
	cmd.Flags().IntVar(&c.numPerPartition, "num-per-partition", 0, "stop printing individual partitions after this many records; 0 is unbounded")
	cmd.Flags().StringVarP(&c.format, "format", "f", `%v\n`, "record output format")
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

Slash escapes:
  \t    tab
  \n    newline
  \r    carriage return
  \\    backslash
  \xNN  any byte (hex)

Percent verbs:
  %t    topic
  %T    topic length
  %k    key
  %K    key length
  %v    value
  %V    value length
  %h    begin the header specification
  %H    number of headers
  %p    partition
  %o    offset
  %e    leader epoch
  %d    timestamp (date, formatting described below)
  %a    record attributes (formatting required, described below)
  %x    producer id
  %y    producer epoch
  %D    share group delivery count (0 if not from a share group)
  %A    share group acquisition deadline (timestamp, like %d; 0 if not share)
  %[    partition log start offset
  %|    partition last stable offset
  %]    partition high watermark
  %i    format iteration number (starts at 1)
  %%    percent sign
  %{    left brace
  %}    right brace


HEADER SPECIFICATION

%h opens a nested format that is applied to each header. Inside the braces,
the key and value escapes are:
  %K    header key length
  %k    header key
  %V    header value length
  %v    header value

For example, "%H %h{%k %v }" will print the number of headers, and then each
header key and value with a space after each.


NUMBERS

All number verbs accept braces that control how the number is printed:
  %T{ascii}       the default, print the number as ascii
  %T{number}      alias for ascii
  %T{hex64}       print 16 hex characters for the number
  %T{hex32}       print 8 hex characters for the number
  %T{hex16}       print 4 hex characters for the number
  %T{hex8}        print 2 hex characters for the number
  %T{hex4}        print 1 hex character for the number
  %T{hex}         print as many hex characters as necessary for the number
  %T{big64}       print the number in big endian uint64 format
  %T{big32}       print the number in big endian uint32 format
  %T{big16}       print the number in big endian uint16 format
  %T{big8}        alias for byte
  %T{little64}    print the number in little endian uint64 format
  %T{little32}    print the number in little endian uint32 format
  %T{little16}    print the number in little endian uint16 format
  %T{little8}     alias for byte
  %T{byte}        print the number as a single byte
  %T{bool}        print "true" if the number is non-zero, otherwise "false"

All numbers are truncated as necessary per each given format.


TIMESTAMPS

Timestamps (%d, %A) can be specified in three formats: plain number formatting,
native Go timestamp formatting, or strftime formatting. Number formatting
follows the rules above using the millisecond timestamp value. Go and strftime
have further internal format options:

  %d{go##2006-01-02T15:04:05Z07:00##}
  %d{strftime[%F]}

An arbitrary amount of pounds, braces, and brackets are understood before
beginning the actual timestamp formatting. For Go formatting, the format is
simply passed to the time package's AppendFormat function. For strftime, all
"man strftime" options are supported. Time is always in UTC.


ATTRIBUTES

Record attributes require formatting, where each formatting option selects
which attribute to print and how to print it.

  %a{compression}              prints "none", "gzip", "snappy", "lz4", "zstd"
  %a{compression;number}       compression as a number
  %a{compression;hex8}         compression as hex
  %a{timestamp-type}           -1 for pre-0.10, 0 for client, 1 for broker
  %a{timestamp-type;big64}     timestamp type as big endian uint64
  %a{transactional-bit}        1 if transactional, else 0
  %a{transactional-bit;bool}   "true" / "false"
  %a{control-bit}              1 if control record, else 0
  %a{control-bit;bool}         "true" / "false"

The ";number" suffix accepts any number format from the NUMBERS section.


TEXT

Topics, keys, and values have "base64", "base64raw", "hex", and "unpack"
formatting options:

  %t{hex}
  %k{unpack{<bBhH>iIqQc.$}}
  %v{base64}
  %v{base64raw}

Unpack formatting is inside of enclosing pounds, braces, or brackets, the
same way that timestamp formatting is understood. The syntax roughly follows
Python's struct packing/unpacking rules:

  x    pad character (does not parse input)
  <    parse what follows as little endian
  >    parse what follows as big endian
  b    signed byte
  B    unsigned byte
  h    int16 ("half word")
  H    uint16 ("half word")
  i    int32
  I    uint32
  q    int64 ("quad word")
  Q    uint64 ("quad word")
  c    any character
  .    alias for c
  s    consume the rest of the input as a string
  $    match the end of the line (append error string if anything remains)

Unlike Python, a '<' or '>' can appear anywhere in the format string and
affects everything that follows. It is possible to switch endianness multiple
times. If the parser needs more data than available, or if more input remains
after '$', an error message will be appended.


EXAMPLES

Default (value only, newline-delimited):
  -f '%v\n'

Key and value, tab-separated:
  -f '%k\t%v\n'

Timestamped, with topic/partition/offset:
  -f '%d{strftime[%F %T]} %t[%p]@%o %v\n'

Inspect headers:
  -f '%v headers=%H %h{%k=%v,}\n'

Show share-group delivery count (with --share-group):
  -f '%v delivery=%D\n'


REMARKS

Note that this command allows you to consume the Kafka special internal topics
__consumer_offsets and __transaction_state. To do so, either of these topics
must be the only topic specified.

For __consumer_offsets, to dump information about a specific group, use the -G
flag. Doing so will also hide transaction markers. For __transaction_state, you
can use -G to dump information about a specific transactional ID.
`
