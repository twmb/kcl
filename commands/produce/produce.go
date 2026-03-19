package produce

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	var (
		topicFlag            string
		informat             string
		verboseFormat        string
		compression          string
		acks                 int
		retries              int
		tombstone            bool
		partition            int32
		deliveryTimeout      time.Duration
		maxMessageBytes      int32
		allowAutoTopicCreate bool
	)

	cmd := &cobra.Command{
		Use:   "produce [TOPIC]",
		Short: "Produce records.",
		Long: `Produce records, optionally to a specific topic, from stdin.

By default, producing reads newline delimited, unkeyed records from stdin.
The input format (-f) can be specified with delimiters or with sized numbers,
and the format can parse a topic, key, value, and header keys and values.

The output format (-o) controls what is printed after each record is produced
(e.g., to confirm topic/partition/offset). The output format uses the same
syntax as "kcl consume --format"; see "kcl consume --help" for full output
format documentation.

Slash escapes:
  \t    tab
  \n    newline
  \r    carriage return
  \\    backslash
  \xNN  any byte (hex)

Percent verbs for reading records from stdin:
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
  %d    timestamp (read as milliseconds)
  %x    producer id
  %y    producer epoch
  %%    percent sign
  %{    left brace
  %}    right brace

If using length / number verbs (i.e., "sized" verbs), they must occur before
what they are sizing.

If the format includes %t, the topic is parsed from input and no topic
argument should be given on the command line.


HEADER SPECIFICATION

Similar to number formatting, headers are parsed using a nested primitive
format option, accepting the key and value escapes:
  %K    header key length
  %k    header key
  %V    header value length
  %v    header value


NUMBERS

All size numbers can be parsed in the following ways:
  %V{ascii}       parse numeric digits until a non-numeric (the default)
  %V{number}      alias for ascii
  %V{hex64}       read 16 hex characters for the number
  %V{hex32}       read 8 hex characters for the number
  %V{hex16}       read 4 hex characters for the number
  %V{hex8}        read 2 hex characters for the number
  %V{hex4}        read 1 hex character for the number
  %V{big64}       read the number as big endian uint64 format
  %V{big32}       read the number as big endian uint32 format
  %V{big16}       read the number as big endian uint16 format
  %V{big8}        alias for byte
  %V{little64}    read the number as little endian uint64 format
  %V{little32}    read the number as little endian uint32 format
  %V{little16}    read the number as little endian uint16 format
  %V{little8}     read the number as a byte
  %V{byte}        read the number as a byte
  %V{bool}        read "true" as 1, "false" as 0
  %V{3}           read 3 characters (any number)

Unlike record formatting, timestamps can only be read as numbers because Go
or strftime formatting can both be variable length and do not play too well
with delimiters. Timestamp numbers are read as milliseconds.


TEXT

Topics, keys, and values can be decoded using "base64", "hex", "json", and
"re" (regex) formatting options. Any size specification is the size of the
encoded value actually being read (i.e., size as seen, not size when decoded).
JSON values are compacted after being read.

  %T%t{hex}     -  4abcd reads four hex characters "abcd"
  %V%v{base64}  -  2z9 reads two base64 characters "z9"
  %v{json} %k   -  {"foo" : "bar"} foo reads a JSON object and then "foo"

As well, these text options can be parsed with regular expressions:

  %k{re[\d*]}%v{re[\s+]}


EXAMPLES

To read a newline delimited file, each line a record (no keys):
  -f '%v\n'

To read that same file, with each line alternating key/value:
  -f '%k\n%v\n'

To read a file where each line has a key and value beginning with "key: " and
", value: ":
  -f 'key: %k, value: %v\n'

To read a binary file with keys and values having four byte big endian
prefixes:
  -f '%K{big32}%k%V{big32}%v'

To read a similar file that also has a count of headers (big endian short) and
then headers (also sized with big endian shorts) following the value:
  -f '%K{big32}%k%V{big32}%v%H{big16}%h{%K{big16}%k%V{big16}%v}'

To read a similar file that has the topic to produce to before the key, also
sized with a big endian short:
  -f '%T{big16}%t%K{big32}%k%V{big32}%v%H{big16}%h{%K{big16}%k%V{big16}%v}'

To read a compact key, value, and single header, with each piece being 3 bytes:
  -f '%K{3}%V{3}%H{1}%k%v%h{%K{3}%k%V{3}%v}'

To read JSON-encoded values:
  -f '%v{json}\n'

To show partition and offset for each produced record:
  -o 'produced to %t[%p]@%o\n'
`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if topicFlag != "" {
				if len(args) > 0 {
					return fmt.Errorf("topic specified both as -t flag and positional argument")
				}
				args = []string{topicFlag}
			}

			reader, err := kgo.NewRecordReader(os.Stdin, informat)
			if err != nil {
				return fmt.Errorf("unable to parse in format: %v", err)
			}

			var verboseFormatter *kgo.RecordFormatter
			var verboseBuf []byte
			if verboseFormat != "" {
				verboseFormatter, err = kgo.NewRecordFormatter(verboseFormat)
				if err != nil {
					return fmt.Errorf("unable to parse output-format: %v", err)
				}
			}

			var codec kgo.CompressionCodec
			switch compression {
			case "none":
				codec = kgo.NoCompression()
			case "gzip":
				codec = kgo.GzipCompression()
			case "snappy":
				codec = kgo.SnappyCompression()
			case "lz4":
				codec = kgo.Lz4Compression()
			case "zstd":
				codec = kgo.ZstdCompression()
			default:
				return fmt.Errorf("invalid compression codec %q", codec)
			}
			cl.AddOpt(kgo.ProducerBatchCompression(codec))

			switch acks {
			case -1:
				cl.AddOpt(kgo.RequiredAcks(kgo.AllISRAcks()))
			case 0:
				cl.AddOpt(kgo.RequiredAcks(kgo.NoAck()))
				cl.AddOpt(kgo.DisableIdempotentWrite())
			case 1:
				cl.AddOpt(kgo.RequiredAcks(kgo.LeaderAck()))
			default:
				return fmt.Errorf("invalid acks %d not in allowed -1, 0, 1", acks)
			}

			if partition > -1 {
				cl.AddOpt(kgo.RecordPartitioner(kgo.ManualPartitioner()))
			}

			if retries > -1 {
				cl.AddOpt(kgo.RecordRetries(retries))
			}
			if deliveryTimeout > 0 {
				cl.AddOpt(kgo.RecordDeliveryTimeout(deliveryTimeout))
			}
			if maxMessageBytes > 0 {
				cl.AddOpt(kgo.ProducerBatchMaxBytes(maxMessageBytes))
			}
			if allowAutoTopicCreate {
				cl.AddOpt(kgo.AllowAutoTopicCreation())
			}

			for {
				r, err := reader.ReadRecord()
				if err != nil {
					if err != io.EOF {
						return fmt.Errorf("final error: %v", err)
					}
					break
				}
				if tombstone && len(r.Value) == 0 {
					r.Value = nil
				}
				if r.Topic == "" {
					if len(args) == 0 {
						return fmt.Errorf("topic missing from both produce line and from parse format")
					}
					r.Topic = args[0]
				}

				// Override the partition in the case when the manual partitioner is used.
				r.Partition = partition

				cl.Client().Produce(context.Background(), r, func(r *kgo.Record, err error) {
					out.MaybeDie(err, "unable to produce record: %v", err)
					if verboseFormatter != nil {
						verboseBuf = verboseFormatter.AppendRecord(verboseBuf[:0], r)
						os.Stdout.Write(verboseBuf)
					}
				})
			}

			cl.Client().Flush(context.Background())
			return nil
		},
	}

	cmd.Flags().StringVarP(&topicFlag, "topic", "t", "", "topic to produce to (alternative to positional argument)")
	cmd.Flags().StringVarP(&informat, "format", "f", "%v\n", "record only delimiter")
	cmd.Flags().StringVarP(&verboseFormat, "output-format", "o", "", "format string for produced record output (topic, partition, offset of each record)")
	cmd.Flags().StringVarP(&compression, "compression", "z", "snappy", "compression to use for producing batches (none, gzip, snappy, lz4, zstd)")
	cmd.Flags().IntVar(&acks, "acks", -1, "number of acks required, -1 is all in sync replicas, 1 is leader replica only, 0 is no acks required (0 disables idempotency)")
	cmd.Flags().IntVar(&retries, "retries", -1, "number of times to retry producing if non-negative")
	cmd.Flags().BoolVarP(&tombstone, "tombstone", "Z", false, "produce empty values as tombstones")
	cmd.Flags().Int32VarP(&partition, "partition", "p", -1, "a specific partition to produce to, if non-negative")
	cmd.Flags().DurationVar(&deliveryTimeout, "delivery-timeout", 0, "per-record delivery timeout (0 is no timeout)")
	cmd.Flags().Int32Var(&maxMessageBytes, "max-message-bytes", 0, "max record batch size in bytes (0 uses broker default)")
	cmd.Flags().BoolVar(&allowAutoTopicCreate, "allow-auto-topic-creation", false, "allow auto-creation of topics that don't exist")

	return cmd
}
