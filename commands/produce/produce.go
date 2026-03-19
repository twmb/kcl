package produce

import (
	"context"
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
The input format can be specified with delimiters or with sized numbers, and
the format can parse a topic, key, value, and header keys and values.

Escape sequences:
  \n    newline
  \r    carriage return
  \t    tab
  \\    backslash
  \xNN  any byte (hex)

Format options:
  %t    topic name
  %T    topic name length
  %k    record key
  %K    record key length
  %v    record value
  %V    record value length
  %h    begin the header specification
  %H    number of headers
  %p    partition
  %o    offset
  %e    leader epoch
  %d    timestamp (milliseconds)
  %x    producer id
  %y    producer epoch
  %%    percent sign
  %{    left brace
  %}    right brace

Headers have their own internal format:
  %k    header key
  %K    header key length
  %v    header value
  %V    header value length


TEXT DECODING

Topics, keys, and values support "base64", "hex", "json", and "re" decoding:
  %v{base64}     decode base64 input
  %k{hex}        decode hex input
  %v{json}       read a JSON value (object, array, string, number, bool, null)
  %k{re[\d+]}    read input matching a regular expression

Size specifications refer to the encoded size actually read.


NUMBER FORMATTING

Size and number verbs (%T, %K, %V, %H, %p, %o, %e, %d, %x, %y) accept a
brace modifier:
  ascii / number    parse decimal digits (the default)
  hex64..hex4       read fixed-width hex (16, 8, 4, 2, 1 chars)
  big64 / big32 / big16 / big8   read big endian binary
  little64 / little32 / little16 / little8   read little endian binary
  byte              read one byte
  bool              read "true" as 1, "false" as 0
  ###               read exactly N characters as a number

Without braces, numbers default to ascii. Note that ascii parsing reads digits
greedily—if the value starts with digits and follows immediately after a size
verb, the parser will consume those digits as part of the number. Use a space
or delimiter to separate them.


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
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			reader, err := kgo.NewRecordReader(os.Stdin, informat)
			out.MaybeDie(err, "unable to parse in format: %v", err)

			var verboseFormatter *kgo.RecordFormatter
			var verboseBuf []byte
			if verboseFormat != "" {
				verboseFormatter, err = kgo.NewRecordFormatter(verboseFormat)
				out.MaybeDie(err, "unable to parse output-format: %v", err)
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
				out.Die("invalid compression codec %q", codec)
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
				out.Die("invalid acks %d not in allowed -1, 0, 1", acks)
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
						out.Die("final error: %v", err)
					}
					break
				}
				if tombstone && len(r.Value) == 0 {
					r.Value = nil
				}
				if r.Topic == "" {
					if len(args) == 0 {
						out.Die("topic missing from both produce line and from parse format")
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
		},
	}

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
