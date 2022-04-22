package produce

import (
	"bufio"
	"context"
	"io"
	"os"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/format"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	var (
		informat      string
		maxBuf        int
		verboseFormat string
		compression   string
		escapeChar    string
		acks          int
		retries       int
		tombstone     bool
		partition     int
	)

	cmd := &cobra.Command{
		Use:   "produce [TOPIC]",
		Short: "Produce records.",
		Long: `Produce records, optionally to a defined, from stdin.

By default, producing consumes newline delimited, unkeyed records from stdin.
The input format can be specified with delimiters or with sized numbers, and
the format can parse a topic, key, value, and header keys and values. Headers
only support sized parsing.

By default, if using delimiters, each field must be under 64KiB in length. This
can be changed with the --max-delim-buf flag.

Delimiters understand \n, \r, \t, and \xXX (hex) escape sequences.

Format options:
  %t    topic name
  %T    topic name length
  %k    record key
  %K    record key length
  %v    record value
  %V    record value length
  %h    begin the header specification
  %H    number of headers
  %%    percent sign
  %{    left brace (required if a brace is after another format option)
  \n    newline
  \r    carriage return
  \t    tab
  \xXX  any ASCII character (input must be hex)

Headers have their own internal format (the same as keys and values above):
  %v    header value
  %V    header value length
  %k    header key
  %K    header key length


NUMBER FORMATTING

It is recommended to have a number specifier "kind" in braces, with the kinds
being:
  big8     eight byte unsigned big endian
  big4     four byte unsigned big endian
  big2     two byte unsigned big endian
  little8  eight byte unsigned little endian
  little4  four byte unsigned little endian
  little2  two byte unsigned little endian
  byte     single byte
  b8       alias for big8
  b4       alias for big4
  b2       alias for big2
  l8       alias for little8
  l4       alias for little4
  l2       alias for little2
  b        alias for byte
  ascii    parses ascii numbers until the number stops
  a        alias for ascii
  ###      an exact number of bytes (in ascii digits)

If braces are elided, the default is to parse ascii. Note that ascii parsing
can potentially lead to a footgun; if the value being parsed begins with a
number, and there is no space between the size definition and the value being
parsed, then the parser will continue reading numbers from the beginning of the
value. For example, for "%V%v" with a value of "2a", the raw text in would be
22a (two bytes, then the value), but that would be parsed as a 22 byte value.
To avoid this problem, you would need to do "%V %v" and "2 2a".

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
  -f '%K{b4}%k%V{b4}%v'

To read a similar file that also has a count of headers (big endian short) and
then headers (also sized with big endian shorts) following the value:
  -f '%K{b4}%k%V{b4}%v%H{b2}%h{%K{b2}%k%V{b2}%v}'

To read a similar file that has the topic to produce to before the key, also
sized with a big endians short:
  -f '%T{b2}%t%K{b4}%k%V{b4}%v%H{b2}%h{%K{b2}%k%V{b2}%v}'

The same, but with a space trailing every field:
  -f '%T{b2}%t %K{b4}%k %V{b4}%v %H{b2}%h{%K{b2}%k %V{b2}%v }'

To read a compact key, value, and single header, with each piece being 3 bytes:
  -f '%K{3}%V{3}%H{1}%k%v%h{%K{3}%k%V{3}%v}'


REMARKS

Delimiters can be of arbitrary length, but must match exactly. When parsing
with sizes, you can ignore any unnecessary characters by putting fake
delimiters in the parsing format. Since the parser ignores indiscriminately,
you may as well use characters that make reading the format a bit easier.

If you do not like %, you can switch the escape character with a flag.
Unfortunately, with exact sizing, the format string is unavoidably noisy.
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if len(escapeChar) == 0 {
				out.Die("invalid empty escape character")
			}
			escape, size := utf8.DecodeRuneInString(escapeChar)
			if size != len(escapeChar) {
				out.Die("invalid multi character escape character")
			}

			reader, err := format.NewReader(informat, escape, maxBuf, os.Stdin, tombstone)
			out.MaybeDie(err, "unable to parse in format: %v", err)
			if reader.ParsesTopic() && len(args) == 1 {
				out.Die("cannot produce to a specific topic; the parse format specifies that it parses a topic")
			}

			var verboseFn func([]byte, *kgo.Record, *kgo.FetchPartition) []byte
			var verboseBuf []byte
			if verboseFormat != "" {
				verboseFn, err = format.ParseWriteFormat(verboseFormat, escape)
				out.MaybeDie(err, "unable to parse verbose-format: %v", err)
			}

			if !reader.ParsesTopic() && len(args) == 0 {
				out.Die("topic missing from both produce line and from parse format")
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

			p := &kgo.FetchPartition{}
			for {
				r, err := reader.Next()
				if err != nil {
					if err != io.EOF {
						out.Die("final error: %v", err)
					}
					break
				}
				if !reader.ParsesTopic() {
					r.Topic = args[0]
				}

				if partition > -1 {
					r.Partition = int32(partition)
				}

				cl.Client().Produce(context.Background(), r, func(r *kgo.Record, err error) {
					out.MaybeDie(err, "unable to produce record: %v", err)
					if verboseFn != nil {
						verboseBuf = verboseFn(verboseBuf[:0], r, p)
						os.Stdout.Write(verboseBuf)
					}
				})
			}

			cl.Client().Flush(context.Background())
		},
	}

	cmd.Flags().StringVarP(&informat, "format", "f", "%v\n", "record only delimiter")
	cmd.Flags().StringVarP(&verboseFormat, "verbose-format", "v", "", "if non-empty, what to write to stdout when a record is successfully produced")
	cmd.Flags().IntVar(&maxBuf, "max-delim-buf", bufio.MaxScanTokenSize, "maximum input to buffer before a delimiter is required, if using delimiters")
	cmd.Flags().StringVarP(&compression, "compression", "z", "snappy", "compression to use for producing batches (none, gzip, snappy, lz4, zstd)")
	cmd.Flags().StringVarP(&escapeChar, "escape-char", "c", "%", "character to use for beginning a record field escape (accepts any utf8, for both format and verbose-format)")
	cmd.Flags().IntVar(&acks, "acks", -1, "number of acks required, -1 is all in sync replicas, 1 is leader replica only, 0 is no acks required (0 disables idempotency)")
	cmd.Flags().IntVar(&retries, "retries", -1, "number of times to retry producing if non-negative")
	cmd.Flags().BoolVarP(&tombstone, "tombstone", "Z", false, "produce empty values as tombstones")
	cmd.Flags().IntVarP(&partition, "partition", "p", -1, "the partition to produce to")

	return cmd
}
