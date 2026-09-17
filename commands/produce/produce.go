package produce

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
	"github.com/twmb/kcl/serde"
)

// recordReader is what the produce loop reads from: kgo's format string
// reader, or ours for -f json.
type recordReader interface {
	ReadRecord() (*kgo.Record, error)
}

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
		headers              []string
		key                  string

		valueSchemaSpec string
		keySchemaSpec   string
	)

	cmd := &cobra.Command{
		Use:   "produce [TOPIC]",
		Short: "Produce records.",
		Long: `Produce records.

Produce records, optionally to a specific topic, from stdin.

By default, producing reads newline delimited, unkeyed records from stdin.
The input format (-f) can be specified with delimiters or with sized numbers,
and the format can parse a topic, key, value, and header keys and values. The
bare word "json" reads JSON objects instead; see JSON INPUT below.

The topic comes from the argument, -t/--topic, a %t in the input format, or
the "topic" of a JSON input object; with none of those, producing is an error
before stdin is read.

-k/--key gives a key to every record whose input carries none. A %k in the
input format wins over -k, as does a key in a JSON input object; -k fills in
where the input has no key at all.

The output format (-o) controls what is printed after each record is produced
(e.g., to confirm topic/partition/offset). The output format uses the same
syntax as "kcl consume --format"; see "kcl consume --help" for full output
format documentation. The bare word "json" prints one JSON object per record;
see JSON OUTPUT below.

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


JSON INPUT

As a special case, -f/--format set to exactly "json" reads one JSON object per
record, the objects "kcl consume -f json" writes, so a consume can be piped
into a produce:

  {"topic":"orders","partition":3,"key":"user-1","value":"...","headers":[...]}

Only the exact word is reserved; -f 'json%v' is still an ordinary format.

The fields, and what each one does:
  topic         used unless a topic is given as the argument or -t, which then
                applies to every object; no topic anywhere is an error
  partition     honored unless -p is given, so a dump replays onto the
                partitions it came from; drop it (jq 'del(.partition)') to let
                the partitioner place the records
  timestamp     milliseconds, kept when present
  key, value    a string is its bytes; null stays null (a tombstone), distinct
                from ""; an object or array (what --decode writes) is produced
                as its compact text, which --schema can then encode
  key_base64, value_base64
                bytes that are not UTF-8, as consume writes them
  headers       [{"key":..,"value":..}], with value_base64 as above
  offset, leader_epoch, delivery_count
                accepted and ignored; the cluster assigns them

A misspelled field is an error.


JSON OUTPUT

-o/--output-format set to exactly "json" prints one JSON object per record as
it is produced:

  {"topic":"orders","partition":3,"offset":1482,"timestamp":1755645291123,"error":""}

"error" is "" on success and the error text on failure, where "offset" and
"timestamp" are null. Every record is attempted; the exit code is 1 if any
failed. Without -o json a failure stops producing at the first error.


EXAMPLES:

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


SCHEMA REGISTRY

The value (--schema) and/or key (--key-schema) can be encoded into the Schema
Registry wire format: the component is read as JSON (via the normal format
verbs) and encoded to the schema's binary form with the registry's magic-byte +
schema-id header. Point kcl at a registry with -R/--registry (see "kcl registry
--help"). This encodes against an EXISTING schema; it never registers one (use
"kcl registry schema create" to register).

The flag value is a small spec:

  topic[@VERSION]          schema for <topic>-value / <topic>-key (latest)
  NAME[@VERSION]           a subject (bare); e.g. orders-value, orders-value@3
  subject:NAME[@VERSION]   an explicit subject (escape hatch for odd names)
  id:N                     a registered schema id

VERSION is a number or "latest" (default). Any form may add a trailing
#MESSAGE to pick the protobuf message in a multi-message schema. The "topic"
strategy cannot be used when the topic is parsed per record (%t, or -f json
with no topic given); use id: or subject: instead.

Examples:

  # Encode values with the latest registered orders-value schema:
  echo '{"id":"a","age":3}' | kcl produce orders --schema topic

  # By explicit subject/version, or by id:
  kcl produce orders --schema orders-value@3
  kcl produce orders --schema id:8

  # Protobuf, selecting the message; and encoding the key too:
  kcl produce orders --schema topic#com.acme.Order
  kcl produce orders -f '%k %v\n' --key-schema id:7 --schema topic
`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if topicFlag != "" {
				if len(args) > 0 {
					return out.Errf(out.ExitUsage, "topic specified both as -t flag and positional argument")
				}
				args = []string{topicFlag}
			}

			isJSON := informat == jsonFormatName
			// A per-record topic comes from %t, or from -f json with no
			// topic given. Without any of those, there is no topic at
			// all, and we say so before reading stdin.
			perRecordTopic := layoutParses(informat, 't') || (isJSON && len(args) == 0)
			if len(args) == 0 && !isJSON && !layoutParses(informat, 't') {
				return out.Errf(out.ExitUsage, "no topic: give one as an argument or with -t/--topic, or parse it from input with %%t in -f")
			}

			var reader recordReader
			if isJSON {
				reader = newJSONReader(os.Stdin)
			} else {
				r, err := kgo.NewRecordReader(os.Stdin, informat)
				if err != nil {
					return out.Errf(out.ExitUsage, "input format %q: %v", informat, err)
				}
				reader = r
			}

			outJSON := verboseFormat == jsonFormatName
			var verboseFormatter *kgo.RecordFormatter
			if verboseFormat != "" && !outJSON {
				var err error
				verboseFormatter, err = kgo.NewRecordFormatter(verboseFormat)
				if err != nil {
					return out.Errf(out.ExitUsage, "output format %q: %v", verboseFormat, err)
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
				return out.Errf(out.ExitUsage, "invalid compression codec %q", compression)
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
				return out.Errf(out.ExitUsage, "invalid acks %d not in allowed -1, 0, 1", acks)
			}

			// -p sends every record to one partition. Without it, a JSON
			// object's partition is honored and the rest are placed by
			// the default partitioner.
			switch {
			case partition > -1:
				cl.AddOpt(kgo.RecordPartitioner(kgo.ManualPartitioner()))
			case isJSON:
				cl.AddOpt(kgo.RecordPartitioner(newJSONPartitioner()))
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

			var staticHeaders []kgo.RecordHeader
			for _, h := range headers {
				k, v, ok := strings.Cut(h, "=")
				if !ok {
					return out.Errf(out.ExitUsage, "invalid header %q: must be in key=value format", h)
				}
				staticHeaders = append(staticHeaders, kgo.RecordHeader{Key: k, Value: []byte(v)})
			}

			// Build Schema Registry encoders for the value and/or key if a
			// --schema / --key-schema spec was given. Both resolve against the
			// same registry; producing never registers schemas.
			var valueEnc, keyEnc *serde.Encoder
			if valueSchemaSpec != "" || keySchemaSpec != "" {
				scl, err := cl.SchemaRegistryClient()
				if err != nil {
					return out.Errf(out.ExitUsage, "%v", err)
				}
				var topic string
				if len(args) > 0 {
					topic = args[0]
				}

				build := func(flag, raw string, isKey bool) (*serde.Encoder, error) {
					spec, err := parseSchemaSpec(raw)
					if err != nil {
						return nil, out.Errf(out.ExitUsage, "invalid %s: %v", flag, err)
					}
					// A single encoder is resolved up front for the whole run.
					// If the subject is derived from the topic but the input
					// carries a per-record topic, records for other topics
					// would be silently encoded against the wrong schema.
					// Reject that rather than corrupt the stream.
					if spec.DerivesSubject() && perRecordTopic {
						return nil, out.Errf(out.ExitUsage, "%s derives the subject from the topic, but the topic is parsed per record; use %s id:N or %s subject:NAME", flag, flag, flag)
					}
					enc, err := serde.NewEncoder(scl, topic, isKey, spec)
					if err != nil {
						return nil, out.Errf(out.ExitError, "%s: %v", flag, err)
					}
					return enc, nil
				}

				if valueSchemaSpec != "" {
					if valueEnc, err = build("--schema", valueSchemaSpec, false); err != nil {
						return err
					}
				}
				if keySchemaSpec != "" {
					if keyEnc, err = build("--key-schema", keySchemaSpec, true); err != nil {
						return err
					}
				}
			}

			// Promises run on kgo's goroutines, one per broker, so the
			// output buffer and the failure count are shared under a lock.
			var (
				outMu  sync.Mutex
				outBuf []byte
				failed int
			)
			promise := func(r *kgo.Record, err error) {
				outMu.Lock()
				defer outMu.Unlock()
				switch {
				case outJSON:
					if err != nil {
						failed++
					}
					os.Stdout.Write(marshalProduced(r, err))
				case err != nil:
					out.Die("unable to produce record: %v", err)
				case verboseFormatter != nil:
					outBuf = verboseFormatter.AppendRecord(outBuf[:0], r)
					os.Stdout.Write(outBuf)
				}
			}

			setKey := cmd.Flags().Changed("key")
			for {
				r, err := reader.ReadRecord()
				if err != nil {
					if err != io.EOF {
						return out.Errf(out.ExitUsage, "input format %q: %v", informat, err)
					}
					break
				}
				if tombstone && len(r.Value) == 0 {
					r.Value = nil
				}
				// -k fills in a key the input did not set: %k sets one
				// even when it read nothing, and a JSON object's null is
				// deliberate but a missing field is not, so both null and
				// absent take -k.
				if setKey && r.Key == nil {
					r.Key = []byte(key)
				}
				if len(args) > 0 && (isJSON || r.Topic == "") {
					r.Topic = args[0]
				}
				if r.Topic == "" {
					return out.Errf(out.ExitUsage, "no topic: the input record names none and none was given as an argument or with -t/--topic")
				}

				// -p wins. Without it, a JSON object's partition is kept
				// for the partitioner; a format string's %p is not, and
				// never was, the default partitioner placing the record.
				if partition > -1 || !isJSON {
					r.Partition = partition
				}

				// Schema Registry encode: JSON in, schema binary (with the
				// registry wire header) out. Tombstones (nil value) are left
				// untouched.
				if keyEnc != nil && r.Key != nil {
					r.Key, err = keyEnc.Encode(nil, r.Key)
					if err != nil {
						return fmt.Errorf("unable to schema-encode key: %v", err)
					}
				}
				if valueEnc != nil && r.Value != nil {
					r.Value, err = valueEnc.Encode(nil, r.Value)
					if err != nil {
						return fmt.Errorf("unable to schema-encode value: %v", err)
					}
				}

				if len(staticHeaders) > 0 {
					r.Headers = append(r.Headers, staticHeaders...)
				}

				cl.Client().Produce(context.Background(), r, promise)
			}

			cl.Client().Flush(context.Background())
			outMu.Lock()
			defer outMu.Unlock()
			if failed > 0 {
				return out.ErrSilent
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&topicFlag, "topic", "t", "", "topic to produce to (alternative to positional argument)")
	cmd.Flags().StringVarP(&informat, "format", "f", "%v\n", "record input format; the bare word 'json' reads the objects consume -f json writes")
	cmd.Flags().StringVarP(&verboseFormat, "output-format", "o", "", "format string for produced record output (topic, partition, offset of each record); the bare word 'json' prints one JSON object per record")
	cmd.Flags().StringVarP(&key, "key", "k", "", "key for every record whose input carries none (a %k in -f or a key in a JSON object wins)")
	cmd.Flags().StringVarP(&compression, "compression", "z", "snappy", "compression to use for producing batches (none, gzip, snappy, lz4, zstd)")
	cmd.Flags().IntVar(&acks, "acks", -1, "number of acks required, -1 is all in sync replicas, 1 is leader replica only, 0 is no acks required (0 disables idempotency)")
	cmd.Flags().IntVar(&retries, "retries", -1, "number of times to retry producing if non-negative")
	cmd.Flags().BoolVarP(&tombstone, "tombstone", "Z", false, "produce empty values as tombstones")
	cmd.Flags().Int32VarP(&partition, "partition", "p", -1, "a specific partition to produce to, if non-negative")
	cmd.Flags().DurationVar(&deliveryTimeout, "delivery-timeout", 0, "per-record delivery timeout (0 is no timeout)")
	cmd.Flags().Int32Var(&maxMessageBytes, "max-message-bytes", 0, "max record batch size in bytes (0 uses broker default)")
	cmd.Flags().BoolVar(&allowAutoTopicCreate, "allow-auto-topic-creation", false, "allow auto-creation of topics that don't exist")
	cmd.Flags().StringArrayVarP(&headers, "header", "H", nil, "header in key=value format to attach to each record (repeatable)")

	cmd.Flags().StringVarP(&valueSchemaSpec, "schema", "s", "", "Schema Registry encode the value; spec is topic[@ver] | subject[@ver] | subject:NAME[@ver] | id:N, with optional #message (see help)")
	cmd.Flags().StringVar(&keySchemaSpec, "key-schema", "", "Schema Registry encode the key; same spec form as --schema")

	return cmd
}

// layoutParses reports whether a -f format string reads verb. A slash
// escape never starts a verb, %% %{ %} are literals, and the braces after a
// verb hold its options or, for %h, the header format, whose %k and %v are
// the header's own.
func layoutParses(layout string, verb byte) bool {
	for i := 0; i < len(layout); i++ {
		switch layout[i] {
		case '\\':
			i++
		case '%':
			i++
			if i >= len(layout) {
				return false
			}
			switch layout[i] {
			case '%', '{', '}':
				continue
			case verb:
				return true
			}
			if i+1 < len(layout) && layout[i+1] == '{' {
				// A brace after a percent is a literal, as kgo reads it.
				depth := 0
				for i++; i < len(layout); i++ {
					switch layout[i] {
					case '{':
						if layout[i-1] != '%' {
							depth++
						}
					case '}':
						if layout[i-1] != '%' {
							depth--
						}
					}
					if depth == 0 {
						break
					}
				}
			}
		}
	}
	return false
}
