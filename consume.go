package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/go-strftime"
	"github.com/twmb/kgo"
)

func init() {
	root.AddCommand(consumeCmd())
}

func consumeCmd() *cobra.Command {
	var regex bool
	var partitions []int32
	var offset string
	var num int
	var format string

	cmd := &cobra.Command{
		Use:   "consume TOPICS...",
		Short: "Consume topic records",
		Long: `Consume topic records and print them.

This function consumes Kafka topics and prints the records with a configurable
format. The output format takes similar arguments as kafkacat, with the default
being to newline delimit record values.

The input topics can be regular expressions with the --regex (-r) flag.

Format options:
  %s    record value
  %S    length of a record
  %v    alias for %s (record value)
  %V    alias for %S (length of a record)
  %R    length of a record (8 byte big endian)
  %k    record key
  %K    length of a record key
  %T    record timestamp (milliseconds since epoch).
  %t    record topic
  %p    record partition
  %o    record offset
  %%    percent sign
  \n    newline
  \r    carriage return
  \t    tab
  \xXX  any ASCII character (input must be hex)

%T supports enhanced time formatting through opening inside {}.
The one current enhanced option is strftime.

To use strftime formatting, open with "%T{strftime" and close with "}".
After "%T{strftime", you can use any delimiter to open the strftime
format and subsequently close it; the delimiter can be repeated.
If your delimiter is {, [, (, the closing delimiter is ), ], or }.

For example

  %T{strftime[[[%F]]]}

will output the timestamp with strftime's %F option.

Putting it all together:
  -f 'Topic %t [%p] at offset %o (%T{strftime[%F %T]}): key %k: %s\n'
`,
		ValidArgs: []string{
			"--partitions",
			"--offset",
			"--num",
			"--format",
			"--regex",
		},
		Args: cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			var topicParts []kgo.TopicPartitions
			var err error
			if regex {
				for _, arg := range args { // quickly validate all input
					if _, err := regexp.Compile(arg); err != nil {
						die("unable to compile regexp %q: %v", arg, err)
					}
				}
				topicParts, err = client().TopicPartitions()
			} else {
				topicParts, err = client().TopicPartitions(args...)
			}
			maybeDie(err, "unable to request topic partitions: %v", err)

			if regex {
				keep := make([]kgo.TopicPartitions, 0, len(topicParts))
				for _, arg := range args {
					if len(topicParts) == 0 {
						break
					}

					re := regexp.MustCompile(arg)
					for _, topicPart := range topicParts {
						if re.MatchString(topicPart.Topic) {
							keep = append(keep, topicPart)
						}
					}
				}

				topicParts = keep
			}

			if len(topicParts) == 0 {
				die("no matching topic partitions to consume")
			}

			var koffset kgo.Offset
			switch {
			case offset == "start":
				koffset = kgo.ConsumeStartOffset()
			case offset == "end":
				koffset = kgo.ConsumeEndOffset()
			case strings.HasPrefix(offset, "end-"):
				v, err := strconv.Atoi(offset[4:])
				maybeDie(err, "unable to parse relative offset number in %q: %v", offset, err)
				koffset = kgo.ConsumeEndRelativeOffset(v)
			case strings.HasPrefix(offset, "start+"):
				v, err := strconv.Atoi(offset[6:])
				maybeDie(err, "unable to parse relative offset number in %q: %v", offset, err)
				koffset = kgo.ConsumeStartRelativeOffset(v)
			default:
				v, err := strconv.Atoi(offset)
				maybeDie(err, "unable to parse exact offset number in %q: %v", offset, err)
				koffset = kgo.ConsumeExactOffset(int64(v))
			}

			co := &consumeOutput{
				max: num,
			}
			co.buildFormatFn(format)

			offsets := make(map[string]map[int32]kgo.Offset)
			for _, topicPart := range topicParts {
				topicOffsets := offsets[topicPart.Topic]
				if topicOffsets == nil {
					topicOffsets = make(map[int32]kgo.Offset)
					offsets[topicPart.Topic] = topicOffsets
				}
				for _, part := range topicPart.Partitions {
					if len(partitions) > 0 {
						var requested bool
						for _, requestedPart := range partitions {
							if requestedPart == part {
								requested = true
								break
							}
						}
						if !requested {
							continue
						}
					}
					topicOffsets[part] = koffset
				}
			}

			client().AssignPartitions(offsets)
			co.consume()

		},
	}

	cmd.Flags().Int32SliceVarP(&partitions, "partitions", "p", nil, "comma delimited list of specific partitions to consume")
	cmd.Flags().StringVarP(&offset, "offset", "o", "start", "offset to start consuming from (start, end, 47, start+2, end-3)")
	cmd.Flags().IntVarP(&num, "num", "n", 0, "quit after consuming this number of records; 0 is unbounded")
	cmd.Flags().StringVarP(&format, "format", "f", `%s\n`, "output format")
	cmd.Flags().BoolVarP(&regex, "regex", "r", false, "parse topics as regex; consume any topic that matches any expression")
	// TODO: wait millis, size

	return cmd
}

type consumeOutput struct {
	num int
	max int

	format func(*kgo.Record)
}

func (co *consumeOutput) buildFormatFn(format string) {
	var argFns []func([]byte, *kgo.Record) []byte
	var pieces [][]byte
	var piece []byte
	for len(format) > 0 {
		b := format[0]
		format = format[1:]

		switch b {
		default:
			piece = append(piece, b)

		case '\\':
			if len(format) == 0 {
				die("invalid slash escape at end of format string")
			}
			switch format[0] {
			case 't':
				piece = append(piece, '\t')
			case 'n':
				piece = append(piece, '\n')
			case 'r':
				piece = append(piece, '\r')
			case 'x':
				if len(format) < 3 { // on x, need two more
					die("invalid non-terminated hex escape sequence at end of format string")
				}
				hex := format[1:3]
				n, err := strconv.ParseInt(hex, 16, 8)
				maybeDie(err, "unable to parse hex escape sequence %q: %v", hex, err)
				piece = append(piece, byte(n))
				format = format[2:] // two here, one below
			default:
				die("unknown slash escape sequence %q", format[:1])
			}
			format = format[1:]

		case '%':
			if len(format) == 0 {
				die("invalid percent escape sequence at end of format string")
			}
			if format[0] == '%' {
				piece = append(piece, '%')
				format = format[1:]
				continue
			}

			// Always cut the piece, even if it is empty. We alternate piece, argFn.
			pieces = append(pieces, piece)
			piece = []byte{}

			next := format[0]
			format = format[1:]
			switch next {
			case 's', 'v':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return append(out, r.Value...) })

			case 'S', 'V':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strconv.AppendInt(out, int64(len(r.Value)), 10) })

			case 'R':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte {
					out = append(out, 0, 0, 0, 0, 0, 0, 0, 0)
					binary.BigEndian.PutUint64(out[len(out)-8:], uint64(len(r.Value)))
					return out
				})

			case 'k':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return append(out, r.Key...) })

			case 'K':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strconv.AppendInt(out, int64(len(r.Key)), 10) })

			case 't':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return append(out, r.Topic...) })

			case 'p':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strconv.AppendInt(out, int64(r.Partition), 10) })

			case 'o':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strconv.AppendInt(out, r.Offset, 10) })

			case 'T':
				if len(format) > 0 && format[0] == '{' {
					format = format[1:]
					switch {
					case strings.HasPrefix(format, "strftime"):
						tfmt, rem, err := nomOpenClose(format[len("strftime"):])
						if err != nil {
							die("strftime parse err: %v", err)
						}
						if len(rem) == 0 || rem[0] != '}' {
							die("%%T{strftime missing closing }")
						}
						format = rem[1:]
						argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strftime.AppendFormat(out, tfmt, r.Timestamp) })
					default:
						die("unknown %%T{ time qualifier name")
					}
				} else {
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strconv.AppendInt(out, r.Timestamp.UnixNano(), 10) })
				}

			default:
				die("unknown percent escape sequence %q", format[:1])
			}
		}
	}

	if len(piece) > 0 {
		pieces = append(pieces, piece)
		argFns = append(argFns, func(out []byte, _ *kgo.Record) []byte { return out })
	}
	var out []byte
	co.format = func(r *kgo.Record) {
		out = out[:0]
		for i, piece := range pieces {
			out = append(out, piece...)
			out = argFns[i](out, r)
		}
		os.Stdout.Write(out)
	}
}

func nomOpenClose(src string) (string, string, error) {
	if len(src) == 0 {
		return "", "", errors.New("empty format")
	}
	delim := src[0]
	openers := 1
	for openers < len(src) && src[openers] == delim {
		openers++
	}
	switch delim {
	case '{':
		delim = '}'
	case '[':
		delim = ']'
	case '(':
		delim = ')'
	}
	src = src[openers:]
	end := strings.Repeat(string([]byte{delim}), openers)
	idx := strings.Index(src, end)
	if idx < 0 {
		return "", "", fmt.Errorf("missing end delim %q", end)
	}
	middle := src[:idx]
	return middle, src[idx+len(end):], nil
}

func (co *consumeOutput) consume() {
	for {
		fetches := client().PollConsumer(context.Background())

		// TODO Errors(), print to stderr
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			co.num++
			co.format(record)

			if co.num == co.max {
				os.Exit(0)
			}
		}
	}
}
