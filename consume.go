package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cobra"
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
  \n    newline
  \r    carriage return
  \t    tab
  \xXX  any ASCII character (input must be hex)

Example:
  -f 'Topic %t [%p] at offset %o: key %k: %s\n'
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
					for i := 0; i < len(topicParts); i++ {
						if re.MatchString(topicParts[i].Topic) {
							keep = append(keep, topicParts[i])
							topicParts[i] = topicParts[len(topicParts)-1]
							topicParts = topicParts[:len(topicParts)-1]
							i--
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

			var wg sync.WaitGroup

			for _, topicPart := range topicParts {
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

					partConsumer, err := client().ConsumePartition(topicPart.Topic, part, koffset)
					maybeDie(err, "unable to consume topic %q partition %d: %v", topicPart.Topic, part, err)

					wg.Add(1)
					go func() {
						defer wg.Done()
						co.consume(partConsumer)
					}()
				}
			}

			wg.Wait()

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
	mu  sync.Mutex
	num int
	max int

	format func(*kgo.Record)
}

func (co *consumeOutput) buildFormatFn(format string) {
	out := make([]byte, 0, len(format))
	var argFns []func(*kgo.Record) interface{}
	for len(format) > 0 {
		b := format[0]
		format = format[1:]

		switch b {
		default:
			out = append(out, b)

		case '\\':
			if len(format) == 0 {
				die("invalid slash escape at end of format string")
			}
			switch format[0] {
			case 't':
				out = append(out, '\t')
			case 'n':
				out = append(out, '\n')
			case 'r':
				out = append(out, '\r')
			case 'x':
				if len(format) < 3 { // on x, need two more
					die("invalid non-terminated hex escape sequence at end of format string")
				}
				hex := format[1:3]
				n, err := strconv.ParseInt(hex, 16, 8)
				maybeDie(err, "unable to parse hex escape sequence %q: %v", hex, err)
				out = append(out, byte(n))
				format = format[2:] // two here, one below
			default:
				die("unknown slash escape sequence %q", format[:1])
			}
			format = format[1:]

		case '%':
			if len(format) == 0 {
				die("invalid percent escape sequence at end of format string")
			}

			out = append(out, '%')
			switch format[0] {
			case 's', 'v':
				argFns = append(argFns, func(r *kgo.Record) interface{} { return r.Value })
				out = append(out, 's')

			case 'S', 'V':
				argFns = append(argFns, func(r *kgo.Record) interface{} { return len(r.Value) })
				out = append(out, 'd')

			case 'R':
				argFns = append(argFns, func(r *kgo.Record) interface{} {
					buf := make([]byte, 8)
					binary.BigEndian.PutUint64(buf, uint64(len(r.Value)))
					return buf
				})
				out = append(out, 's')

			case 'k':
				argFns = append(argFns, func(r *kgo.Record) interface{} { return r.Key })
				out = append(out, 's')

			case 'K':
				argFns = append(argFns, func(r *kgo.Record) interface{} { return len(r.Key) })
				out = append(out, 'd')

			case 't':
				argFns = append(argFns, func(r *kgo.Record) interface{} { return r.Topic })
				out = append(out, 's')

			case 'p':
				argFns = append(argFns, func(r *kgo.Record) interface{} { return r.Partition })
				out = append(out, 'd')

			case 'o':
				argFns = append(argFns, func(r *kgo.Record) interface{} { return r.Offset })
				out = append(out, 'd')

			case 'T':
				argFns = append(argFns, func(r *kgo.Record) interface{} { return r.Timestamp.UnixNano() })
				out = append(out, 'd')

			default:
				die("unknown percent escape sequence %q", format[:1])
			}

			format = format[1:]
		}
	}

	format = string(out)
	args := make([]interface{}, 0, len(argFns))
	co.format = func(r *kgo.Record) {
		args = args[:0]
		for _, argFn := range argFns {
			args = append(args, argFn(r))
		}
		fmt.Printf(format, args...)
	}
}

func (co *consumeOutput) consume(pc *kgo.PartitionConsumer) {
	errs := pc.Errors()
	recs := pc.Records()

	for {
		select {
		case _, ok := <-errs:
			if !ok {
				errs = nil
			}

		case rs, ok := <-recs:
			if !ok {
				recs = nil
			}

			co.mu.Lock()
			for _, r := range rs {
				co.num++

				co.format(r)

				if co.num == co.max {
					os.Exit(0)
				}
			}
			co.mu.Unlock()
		}

		if errs == nil && recs == nil {
			return
		}
	}
}
