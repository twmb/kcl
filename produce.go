package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/spf13/cobra"
	"github.com/twmb/kgo"
)

func init() {
	root.AddCommand(produceCmd())
}

func produceCmd() *cobra.Command {
	var recDelim string
	var keyDelim string
	var maxBuf int
	var verbose bool

	cmd := &cobra.Command{
		Use:   "produce TOPIC",
		Short: "Produce records to a topic",
		Long: `Produce records to a topic, taking input from stdin or files.

By default, producing consumes newline delimited, unkeyed records from stdin.
The flags allow for switching the delimiter, or using the delimiter for
keys and values, or consuming from files.

If the keyed-record delimiter option is used, the record-only option will be
ignored.

If streaming records, each key or value must be under 64KiB in length. This can
be changed with the --max-read-buf flag.

The input delimiter understands \n, \r, \t, and \xXX (hex) escape sequences.
`,
		ValidArgs: []string{
			"--delim",
			"--keyed-record-delim",
			"--verbose",
			"--max-read-buf",
		},
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			if keyDelim != "" {
				recDelim = keyDelim
			}

			delim := parseDelim(recDelim)

			scanner := bufio.NewScanner(os.Stdin)
			scanner.Buffer(nil, maxBuf)
			scanner.Split(splitDelimFn(delim))

			var wg sync.WaitGroup
			for scanner.Scan() {
				r := &kgo.Record{
					Topic: args[0],
				}
				if keyDelim != "" {
					r.Key = append([]byte(nil), scanner.Bytes()...)
					if !scanner.Scan() {
						die("missing final value delim")
					}
				}
				r.Value = append([]byte(nil), scanner.Bytes()...)

				wg.Add(1)
				client.Produce(r, func(r *kgo.Record, err error) {
					defer wg.Done()
					maybeDie(err, "unable to produce record: %v", err)
					if verbose {
						fmt.Printf("Successful send to topic %s partition %d offset %d\n",
							r.Topic, r.Partition, r.Offset)
					}
				})
			}
			wg.Wait()

			if scanner.Err() != nil {
				die("final scan error: %v", scanner.Err())
			}
		},
	}

	cmd.Flags().StringVarP(&recDelim, "delim", "D", "\n", "record only delimiter")
	cmd.Flags().StringVarP(&keyDelim, "keyed-record-delim", "K", "", "key and record delimiter")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose information of the producing of records")
	cmd.Flags().IntVar(&maxBuf, "max-read-buf", bufio.MaxScanTokenSize, "maximum input to buffer before a delimiter is required")

	return cmd
}

func parseDelim(in string) []byte {
	out := make([]byte, 0, len(in))
	for len(in) > 0 {
		b := in[0]
		in = in[1:]
		switch b {
		default:
			out = append(out, b)
		case '\\':
			if len(in) == 0 {
				die("invalid slash escape at end of delim string")
			}
			switch in[0] {
			case 't':
				out = append(out, '\t')
			case 'n':
				out = append(out, '\n')
			case 'r':
				out = append(out, '\r')
			case 'x':
				if len(in) < 3 { // on x, need two more
					die("invalid non-terminated hex escape sequence at end of delim string")
				}
				hex := in[1:3]
				n, err := strconv.ParseInt(hex, 16, 8)
				maybeDie(err, "unable to parse hex escape sequence %q: %v", hex, err)
				out = append(out, byte(n))
				in = in[2:] // two here, one below
			default:
				die("unknown slash escape sequence %q", in[:1])
			}
			in = in[1:]
		}
	}
}

func splitDelimFn(delim []byte) bufio.ScanFunc {
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.Index(data, delim); i >= 0 {
			return i + 1, data[0:i], nil
		}
		if atEOF {
			return len(data), data, nil // non terminated line
		}
		return 0, nil, nil
	}
}
