package consume

import "github.com/spf13/cobra"

func (c *consumption) command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "consume TOPICS...",
		Short: "Consume topic records",
		Long:  help,
		ValidArgs: []string{
			"--partitions",
			"--offset",
			"--num",
			"--format",
			"--regex",
		},
		Args: cobra.MinimumNArgs(1), // topic
		Run: func(_ *cobra.Command, args []string) {
			c.run(args)
		},
	}
	cmd.Flags().Int32SliceVarP(&c.partitions, "partitions", "p", nil, "comma delimited list of specific partitions to consume")
	cmd.Flags().StringVarP(&c.offset, "offset", "o", "start", "offset to start consuming from (start, end, 47, start+2, end-3)")
	cmd.Flags().IntVarP(&c.num, "num", "n", 0, "quit after consuming this number of records; 0 is unbounded")
	cmd.Flags().StringVarP(&c.format, "format", "f", `%s\n`, "output format")
	cmd.Flags().BoolVarP(&c.regex, "regex", "r", false, "parse topics as regex; consume any topic that matches any expression")
	// TODO: wait millis, size
	return cmd
}

const help = `Consume topic records and print them.

This function consumes Kafka topics and prints the records with a configurable
format. The output format takes similar arguments as kafkacat, with the default
being to newline delimit record values.

The input topics can be regular expressions with the --regex (-r) flag.

Format options:
  %s    record value
  %S    length of a record
  %v    alias for %s
  %V    alias for %S
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

To use strftime formatting, open with "%T{strftime" and close with "}".
After "%T{strftime", you can use any delimiter to open the strftime
format and subsequently close it; the delimiter can be repeated.
If your delimiter is {, [, (, the closing delimiter is ), ], or }.

For example,
  %T{strftime[[%F]]}
will output the timestamp with strftime's %F option.

To use Go time formatting, open with "T{go" and close with "}".
The Go time formatting follows the same delimiting rules as strftime.

For example,
  %T{go#15:04:05.99#}
will output the timestamp as HH:MM:SS.ms.

Putting it all together:
  -f 'Topic %t [%p] at offset %o @%T{strftime[%F %T]}: key %k: %s\n'
`
