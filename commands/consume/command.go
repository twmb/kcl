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
	cmd.Flags().StringVarP(&c.group, "group", "g", "", "group to assign")
	cmd.Flags().StringVarP(&c.groupAlg, "balancer", "b", "cooperative-sticky", "group balancer to use if group consuming (range, roundrobin, sticky, cooperative-sticky)")
	cmd.Flags().StringVarP(&c.instanceID, "instance-id", "i", "", "group instance ID to use for consuming; empty means none (implies static membership, Kafka 2.3.0+)")
	cmd.Flags().Int32SliceVarP(&c.partitions, "partitions", "p", nil, "comma delimited list of specific partitions to consume")
	cmd.Flags().StringVarP(&c.offset, "offset", "o", "start", "offset to start consuming from (start, end, 47, start+2, end-3)")
	cmd.Flags().IntVarP(&c.num, "num", "n", 0, "quit after consuming this number of records; 0 is unbounded")
	cmd.Flags().IntVar(&c.numPerPartition, "num-per-partition", 0, "stop printing individual partitions after this many records; 0 is unbounded")
	cmd.Flags().StringVarP(&c.format, "format", "f", `%v\n`, "output format")
	cmd.Flags().BoolVarP(&c.regex, "regex", "r", false, "parse topics as regex; consume any topic that matches any expression")
	cmd.Flags().StringVarP(&c.escapeChar, "escape-char", "c", "%", "character to use for beginning a record field escape (accepts any utf8)")
	cmd.Flags().Int32Var(&c.fetchMaxBytes, "fetch-max-bytes", 1<<20, "maximum amount of bytes per fetch request per broker")
	cmd.Flags().DurationVar(&c.fetchMaxWait, "fetch-max-wait", 5*time.Second, "maximum amount of time to wait when fetching from a broker before the broker replies")
	cmd.Flags().StringVar(&c.rack, "rack", "", "the rack to use for fetch requests; setting this opts in to nearest replica fetching (Kafka 2.2.0+)")
	cmd.Flags().BoolVar(&c.readUncommitted, "read-uncommitted", false, "opt in to reading uncommitted offsets")
	cmd.Flags().Int32Var(&c.untilOffset, "until-offset", -1, "consume each partition up to the latest stable offset, decremented by this value.")
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

  %x    record producer id
  %y    record producer epoch
  %[    partition log start offset
  %|    partition last stable offset
  %]    partition high watermark

  %i    format iteration number (starts at 1)
  %%    percent sign
  %{    left brace
  \n    newline
  \r    carriage return
  \t    tab
  \\    slash
  \xXX  any ASCII character (input must be hex)

Headers have their own internal format (the same as keys and values above):
  %v    header value
  %V    header value length
  %k    header key
  %K    header key length
Other signifiers in the header section are ignored.

All strings or byte arrays support printing as base64 or hex encoded values
by including {base64} or {hex} after the escape format, e.g., %v{hex}.


NUMBER FORMATTING

By default, numbers are printed as just their textual representation.

However, all numbers support big/little endian compact encoding in braces
following the number:
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
  ascii    textual representation (the default)

For example,
  %T{big8}
will print the length of a topic as an eight byte big endian.

Number formatting does not check for overflow.


TIME FORMATTING

%d supports enhanced time formatting inside braces.

To use strftime formatting, open with "%d{strftime" and close with "}".
After "%d{strftime", you can use any delimiter to open the strftime
format and subsequently close it; the delimiter can be repeated.
If your delimiter is {, [, (, the closing delimiter is ), ], or }.

For example,
  %d{strftime[[%F]]}
will output the timestamp with strftime's %F option.

To use Go time formatting, open with "%d{go" and close with "}".
The Go time formatting follows the same delimiting rules as strftime.

For example,
  %d{go#06-01-02 15:04:05.999#}
will output the timestamp as YY-MM-DD HH:MM:SS.ms.


EXAMPLES

A basic example:
  -f 'Topic %t [%p] at offset %o @%d{strftime[%F %T]}: key %k: %v\n'

To mirror a topic, you can use the following format for consuming from one
topic and pipe the results to producing with this same format:
  -f '%K{b4}%k%V{b4}%v%H{b4}%h{%K{b4}%k%V{b4}%v}'


REMARKS

Note that this command allows you to consume the Kafka special internal topics
__consumer_offsets and __transaction_state. To do so, either of these topics
must be the only topic specified.

For __consumer_offsets, to dump information about a specific group, use the -G
flag. Doing so will also hide transaction markers. For __transaction_state, you
can use -G to dump information about a specific transactional ID.

Combined with producing, these two commands allow you to easily mirror a topic.

If you do not like %, you can switch the escape character with a flag.
Unfortunately, with exact sizing, the format string is unavoidably noisy.

The --until-offset flag allows kcl to terminate consumption after reaching some
offset, in relation to the latest stable offsets. For example, if the LSO of
the topic partition is 25 and --until-offset=2 the topic partition will be
consumed through offset 23.
`
