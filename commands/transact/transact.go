// Package transact provides transactions.
package transact

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"sync/atomic"
	"syscall"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/format"
	"github.com/twmb/kcl/out"
)

const help = `Consume records, exec a program to modify them, and then produce.

This command is wraps consuming and producing in transactions. Since this is
mostly a combination of consuming and producing, docs for consuming and
producing are elided. See the docs under the consume and produce commands.

Transactions require a globally unique transactional ID. If the transactional
ID is shared with anything else, either kcl will be fenced or that other thing
will be fenced.

kcl executes the ETL_COMMAND for every batch of records received, formatting
the records as requested per the -w flag to the commands STDIN, and reading
back modified records from the commands STDOUT as per the -r flag.

Once all records are read, kcl begins a transaction, writes all records to
Kafka, and finishes the transaction.
`

func Command(cl *client.Client) *cobra.Command {
	var (
		escapeChar string // common

		// Consuming opts
		topics      []string
		regex       bool
		group       string
		groupAlg    string
		instanceID  string
		writeFormat string
		rack        string

		rwFormat string

		// Producing opts
		readFormat  string
		maxBuf      int
		destTopic   string
		compression string
		txnID       string
		verbose     bool
	)

	cmd := &cobra.Command{
		Use:   "transact [FLAGS] ETL_COMMAND...",
		Short: "Transactionally consume, exec a program, and write back to Kafka; requires Kafka 0.11.0+.",
		Long:  help,
		Args:  cobra.MinimumNArgs(1), // exec
		Run: func(_ *cobra.Command, args []string) {
			if len(escapeChar) == 0 {
				out.Die("invalid empty escape character")
			}
			escape, size := utf8.DecodeRuneInString(escapeChar)
			if size != len(escapeChar) {
				out.Die("invalid multi character escape character")
			}
			if len(txnID) == 0 {
				out.Die("invalid empty transactional id")
			}

			if rwFormat != "" {
				readFormat = rwFormat
				writeFormat = rwFormat
			}

			///////////////
			// consuming //
			///////////////
			w, err := format.ParseWriteFormat(writeFormat, escape)
			out.MaybeDie(err, "unable to parse write format: %v", err)

			// create group opts:
			// topics,
			// regex,
			// balancer,
			// instance ID
			var groupOpts []kgo.GroupOpt
			groupOpts = append(groupOpts, kgo.GroupTopics(topics...))
			if regex {
				groupOpts = append(groupOpts, kgo.GroupTopicsRegex())
			}
			var balancer kgo.GroupBalancer
			switch groupAlg {
			case "range":
				balancer = kgo.RangeBalancer()
			case "roundrobin":
				balancer = kgo.RoundRobinBalancer()
			case "sticky":
				balancer = kgo.StickyBalancer()
			case "cooperative-sticky":
				balancer = kgo.CooperativeStickyBalancer()
			default:
				out.Die("unrecognized group balancer %q", groupAlg)
			}
			groupOpts = append(groupOpts, kgo.Balancers(balancer))
			if instanceID != "" {
				groupOpts = append(groupOpts, kgo.InstanceID(instanceID))
			}

			cl.AddOpt(kgo.FetchIsolationLevel(kgo.ReadCommitted())) // we will be reading committed
			cl.AddOpt(kgo.Rack(rack))

			///////////////
			// producing //
			///////////////
			r, err := format.NewReader(readFormat, escape, maxBuf, nil)
			out.MaybeDie(err, "unable to parse read format: %v", err)
			if r.ParsesTopic() && len(destTopic) != 0 {
				out.Die("cannot produce to a destination topic; the read format specifies that it parses a topic")
			}
			if !r.ParsesTopic() && len(destTopic) == 0 {
				out.Die("destiniation topic is missing and the read format does not specify that it parses a topic")
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
			cl.AddOpt(kgo.TransactionalID(txnID))
			cl.AddOpt(kgo.StopOnDataLoss())
			cl.AddOpt(kgo.BatchCompression(codec))

			sigs := make(chan os.Signal, 2)
			signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

			///////////
			// begin //
			///////////

			// If we made it this far, our options are valid:
			// assign our group and begin execing.
			txnSess := cl.Client().AssignGroupTransactSession(group, groupOpts...)

			quitCtx, cancel := context.WithCancel(context.Background())
			go transact(quitCtx, cl.Client(), txnSess, w, r, destTopic, verbose, args...)

			<-sigs
			cancel()
			<-sigs
		},
	}

	cmd.Flags().StringVarP(&escapeChar, "escape-char", "c", "%", "character to use for beginning a record field escape (accepts any utf8)")

	cmd.Flags().StringArrayVarP(&topics, "topic", "t", nil, "topic to consume (repeatable)")
	cmd.Flags().BoolVar(&regex, "regex", false, "parse topics as regex; consume any topic that matches any expression")
	cmd.Flags().StringVarP(&group, "group", "g", "", "group to assign")
	cmd.Flags().StringVarP(&groupAlg, "balancer", "b", "cooperative-sticky", "group balancer to use if group consuming (range, roundrobin, sticky, cooperative-sticky)")
	cmd.Flags().StringVarP(&instanceID, "instance-id", "i", "", "group instance ID to use for consuming; empty means none (implies static membership; Kafka 2.5.0+)")
	cmd.Flags().StringVar(&rack, "rack", "", "the rack to use for fetch requests; setting this opts in to nearest replica fetching (Kafka 2.2.0+)")

	cmd.Flags().StringVarP(&writeFormat, "write-format", "w", "%t\t%k\t%v\n", "format to write to the transform program")
	cmd.Flags().StringVarP(&readFormat, "read-format", "r", "%t\t%k\t%v\n", "format to read from the transform program")
	cmd.Flags().StringVar(&rwFormat, "rw", "", "if non-empty, the format to use for both reading and writing (overrides w and r)")

	cmd.Flags().IntVar(&maxBuf, "max-delim-buf", bufio.MaxScanTokenSize, "maximum input to buffer before a delimiter is required, if using delimiters")
	cmd.Flags().StringVarP(&compression, "compression", "z", "snappy", "compression to use for producing batches (none, gzip, snappy, lz4, zstd)")
	cmd.Flags().StringVarP(&destTopic, "destination-topic", "d", "", "if non-empty, the topic to produce to (read-format must not contain %t)")
	cmd.Flags().StringVarP(&txnID, "txn-id", "x", "", "transactional ID")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose printing of transactions")

	return cmd
}

func transact(
	quitCtx context.Context,
	cl *kgo.Client,
	txnSess *kgo.GroupTransactSession,
	w func([]byte, *kgo.Record, *kgo.FetchPartition) []byte,
	r *format.Reader,
	destTopic string,
	verbose bool,
	args ...string,
) {
	defer cl.Close()

	var buf []byte

	for {

		fetches := cl.PollFetches(quitCtx)
		select {
		case <-quitCtx.Done():
			out.Die("Quitting.")
		default:
		}

		if verbose {
			fmt.Println("Fetched, executing program and writing records...")
		}

		buf = buf[:0]
		for _, fetch := range fetches {
			for _, topic := range fetch.Topics {
				for _, partition := range topic.Partitions {
					out.MaybeDie(partition.Err, "fetch partition error: %v", partition.Err)
					for _, record := range partition.Records {
						buf = w(buf, record, &partition)
					}
				}
			}
		}

		if verbose {
			fmt.Printf("Buffered records (%d bytes), executing program and writing to it...\n", len(buf))
		}

		cmd := exec.Command(args[0], args[1:]...)
		stdin, err := cmd.StdinPipe()
		out.MaybeDie(err, "unable to create stdin pipe: %v", err)
		stdout, err := cmd.StdoutPipe()
		out.MaybeDie(err, "unable to create stdout pipe: %v", err)

		err = cmd.Start()
		out.MaybeDie(err, "unable to start transform program: %v", err)

		_, err = stdin.Write(buf)
		out.MaybeDie(err, "unable to write to transform program: %v", err)
		stdin.Close()
		out.MaybeDie(err, "unable to close write pipe to transform program: %v", err)

		if verbose {
			fmt.Println("Wrote records, reading new records back...")
		}

		var received []*kgo.Record
		r.SetReader(stdout)
		for {
			receive, err := r.Next()
			if err != nil {
				if err != io.EOF {
					out.Die("invalid record received: %v", err)
				}
				break
			}
			if !r.ParsesTopic() {
				receive.Topic = destTopic
			}
			received = append(received, receive)
		}
		err = cmd.Wait()
		out.MaybeDie(err, "error on waiting for command to finish: %v", err)

		if verbose {
			fmt.Printf("Finished receiving %d records, beginning a transaction to produce them...\n", len(received))
		}

		if err = txnSess.Begin(); err != nil {
			out.MaybeDie(err, "error beginning transaction: %v", err)
		}

		var stopProduction uint32
		var firstProduceErr error
		ctx, cancel := context.WithCancel(context.Background())
		for _, record := range received {
			cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
				if err != nil {
					if atomic.SwapUint32(&stopProduction, 1) == 0 {
						firstProduceErr = err
						cancel()
					}
				}
			})
		}

		if verbose {
			if firstProduceErr == nil {
				fmt.Println("Production complete, flushing and potentially committing...")
			} else {
				fmt.Fprintf(os.Stderr, "Production of records failed, first produce error: %v; aborting transaction...\n", firstProduceErr)
			}
		}

		committed, err := txnSess.End(context.Background(), kgo.TransactionEndTry(firstProduceErr == nil))
		out.MaybeDie(err, "unable to end transaction: %v", err)

		if !committed {
			fmt.Fprintln(os.Stderr, "Transaction was aborted.")
		} else if verbose {
			fmt.Println("Transaction was committed.")
		}
	}

}
