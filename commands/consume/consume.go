// Package consume contains a cobra command to consume records.
package consume

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/twmb/go-strftime"
	"github.com/twmb/kgo"
	"github.com/twmb/kgo/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

type consumption struct {
	cl *client.Client

	group      string
	regex      bool
	partitions []int32
	offset     string
	num        int
	format     string
}

// Command returns a consume command.
func Command(cl *client.Client) *cobra.Command {
	return (&consumption{cl: cl}).command()
}

func (c *consumption) run(topics []string) {
	var consumeOpts []kgo.ConsumeOpt
	var groupOpts []kgo.GroupOpt
	offset := c.parseOffset()
	if len(c.partitions) == 0 {
		consumeOpts = append(consumeOpts, kgo.ConsumeTopics(offset, topics...))
		groupOpts = append(groupOpts, kgo.GroupTopics(topics...))
	} else {
		if len(c.group) != 0 {
			out.Die("incompatible flag assignment: group consuming cannot be used with direct partition consuming")
		}
		offsets := make(map[string]map[int32]kgo.Offset)
		for _, topic := range topics {
			partOffsets := make(map[int32]kgo.Offset, len(c.partitions))
			for _, partition := range c.partitions {
				partOffsets[partition] = offset
			}
			offsets[topic] = partOffsets
		}
		consumeOpts = append(consumeOpts, kgo.ConsumePartitions(offsets))
	}
	if c.regex {
		consumeOpts = append(consumeOpts, kgo.ConsumeTopicsRegex())
		groupOpts = append(groupOpts, kgo.GroupTopicsRegex())
	}

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	cl := c.cl.Client()
	if len(c.group) > 0 {
		cl.AssignGroup(c.group, groupOpts...)
	} else {
		cl.AssignPartitions(consumeOpts...)
	}
	co := &consumeOutput{
		cl:   cl,
		max:  c.num,
		done: make(chan struct{}),
	}
	co.ctx, co.cancel = context.WithCancel(context.Background())
	co.buildFormatFn(c.format)

	go co.consume()

	<-sigs
	done := make(chan struct{})
	go func() {
		defer close(done)
		atomic.StoreUint32(&co.quit, 1)
		co.cancel()
		<-co.done
		commitDone := make(chan struct{})
		wait := func(_ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, _ error) {
			close(commitDone)
		}
		cl.Commit(context.Background(), cl.Uncommitted(), wait)
		<-commitDone
		cl.Close() // leaves group
	}()
	select {
	case <-sigs:
	case <-done:
	}
}

func (c *consumption) parseOffset() kgo.Offset {
	var koffset kgo.Offset
	switch {
	case c.offset == "start":
		koffset = kgo.ConsumeStartOffset()
	case c.offset == "end":
		koffset = kgo.ConsumeEndOffset()
	case strings.HasPrefix(c.offset, "end-"):
		v, err := strconv.Atoi(c.offset[4:])
		out.MaybeDie(err, "unable to parse relative end offset number in %q: %v", c.offset, err)
		koffset = kgo.ConsumeEndRelativeOffset(v)
	case strings.HasPrefix(c.offset, "start+"):
		v, err := strconv.Atoi(c.offset[6:])
		out.MaybeDie(err, "unable to parse relative start offset number in %q: %v", c.offset, err)
		koffset = kgo.ConsumeStartRelativeOffset(v)
	default:
		v, err := strconv.Atoi(c.offset)
		out.MaybeDie(err, "unable to parse exact offset number in %q: %v", c.offset, err)
		koffset = kgo.ConsumeExactOffset(int64(v))
	}
	return koffset
}

type consumeOutput struct {
	cl *kgo.Client

	num int
	max int

	ctx    context.Context
	cancel func()
	quit   uint32
	done   chan struct{}

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
				out.Die("invalid slash escape at end of format string")
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
					out.Die("invalid non-terminated hex escape sequence at end of format string")
				}
				hex := format[1:3]
				n, err := strconv.ParseInt(hex, 16, 8)
				out.MaybeDie(err, "unable to parse hex escape sequence %q: %v", hex, err)
				piece = append(piece, byte(n))
				format = format[2:] // two here, one below
			default:
				out.Die("unknown slash escape sequence %q", format[:1])
			}
			format = format[1:]

		case '%':
			if len(format) == 0 {
				out.Die("invalid percent escape sequence at end of format string")
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
							out.Die("strftime parse err: %v", err)
						}
						if len(rem) == 0 || rem[0] != '}' {
							out.Die("%%T{strftime missing closing }")
						}
						format = rem[1:]
						argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strftime.AppendFormat(out, tfmt, r.Timestamp) })

					case strings.HasPrefix(format, "go"):
						tfmt, rem, err := nomOpenClose(format[len("go"):])
						if err != nil {
							out.Die("go parse err: %v", err)
						}
						if len(rem) == 0 || rem[0] != '}' {
							out.Die("%%T{go missing closing }")
						}
						format = rem[1:]
						argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return r.Timestamp.AppendFormat(out, tfmt) })

					default:
						out.Die("unknown %%T{ time qualifier name")
					}
				} else {
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strconv.AppendInt(out, r.Timestamp.UnixNano(), 10) })
				}

			default:
				out.Die("unknown percent escape sequence %q", format[:1])
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

// nomOpenClose extracts a middle section from a string beginning with repeated
// delimiters and returns it as with remaining (past end delimiters) string.
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
	end := strings.Repeat(string(delim), openers)
	idx := strings.Index(src, end)
	if idx < 0 {
		return "", "", fmt.Errorf("missing end delim %q", end)
	}
	middle := src[:idx]
	return middle, src[idx+len(end):], nil
}

func (co *consumeOutput) consume() {
	defer close(co.done)
	for atomic.LoadUint32(&co.quit) == 0 {
		fetches := co.cl.PollFetches(co.ctx)
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
