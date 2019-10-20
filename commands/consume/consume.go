// Package consume contains a cobra command to consume records.
package consume

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/twmb/go-strftime"
	"github.com/twmb/kafka-go/pkg/kgo"
	"github.com/twmb/kafka-go/pkg/kmsg"

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

	start int64 // if exact range
	end   int64 // if exact range
}

// Command returns a consume command.
func Command(cl *client.Client) *cobra.Command {
	return (&consumption{cl: cl}).command()
}

func (c *consumption) run(topics []string) {
	var isConsumerOffsets bool
	for _, topic := range topics {
		isConsumerOffsets = isConsumerOffsets || topic == "__consumer_offsets"
	}
	if isConsumerOffsets && len(topics) != 1 {
		out.Die("__consumer_offsets must be the only topic listed when trying to consume it")
	}

	var directOpts []kgo.DirectConsumeOpt
	var groupOpts []kgo.GroupOpt
	offset := c.parseOffset()
	if len(c.partitions) == 0 {
		directOpts = append(directOpts, kgo.ConsumeTopics(offset, topics...))
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
		directOpts = append(directOpts, kgo.ConsumePartitions(offsets))
	}
	if c.regex {
		directOpts = append(directOpts, kgo.ConsumeTopicsRegex())
		groupOpts = append(groupOpts, kgo.GroupTopicsRegex())
	}

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	cl := c.cl.Client()
	if len(c.group) > 0 && !isConsumerOffsets {
		cl.AssignGroup(c.group, groupOpts...)
	} else {
		cl.AssignPartitions(directOpts...)
	}
	co := &consumeOutput{
		cl:    cl,
		max:   c.num,
		start: c.start,
		end:   c.end,
		group: c.group,
		done:  make(chan struct{}),
	}
	co.ctx, co.cancel = context.WithCancel(context.Background())
	if isConsumerOffsets {
		co.buildConsumerOffsetsFormatFn()
	} else {
		co.buildFormatFn(c.format)
	}

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
	c.end = -1
	var opts []kgo.OffsetOpt
	switch {
	case c.offset == "start":
		opts = append(opts, kgo.AtStart())
	case c.offset == "end":
		opts = append(opts, kgo.AtEnd())
	case strings.HasPrefix(c.offset, "end-"):
		v, err := strconv.Atoi(c.offset[4:])
		out.MaybeDie(err, "unable to parse relative end offset number in %q: %v", c.offset, err)
		opts = append(opts, kgo.AtEnd(), kgo.Relative(int64(-v)))
	case strings.HasPrefix(c.offset, "start+"):
		v, err := strconv.Atoi(c.offset[6:])
		out.MaybeDie(err, "unable to parse relative start offset number in %q: %v", c.offset, err)
		opts = append(opts, kgo.AtStart(), kgo.Relative(int64(v)))
	default:
		match := regexp.MustCompile(`^(\d+)(?:-(\d+))?$`).FindStringSubmatch(c.offset)
		if len(match) == 0 {
			out.Die("unable to parse exact or range offset in %q", c.offset)
		}
		at, _ := strconv.ParseInt(match[1], 10, 64)
		c.start = at
		if match[2] != "" {
			c.end, _ = strconv.ParseInt(match[2], 10, 64)
		}
		opts = append(opts, kgo.At(at))
	}
	return kgo.NewOffset(opts...)
}

type consumeOutput struct {
	cl *kgo.Client

	num int
	max int

	start int64 // if exact range
	end   int64 // if exact range

	group string // for filtering __consumer_offsets

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
			openBrace := len(format) > 2 && format[1] == '{'
			var handledBrace bool

			// Always cut the piece, even if it is empty. We alternate piece, argFn.
			pieces = append(pieces, piece)
			piece = []byte{}

			next := format[0]
			format = format[1:]
			switch next {
			case 's', 'v':
				if handledBrace = openBrace; handledBrace {
					format = format[1:]
					switch {
					case strings.HasPrefix(format, "base64}"):
						argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return appendBase64(out, r.Value) })
						format = format[len("base64}"):]
					default:
						out.Die("unknown %%v{ escape")
					}
				} else {
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return append(out, r.Value...) })
				}

			case 'S', 'V':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strconv.AppendInt(out, int64(len(r.Value)), 10) })

			case 'R':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte {
					out = append(out, 0, 0, 0, 0, 0, 0, 0, 0)
					binary.BigEndian.PutUint64(out[len(out)-8:], uint64(len(r.Value)))
					return out
				})

			case 'k':
				if handledBrace = openBrace; handledBrace {
					format = format[1:]
					switch {
					case strings.HasPrefix(format, "base64}"):
						argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return appendBase64(out, r.Key) })
						format = format[len("base64}"):]
					default:
						out.Die("unknown %%k{ escape")
					}
				} else {
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return append(out, r.Key...) })
				}

			case 'K':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strconv.AppendInt(out, int64(len(r.Key)), 10) })

			case 't':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return append(out, r.Topic...) })

			case 'p':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strconv.AppendInt(out, int64(r.Partition), 10) })

			case 'o':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strconv.AppendInt(out, r.Offset, 10) })

			case 'e':
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strconv.AppendInt(out, int64(r.LeaderEpoch), 10) })

			case 'T':
				if handledBrace = openBrace; handledBrace {
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

			if openBrace && !handledBrace {
				out.Die("unhandled, unknown open brace %q", format[:2])
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

func appendBase64(dst, src []byte) []byte {
	fin := append(dst, make([]byte, base64.RawStdEncoding.EncodedLen(len(src)))...)
	base64.RawStdEncoding.Encode(fin[len(dst):], src)
	return fin
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
			// This record offset could be before the requested start
			// following an out of range reset.
			if co.start > 0 && record.Offset < co.start ||
				co.end > 0 && record.Offset >= co.end {
				continue
			}
			co.num++
			co.format(record)

			if co.num == co.max {
				os.Exit(0)
			}
		}
	}
}

///////////////////////
// __consumer_offset //
///////////////////////

func (co *consumeOutput) buildConsumerOffsetsFormatFn() {
	var out []byte
	co.format = func(r *kgo.Record) {
		out = out[:0]
		out = co.formatConsumerOffsets(out, r)
		os.Stdout.Write(out)
	}
}

func (co *consumeOutput) formatConsumerOffsets(out []byte, r *kgo.Record) []byte {
	orig := out
	out = append(out, r.Topic...)
	out = append(out, " partition "...)
	out = strconv.AppendInt(out, int64(r.Partition), 10)
	out = append(out, " offset "...)
	out = strconv.AppendInt(out, r.Offset, 10)
	out = append(out, " at "...)
	out = r.Timestamp.AppendFormat(out, "[2006-01-02 15:04:05.999]\n")

	if len(r.Key) < 2 || r.Key[0] != 0 {
		out = append(out, "(corrupt short key)\n\n"...)
		return out
	}

	var keep bool
	switch v := r.Key[1]; v {
	case 0, 1:
		out, keep = co.formatOffsetCommit(out, r)
	case 2:
		out, keep = co.formatGroupMetadata(out, r)
	default:
		out = append(out, "(unknown offset key format "...)
		out = append(out, r.Key[1])
		out = append(out, ')')
	}
	if !keep {
		return orig
	}

	out = append(out, "\n\n"...)
	return out
}

func (co *consumeOutput) formatOffsetCommit(dst []byte, r *kgo.Record) ([]byte, bool) {
	{
		var k kmsg.OffsetCommitKey
		if err := k.ReadFrom(r.Key); err != nil {
			if len(r.Key) == 0 {
				return append(dst, "OffsetCommitKey (empty)"...), co.group == ""
			}
			return append(dst, fmt.Sprintf("OffsetCommitKey (could not decode %d bytes)", len(r.Key))...), co.group == ""
		}

		// We can now apply our group filter: do so; if we are keeping
		// this info, all returns after are true.
		if co.group != "" && co.group != k.Group {
			return dst, false
		}

		w := bytes.NewBuffer(dst)
		fmt.Fprintf(w, "OffsetCommitKey(%d)\n", k.Version)

		tw := out.BeginTabWriteTo(w)
		fmt.Fprintf(tw, "\tGroup\t%s\n", k.Group)
		fmt.Fprintf(tw, "\tTopic\t%s\n", k.Topic)
		fmt.Fprintf(tw, "\tPartition\t%d\n", k.Partition)
		tw.Flush()

		dst = w.Bytes()
	}

	{
		var v kmsg.OffsetCommitValue
		if err := v.ReadFrom(r.Value); err != nil {
			if len(r.Value) == 0 {
				return append(dst, "OffsetCommitValue (empty)"...), true
			}
			return append(dst, fmt.Sprintf("OffsetCommitValue (could not decode %d bytes)", len(r.Value))...), true
		}

		w := bytes.NewBuffer(dst)
		fmt.Fprintf(w, "OffsetCommitValue(%d)\n", v.Version)

		tw := out.BeginTabWriteTo(w)
		fmt.Fprintf(tw, "\tOffset\t%d\n", v.Offset)
		if v.Version >= 3 {
			fmt.Fprintf(tw, "\tLeaderEpoch\t%d\n", v.LeaderEpoch)
		}
		fmt.Fprintf(tw, "\tMetadata\t%s\n", v.Metadata)
		fmt.Fprintf(tw, "\tCommitTimestamp\t%d\n", v.CommitTimestamp)
		if v.Version == 1 {
			fmt.Fprintf(tw, "\tExpireTimestamp\t%d\n", v.ExpireTimestamp)
		}
		tw.Flush()

		return w.Bytes(), true
	}
}

func (co *consumeOutput) formatGroupMetadata(dst []byte, r *kgo.Record) ([]byte, bool) {
	{
		var k kmsg.GroupMetadataKey
		if err := k.ReadFrom(r.Key); err != nil {
			if len(r.Key) == 0 {
				return append(dst, "GroupMetadataKey (empty)"...), co.group == ""
			}
			return append(dst, fmt.Sprintf("GroupMetadataKey (could not decode %d bytes)", len(r.Key))...), co.group == ""
		}

		// We can now apply our group filter: do so; if we are keeping
		// this info, all returns after are true.
		if co.group != "" && co.group != k.Group {
			return dst, false
		}

		w := bytes.NewBuffer(dst)
		fmt.Fprintf(w, "GroupMetadataKey(%d)\n", k.Version)

		tw := out.BeginTabWriteTo(w)
		fmt.Fprintf(tw, "\tGroup\t%s\n", k.Group)
		tw.Flush()

		dst = w.Bytes()
	}

	{
		var v kmsg.GroupMetadataValue
		if err := v.ReadFrom(r.Value); err != nil {
			if len(r.Value) == 0 {
				return append(dst, "GroupMetadataValue (empty)"...), true
			}
			return append(dst, fmt.Sprintf("GroupMetadataValue (could not decode %d bytes)", len(r.Value))...), true
		}

		w := bytes.NewBuffer(dst)
		fmt.Fprintf(w, "GroupMetadataValue(%d)\n", v.Version)

		tw := out.BeginTabWriteTo(w)
		fmt.Fprintf(tw, "\tProtocolType\t%s\n", v.ProtocolType)
		fmt.Fprintf(tw, "\tGeneration\t%d\n", v.Generation)
		if v.Protocol == nil {
			fmt.Fprintf(tw, "\tProtocol\t%s\n", "(null)")
		} else {
			fmt.Fprintf(tw, "\tProtocol\t%s\n", *v.Protocol)
		}
		if v.Leader == nil {
			fmt.Fprintf(tw, "\tLeader\t%s\n", "(null)")
		} else {
			fmt.Fprintf(tw, "\tLeader\t%s\n", *v.Leader)
		}

		if v.Version >= 2 {
			fmt.Fprintf(tw, "\tStateTimestamp\t%d\n", v.CurrentStateTimestamp)
		}

		fmt.Fprintf(tw, "\tMembers\t\n")
		tw.Flush()

		for _, member := range v.Members {
			tw = out.BeginTabWriteTo(w)
			fmt.Fprintf(tw, "\t\tMemberID\t%s\n", member.MemberID)
			if v.Version >= 3 {
				if member.InstanceID == nil {
					fmt.Fprintf(tw, "\t\tInstanceID\t%s\n", "(null)")
				} else {
					fmt.Fprintf(tw, "\t\tInstanceID\t%d\n", *member.InstanceID)
				}
			}
			fmt.Fprintf(tw, "\t\tClientID\t%s\n", member.ClientID)
			fmt.Fprintf(tw, "\t\tClientHost\t%s\n", member.ClientHost)
			fmt.Fprintf(tw, "\t\tRebalanceTimeout\t%d\n", member.RebalanceTimeoutMillis)
			fmt.Fprintf(tw, "\t\tSessionTimeout\t%d\n", member.SessionTimeoutMillis)

			if v.ProtocolType != "consumer" {
				fmt.Fprintf(tw, "\t\tSubscription\t(unknown proto type) %x\n", member.Subscription)
				fmt.Fprintf(tw, "\t\tAssignment\t(unknown proto type) %x\n", member.Assignment)
				continue
			}

			var m kmsg.GroupMemberMetadata
			if err := m.ReadFrom(member.Subscription); err != nil {
				fmt.Fprintf(tw, "\t\tSubscription\t(could not decode %d bytes)\n", len(member.Subscription))
			} else {
				fmt.Fprintf(tw, "\t\tSubscription\t\n")
				fmt.Fprintf(tw, "\t\t      Version\t%d\n", m.Version)
				sort.Strings(m.Topics)
				fmt.Fprintf(tw, "\t\t      Topics\t%v\n", m.Topics)
				if v.Protocol != nil && *v.Protocol == "sticky" {
					var s kmsg.StickyMemberMetadata
					if err := s.ReadFrom(m.UserData, m.Version); err != nil {
						fmt.Fprintf(tw, "\t\t      UserData\t(could not read sticky user data)\n")
					} else {
						var sb strings.Builder
						fmt.Fprintf(&sb, "gen %d, current assignment:", s.Generation)
						for _, topic := range s.CurrentAssignment {
							sort.Slice(topic.Partitions, func(i, j int) bool {
								return topic.Partitions[i] < topic.Partitions[j]
							})
							fmt.Fprintf(&sb, " %s=>%v", topic.Topic, topic.Partitions)
						}
						fmt.Fprintf(tw, "\t\t      UserData\t%s\n", sb.String())
					}
				} else {
					fmt.Fprintf(tw, "\t\t b64 UserData\t%s\n", base64.RawStdEncoding.EncodeToString(m.UserData))
				}
			}
			var a kmsg.GroupMemberAssignment
			if err := a.ReadFrom(member.Assignment); err != nil {
				fmt.Fprintf(tw, "\t\tAssignment\t(could not decode %d bytes)\n", len(member.Assignment))
			} else {
				fmt.Fprintf(tw, "\t\tAssignment\t\n")
				fmt.Fprintf(tw, "\t\t      Version\t%d\n", a.Version)
				var sb strings.Builder
				for _, topic := range a.Topics {
					sort.Slice(topic.Partitions, func(i, j int) bool {
						return topic.Partitions[i] < topic.Partitions[j]
					})
					fmt.Fprintf(&sb, "%s=>%v ", topic.Topic, topic.Partitions)
				}
				fmt.Fprintf(tw, "\t\t      Assigned\t%s\n", sb.String())
			}
			tw.Flush()
			fmt.Fprintln(w)
		}

		return w.Bytes(), true
	}
}
