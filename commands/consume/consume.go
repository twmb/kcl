// Package consume contains a cobra command to consume records.
package consume

import (
	"context"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/twmb/kafka-go/pkg/kgo"
	"github.com/twmb/kafka-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/format"
	"github.com/twmb/kcl/out"
)

type consumption struct {
	cl *client.Client

	group      string
	groupAlg   string
	instanceID string
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
	var isConsumerOffsets, isTransactionState bool
	for _, topic := range topics {
		isConsumerOffsets = isConsumerOffsets || topic == "__consumer_offsets"
		isTransactionState = isTransactionState || topic == "__transaction_state"
	}
	if (isConsumerOffsets || isTransactionState) && len(topics) != 1 {
		out.Die("__consumer_offsets or __transaction_state must be the only topic listed when trying to consume it")
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

	var balancer kgo.GroupBalancer
	switch c.groupAlg {
	case "range":
		balancer = kgo.RangeBalancer()
	case "roundrobin":
		balancer = kgo.RoundRobinBalancer()
	case "sticky":
		balancer = kgo.StickyBalancer()
	case "cooperative-sticky":
		balancer = kgo.CooperativeStickyBalancer()
	default:
		out.Die("unrecognized group balancer %q", c.groupAlg)
	}
	groupOpts = append(groupOpts, kgo.GroupBalancers(balancer))

	if c.instanceID != "" {
		groupOpts = append(groupOpts, kgo.GroupInstanceID(c.instanceID))
	}

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	if isConsumerOffsets || isTransactionState {
		c.cl.AddOpt(kgo.WithConsumeKeepControl())
	} else {
		c.cl.AddOpt(kgo.WithConsumeIsolationLevel(kgo.ReadCommitted()))
	}

	cl := c.cl.Client()
	if len(c.group) > 0 && !(isConsumerOffsets || isTransactionState) {
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
	} else if isTransactionState {
		co.buildTransactionStateFormatFn()
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
		cl.CommitOffsets(context.Background(), cl.Uncommitted(), wait)
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

func (co *consumeOutput) buildFormatFn(write string) {
	fn, err := format.ParseWriteFormat(write, '%')
	out.MaybeDie(err, "%v", err)
	var out []byte
	co.format = func(r *kgo.Record) {
		out = fn(out[:0], r)
		os.Stdout.Write(out)
	}
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
