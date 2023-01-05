// Package consume contains a cobra command to consume records.
package consume

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/format"
	"github.com/twmb/kcl/out"
)

type consumption struct {
	cl *client.Client

	group           string
	groupAlg        string
	instanceID      string
	regex           bool
	partitions      []int32
	offset          string
	num             int
	numPerPartition int
	format          string
	escapeChar      string
	rack            string

	readUncommitted bool

	fetchMaxBytes int32
	fetchMaxWait  time.Duration

	start int64 // if exact range
	end   int64 // if exact range

	untilOffset    int
	addUntilOffset bool

	protoFile    string
	protoMessage string
}

// Command returns a consume command.
func Command(cl *client.Client) *cobra.Command {
	return (&consumption{cl: cl}).command()
}

func (c *consumption) run(topics []string) {
	if len(c.escapeChar) == 0 {
		out.Die("invalid empty escape character")
	}
	escape, size := utf8.DecodeRuneInString(c.escapeChar)
	if size != len(c.escapeChar) {
		out.Die("invalid multi character escape character")
	}

	var isConsumerOffsets, isTransactionState bool
	for _, topic := range topics {
		isConsumerOffsets = isConsumerOffsets || topic == "__consumer_offsets"
		isTransactionState = isTransactionState || topic == "__transaction_state"
	}
	if (isConsumerOffsets || isTransactionState) && len(topics) != 1 {
		out.Die("__consumer_offsets or __transaction_state must be the only topic listed when trying to consume it")
	}

	offset := c.parseOffset()
	c.cl.AddOpt(kgo.ConsumeResetOffset(offset))
	if len(c.partitions) == 0 {
		c.cl.AddOpt(kgo.ConsumeTopics(topics...))
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
		c.cl.AddOpt(kgo.ConsumePartitions(offsets))
	}
	if c.regex {
		c.cl.AddOpt(kgo.ConsumeRegex())
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
	c.cl.AddOpt(kgo.Balancers(balancer))

	if c.instanceID != "" {
		c.cl.AddOpt(kgo.InstanceID(c.instanceID))
	}

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	if isConsumerOffsets || isTransactionState {
		c.cl.AddOpt(kgo.KeepControlRecords())
	} else if !c.readUncommitted {
		c.cl.AddOpt(kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}

	c.cl.AddOpt(kgo.FetchMaxBytes(c.fetchMaxBytes))
	c.cl.AddOpt(kgo.FetchMaxWait(c.fetchMaxWait))
	c.cl.AddOpt(kgo.Rack(c.rack))

	isGroup := len(c.group) > 0 && !(isConsumerOffsets || isTransactionState)
	if isGroup {
		c.cl.AddOpt(kgo.ConsumerGroup(c.group))
	}

	if c.untilOffset > -1 {
		c.cl.AddOpt(kgo.KeepControlRecords())
	}

	cl := c.cl.Client()

	ctx, cancel := context.WithCancel(context.Background())
	co := &consumeOutput{
		cl:              cl,
		numPerPartition: c.numPerPartition,
		max:             c.num,
		start:           c.start,
		end:             c.end,
		group:           c.group,
		done:            make(chan struct{}),
		ctx:             ctx,
		cancel:          cancel,
	}
	if c.protoFile != "" {
		var err error
		co.pbd, err = newPBDecoder(c.protoFile, c.protoMessage)
		out.MaybeDie(err, "unable to unmarshal pb: %v", err)
	}

	if c.untilOffset > -1 {
		adm := kadm.NewClient(cl)
		offsets, err := adm.ListEndOffsets(ctx, topics...)
		out.MaybeDie(err, "unable to list end offsets: %v", err)

		// Remove any partitions that are not being consumed.
		if len(c.partitions) > 0 {
			for t, ps := range offsets {
				for p := range ps {
					found := false
					for _, part := range c.partitions {
						if part == p {
							found = true
							break
						}
					}

					if !found {
						delete(offsets[t], p)
					}
				}
			}
		}

		startOffsets, err := adm.ListStartOffsets(ctx, topics...)
		out.MaybeDie(err, "unable to list start offsets: %v", err)

		for t, ps := range startOffsets {
			for p := range ps {
				if hwmPartition, ok := offsets[t]; ok {
					if hwmOffset, ok := hwmPartition[p]; ok {
						if ps[p].Offset >= hwmOffset.Offset {
							delete(offsets[t], p)
						}
					}
				}
			}
		}

		empty := true
		for t, ps := range offsets {
			if len(ps) > 0 {
				empty = false
			} else {
				// Remove any topics that have no partitions.
				delete(offsets, t)
			}
		}

		if empty {
			os.Exit(0)
		}

		co.untilOffset = true
		for t, ps := range offsets {
			for p, o := range ps {
				// Either increment or decrement the offset depending on what was provided (+/-).
				if c.addUntilOffset {
					o.Offset += int64(c.untilOffset)
				} else {
					o.Offset -= int64(c.untilOffset)
				}
				offsets[t][p] = o
			}
		}
		co.untilOffsets = offsets
	}

	if isConsumerOffsets {
		co.buildConsumerOffsetsFormatFn()
	} else if isTransactionState {
		co.buildTransactionStateFormatFn()
	} else {
		fn, err := format.ParseWriteFormat(c.format, escape)
		out.MaybeDie(err, "%v", err)
		var out []byte
		co.format = func(r *kgo.Record, p *kgo.FetchPartition) {
			out = fn(out[:0], r, p)
			os.Stdout.Write(out)
		}
	}

	go co.consume()

	<-sigs
	done := make(chan struct{})
	go func() {
		defer close(done)
		atomic.StoreUint32(&co.quit, 1)
		co.cancel()
		<-co.done
		cl.Close() // leaves group
	}()
	select {
	case <-sigs:
	case <-done:
	}
}

func (c *consumption) parseOffset() kgo.Offset {
	c.end = -1
	c.untilOffset = -1 // Default to -1 to indicate a noop on this option.
	o := kgo.NewOffset()
	switch {
	case c.offset == "start":
		o = o.AtStart()
	case c.offset == "end":
		o = o.AtEnd()
	case strings.HasPrefix(c.offset, "end-"):
		v, err := strconv.Atoi(c.offset[4:])
		out.MaybeDie(err, "unable to parse relative end offset number in %q: %v", c.offset, err)
		o = o.AtEnd().Relative(int64(-v))
	case strings.HasPrefix(c.offset, "start+"):
		v, err := strconv.Atoi(c.offset[6:])
		out.MaybeDie(err, "unable to parse relative start offset number in %q: %v", c.offset, err)
		o = o.AtStart().Relative(int64(v))
	case strings.HasPrefix(c.offset, ":end"):
		o = o.AtStart()
		// Handle the exact match case.
		if c.offset == ":end" {
			c.untilOffset = 0
			return o
		}
		// Parse a format like :end-2 - consume until the end of the partition (current HWM) minus 2.
		// e.g. if the LSO is 6, consume through offset 4.
		if len(c.offset) < 6 {
			out.MaybeDie(errors.New("invalid offset provided"), "must be of the form :end[-/+]<num>")
		}
		op := string(c.offset[4])
		if op != "+" && op != "-" {
			out.MaybeDie(errors.New("invalid offset provided"), "must be of the form :end[-/+]<num>")
		}
		v, err := strconv.Atoi(c.offset[5:])
		out.MaybeDie(err, "unable to parse relative until offset number in %q: %v", c.offset, err)
		if op == "+" {
			c.addUntilOffset = true
		}
		// Something like :end--2.
		if v < 0 {
			v = -v
		}
		c.untilOffset = v
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
		o = o.At(at)
	}
	return o
}

type consumeOutput struct {
	cl *kgo.Client

	numPerPartition int

	num int
	max int

	start int64 // if exact range
	end   int64 // if exact range

	group string // for filtering __consumer_offsets

	untilOffset  bool
	untilOffsets kadm.ListedOffsets

	pbd *pbDecoder

	ctx    context.Context
	cancel func()
	quit   uint32
	done   chan struct{}

	format func(*kgo.Record, *kgo.FetchPartition)
}

func (co *consumeOutput) consume() {
	defer close(co.done)

	type topicPartition struct {
		topic     string
		partition int32
	}
	perPartitionSeen := make(map[topicPartition]int)

	offsetsRemaining := make(map[string]map[int32]struct{})
	for t := range co.untilOffsets {
		offsetsRemaining[t] = make(map[int32]struct{})
		for p := range co.untilOffsets[t] {
			offsetsRemaining[t][p] = struct{}{}
		}
	}

	for atomic.LoadUint32(&co.quit) == 0 {
		if len(co.untilOffsets) != 0 && len(offsetsRemaining) == 0 {
			os.Exit(0)
		}

		fetches := co.cl.PollFetches(co.ctx)
		// TODO Errors(), print to stderr
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			partEndOffset := int64(-1)
			if co.untilOffset {
				t, ok := co.untilOffsets[p.Topic]
				if !ok {
					co.cl.PauseFetchTopics(p.Topic)
					return
				}

				p, ok := t[p.Partition]
				if !ok {
					co.cl.PauseFetchPartitions(map[string][]int32{p.Topic: []int32{p.Partition}})
					return
				}

				partEndOffset = p.Offset
			}

			p.EachRecord(func(r *kgo.Record) {
				if partEndOffset != -1 && r.Offset >= partEndOffset {
					delete(offsetsRemaining[r.Topic], r.Partition)
					if len(offsetsRemaining[r.Topic]) == 0 {
						delete(offsetsRemaining, r.Topic)
					}
					co.cl.PauseFetchPartitions(map[string][]int32{r.Topic: []int32{r.Partition}})
					if r.Offset > partEndOffset {
						return
					}
				}

				// This record offset could be before the requested start
				// following an out of range reset.
				if co.start > 0 && r.Offset < co.start ||
					co.end > 0 && r.Offset >= co.end {
					return
				}

				if co.numPerPartition > 0 {
					tp := topicPartition{p.Topic, p.Partition}
					seen := perPartitionSeen[tp]
					if seen >= co.numPerPartition {
						return
					}
					seen++
					perPartitionSeen[tp] = seen
				}

				// Only increment the count and write if it is not a control message.
				if !r.Attrs.IsControl() {
					co.num++
					if co.pbd != nil {
						r.Value, _ = co.pbd.jsonString(r.Value)
					}
					co.format(r, &p.FetchPartition)

					if co.num == co.max {
						os.Exit(0)
					}
				}
			})
		})
	}
}
