// Package consume contains a cobra command to consume records.
package consume

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/offsetparse"
	"github.com/twmb/kcl/out"
)

type consumption struct {
	cl *client.Client

	group           string
	shareGroup      string
	groupAlg        string
	instanceID      string
	regex           bool
	partitions      []int32
	offset          string
	num             int
	numPerPartition int
	format          string
	rack            string

	readUncommitted     bool
	printControlRecords bool
	timeout             time.Duration

	fetchMaxBytes          int32
	fetchMaxPartitionBytes int32
	fetchMaxWait           time.Duration

	start int64 // if exact range
	end   int64 // if exact range

	untilOffset    int
	addUntilOffset bool

	startTimestampMillis int64 // >=0 if start is timestamp-based
	endTimestampMillis   int64 // >=0 if end is timestamp-based

	grepPatterns []string

	protoFile    string
	protoMessage string
}

// Command returns a consume command.
func Command(cl *client.Client) *cobra.Command {
	return (&consumption{cl: cl}).command()
}

func (c *consumption) run(topics []string) error {
	// Compile grep filters.
	var grepFilters []grepFilter
	if len(c.grepPatterns) > 0 {
		var err error
		grepFilters, err = parseGrepFilters(c.grepPatterns)
		if err != nil {
			return fmt.Errorf("%v", err)
		}
	}

	var isConsumerOffsets, isTransactionState bool
	for _, topic := range topics {
		isConsumerOffsets = isConsumerOffsets || topic == "__consumer_offsets"
		isTransactionState = isTransactionState || topic == "__transaction_state"
	}
	if (isConsumerOffsets || isTransactionState) && len(topics) != 1 {
		return out.Errf(out.ExitUsage, "__consumer_offsets or __transaction_state must be the only topic listed when trying to consume it")
	}

	if c.group != "" && c.shareGroup != "" {
		return out.Errf(out.ExitUsage, "--group and --share-group are mutually exclusive")
	}

	offset, err := c.parseOffset()
	if err != nil {
		return err
	}
	c.cl.AddOpt(kgo.ConsumeResetOffset(offset))
	if len(c.partitions) == 0 {
		c.cl.AddOpt(kgo.ConsumeTopics(topics...))
	} else {
		if c.group != "" || c.shareGroup != "" {
			return out.Errf(out.ExitUsage, "incompatible flag assignment: group consuming cannot be used with direct partition consuming")
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
		return out.Errf(out.ExitUsage, "unrecognized group balancer %q", c.groupAlg)
	}
	c.cl.AddOpt(kgo.Balancers(balancer))

	if c.instanceID != "" {
		c.cl.AddOpt(kgo.InstanceID(c.instanceID))
	}

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	if isConsumerOffsets || isTransactionState || c.printControlRecords {
		c.cl.AddOpt(kgo.KeepControlRecords())
	} else if !c.readUncommitted {
		c.cl.AddOpt(kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}

	c.cl.AddOpt(kgo.FetchMaxBytes(c.fetchMaxBytes))
	if c.fetchMaxPartitionBytes > 0 {
		c.cl.AddOpt(kgo.FetchMaxPartitionBytes(c.fetchMaxPartitionBytes))
	}
	c.cl.AddOpt(kgo.FetchMaxWait(c.fetchMaxWait))
	c.cl.AddOpt(kgo.Rack(c.rack))

	if c.shareGroup != "" {
		c.cl.AddOpt(kgo.ShareGroup(c.shareGroup))
	}

	isGroup := len(c.group) > 0 && !(isConsumerOffsets || isTransactionState)
	if isGroup {
		c.cl.AddOpt(kgo.ConsumerGroup(c.group))
	}

	if c.untilOffset > -1 {
		c.cl.AddOpt(kgo.KeepControlRecords())
	}

	cl := c.cl.Client()

	ctx, cancel := context.WithCancel(context.Background())
	var keepCancel bool
	defer func() {
		if !keepCancel {
			cancel()
		}
	}()

	// Resolve timestamp-based start offsets via ListOffsetsAfterMilli.
	if c.startTimestampMillis >= 0 {
		adm := kadm.NewClient(cl)
		tsOffsets, err := adm.ListOffsetsAfterMilli(ctx, c.startTimestampMillis, topics...)
		if err != nil {
			return fmt.Errorf("unable to resolve timestamp to offsets: %v", err)
		}

		setMap := make(map[string]map[int32]kgo.EpochOffset)
		for topic, parts := range tsOffsets {
			setMap[topic] = make(map[int32]kgo.EpochOffset)
			for partition, lo := range parts {
				setMap[topic][partition] = kgo.EpochOffset{
					Epoch:  lo.LeaderEpoch,
					Offset: lo.Offset,
				}
			}
		}
		cl.SetOffsets(setMap)
	}

	co := &consumeOutput{
		cl:                  cl,
		numPerPartition:     c.numPerPartition,
		max:                 c.num,
		start:               c.start,
		end:                 c.end,
		group:               c.group,
		grepFilters:         grepFilters,
		printControlRecords: c.printControlRecords,
		timeout:             c.timeout,
		done:                make(chan struct{}),
		ctx:                 ctx,
		cancel:              cancel,
	}
	if c.protoFile != "" {
		var err error
		co.pbd, err = newPBDecoder(c.protoFile, c.protoMessage)
		if err != nil {
			return fmt.Errorf("unable to unmarshal pb: %v", err)
		}
	}

	// Resolve timestamp-based end offsets.
	if c.endTimestampMillis >= 0 {
		adm := kadm.NewClient(cl)
		endTsOffsets, err := adm.ListOffsetsAfterMilli(ctx, c.endTimestampMillis, topics...)
		if err != nil {
			return fmt.Errorf("unable to resolve end timestamp to offsets: %v", err)
		}

		// Filter to requested partitions if specified.
		if len(c.partitions) > 0 {
			for t, ps := range endTsOffsets {
				for p := range ps {
					found := false
					for _, part := range c.partitions {
						if part == p {
							found = true
							break
						}
					}
					if !found {
						delete(endTsOffsets[t], p)
					}
				}
			}
		}

		// Remove empty partitions where start >= end.
		if c.startTimestampMillis >= 0 {
			startAdm := kadm.NewClient(cl)
			startOffsets, err := startAdm.ListOffsetsAfterMilli(ctx, c.startTimestampMillis, topics...)
			if err != nil {
				return fmt.Errorf("unable to resolve start timestamp for filtering: %v", err)
			}
			for t, ps := range startOffsets {
				for p, so := range ps {
					if eo, ok := endTsOffsets[t][p]; ok {
						if so.Offset >= eo.Offset {
							delete(endTsOffsets[t], p)
						}
					}
				}
			}
		}

		// Remove topics with no remaining partitions.
		empty := true
		for t, ps := range endTsOffsets {
			if len(ps) > 0 {
				empty = false
			} else {
				delete(endTsOffsets, t)
			}
		}
		if empty {
			os.Exit(0)
		}

		co.untilOffset = true
		co.untilOffsets = endTsOffsets
	}

	if c.untilOffset > -1 {
		adm := kadm.NewClient(cl)
		offsets, err := adm.ListEndOffsets(ctx, topics...)
		if err != nil {
			return fmt.Errorf("unable to list end offsets: %v", err)
		}

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
		if err != nil {
			return fmt.Errorf("unable to list start offsets: %v", err)
		}

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
		f, err := kgo.NewRecordFormatter(c.format)
		if err != nil {
			return fmt.Errorf("%v", err)
		}
		var buf []byte
		co.format = func(r *kgo.Record, p *kgo.FetchPartition) {
			buf = f.AppendPartitionRecord(buf[:0], p, r)
			os.Stdout.Write(buf)
		}
	}

	keepCancel = true // ownership transferred to co / signal handler
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
	return nil
}

func (c *consumption) parseOffset() (kgo.Offset, error) {
	spec, err := offsetparse.Parse(c.offset, time.Now())
	if err != nil {
		return kgo.Offset{}, fmt.Errorf("unable to parse offset %q: %v", c.offset, err)
	}

	c.end = -1
	c.untilOffset = -1
	c.startTimestampMillis = -1
	c.endTimestampMillis = -1

	o := kgo.NewOffset()

	switch spec.Start.Kind {
	case offsetparse.KindStart:
		o = o.AtStart()
		if spec.Start.Delta != 0 {
			o = o.Relative(spec.Start.Delta)
		}
	case offsetparse.KindEnd:
		o = o.AtEnd()
		if spec.Start.Delta != 0 {
			o = o.Relative(spec.Start.Delta)
		}
	case offsetparse.KindExact:
		o = o.At(spec.Start.Value)
		c.start = spec.Start.Value
	case offsetparse.KindRelative:
		// For consume: +N means N after start, -N means N before end.
		if spec.Start.Value >= 0 {
			o = o.AtStart().Relative(spec.Start.Value)
		} else {
			o = o.AtEnd().Relative(spec.Start.Value)
		}
	case offsetparse.KindTimestamp:
		c.startTimestampMillis = spec.Start.Value
		o = o.AtStart() // placeholder; resolved via SetOffsets after client creation
	}

	if spec.End != nil {
		switch spec.End.Kind {
		case offsetparse.KindEnd:
			c.untilOffset = 0
			if spec.End.Delta > 0 {
				c.addUntilOffset = true
				c.untilOffset = int(spec.End.Delta)
			} else if spec.End.Delta < 0 {
				c.untilOffset = int(-spec.End.Delta)
			}
		case offsetparse.KindExact:
			c.end = spec.End.Value
		case offsetparse.KindTimestamp:
			c.endTimestampMillis = spec.End.Value
		}
	}

	return o, nil
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

	grepFilters         []grepFilter
	printControlRecords bool
	timeout             time.Duration

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

	var lastRecordTime time.Time
	if co.timeout > 0 {
		lastRecordTime = time.Now()
	}

	var printedWaiting bool

	for atomic.LoadUint32(&co.quit) == 0 {
		if len(co.untilOffsets) != 0 && len(offsetsRemaining) == 0 {
			os.Exit(0)
		}

		// Check timeout: exit if no records received within the duration.
		if co.timeout > 0 && time.Since(lastRecordTime) > co.timeout {
			os.Exit(0)
		}

		// If polling takes more than 1s with no records, print a
		// one-time hint so the user knows we're not hung.
		var idleTimer *time.Timer
		if !printedWaiting {
			idleTimer = time.AfterFunc(3*time.Second, func() {
				fmt.Fprintln(os.Stderr, "waiting for new records...")
				printedWaiting = true
			})
		}

		fetches := co.cl.PollFetches(co.ctx)

		if idleTimer != nil {
			idleTimer.Stop()
		}
		if co.timeout > 0 && fetches.NumRecords() > 0 {
			lastRecordTime = time.Now()
		}
		fetches.EachError(func(t string, p int32, err error) {
			fmt.Fprintf(os.Stderr, "fetch error %s[%d]: %v\n", t, p, err)
		})
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

				// Apply grep filters.
				if len(co.grepFilters) > 0 && !matchAll(co.grepFilters, r) {
					return
				}

				// Only increment the count and write if it is not a control message
				// (unless --print-control-records is set).
				if co.printControlRecords || !r.Attrs.IsControl() {
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
