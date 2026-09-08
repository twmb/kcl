// Package consume contains a cobra command to consume records.
package consume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/offsetparse"
	"github.com/twmb/kcl/out"
	"github.com/twmb/kcl/serde"
)

type consumption struct {
	cl *client.Client

	group           string
	shareGroup      string
	shareAckType    string
	groupAlg        string
	instanceID      string
	regex           bool
	partitions      []int32
	offset          string
	num             int
	numPerPartition int
	format          string
	rack            string

	readCommitted       bool
	readUncommitted     bool // deprecated no-op: read_uncommitted is now the default
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

	decode []string // schema-registry decode: "key" and/or "value"
}

// Command returns a consume command.
func Command(cl *client.Client) *cobra.Command {
	return (&consumption{cl: cl}).command()
}

// checkTopicsExist errors if any of topics is unknown to the cluster. The
// metadata request does not allow auto creation, so asking does not create.
func checkTopicsExist(ctx context.Context, cl *kgo.Client, topics []string) error {
	req := kmsg.NewPtrMetadataRequest()
	for _, topic := range topics {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return fmt.Errorf("unable to check that topics exist: %v", err)
	}
	for _, t := range resp.Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil && t.Topic != nil {
			return out.Errf(out.ExitError, "unable to consume topic %q: %v", *t.Topic, err)
		}
	}
	return nil
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

	var shareAck kgo.AckStatus
	switch strings.ToLower(c.shareAckType) {
	case "", "accept":
		shareAck = kgo.AckAccept
	case "release":
		shareAck = kgo.AckRelease
	case "reject":
		shareAck = kgo.AckReject
	default:
		return out.Errf(out.ExitUsage, "--share-ack-type must be accept, release, or reject (got %q)", c.shareAckType)
	}
	if c.shareGroup == "" && shareAck != kgo.AckAccept {
		return out.Errf(out.ExitUsage, "--share-ack-type is only valid with --share-group")
	}

	offset, err := c.parseOffset()
	if err != nil {
		return err
	}

	// parseOffset is what sets these, so this cannot be checked any earlier:
	// their zero values are indistinguishable from a requested offset of 0.
	hasEnd := c.end >= 0 || c.untilOffset > -1 || c.endTimestampMillis >= 0

	// Consuming to an end tracks a per-partition end and finishes when every
	// one is reached. A group's assignment is decided by the group and can
	// change while consuming, so there is no fixed set of partitions to
	// finish; the two cannot be combined meaningfully.
	if hasEnd && (c.group != "" || c.shareGroup != "") {
		which := "--group"
		if c.shareGroup != "" {
			which = "--share-group"
		}
		return out.Errf(out.ExitUsage, "%s cannot be combined with an end offset: the group decides which partitions are assigned, and that can change while consuming, so there is no fixed set of partitions to consume to an end", which)
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

	// These are two independent choices. They used to be one if/else,
	// which meant asking to keep control records (or consuming an internal
	// topic) silently switched the isolation level as a side effect.
	// An end offset needs control records kept. A transaction marker
	// occupies an offset, so the last entry before a partition's end can be
	// a marker rather than a record; with markers dropped, the partition
	// never reaches its end and the consume hangs after printing everything
	// it had. They are still not printed or counted -- that is a separate
	// check in the record loop.
	if isConsumerOffsets || isTransactionState || c.printControlRecords || hasEnd {
		c.cl.AddOpt(kgo.KeepControlRecords())
	}
	if c.readCommitted {
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

	// A literal topic that does not exist is a typo far more often than a
	// topic about to be created, so we fail rather than wait; --regex is
	// the way to wait for topics to appear.
	if !c.regex {
		if err := checkTopicsExist(ctx, cl, topics); err != nil {
			return err
		}
	}

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
		shareAck:            shareAck,
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

	var decodeValue, decodeKey bool
	for _, d := range c.decode {
		switch strings.ToLower(d) {
		case "value":
			decodeValue = true
		case "key":
			decodeKey = true
		default:
			return out.Errf(out.ExitUsage, "invalid --decode %q: want key or value", d)
		}
	}
	if decodeValue || decodeKey {
		scl, err := c.cl.SchemaRegistryClient()
		if err != nil {
			return out.Errf(out.ExitUsage, "%v", err)
		}
		co.dec = serde.NewDecoder(scl)
		co.decodeValue = decodeValue
		co.decodeKey = decodeKey
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
			return nil
		}

		co.untilOffset = true
		co.untilOffsets = endTsOffsets
	}

	// An exact end (-o N:M) is resolved the same way as :end, so both share
	// one termination path. Previously an exact end only filtered records and
	// had no way to finish, so the command consumed its range and then hung.
	if c.untilOffset > -1 || c.end >= 0 {
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

		// An exact end bounds every partition, but never beyond what the
		// partition actually holds -- the minimum of the requested end
		// and the high watermark. Setting it unconditionally left any
		// partition short of the requested offset unfinished, so the
		// consume printed everything it had and then waited forever. An
		// empty partition on a multi-partition topic is the common way
		// to hit that.
		if c.end >= 0 {
			for t, ps := range offsets {
				for p, o := range ps {
					if o.Offset > c.end {
						o.Offset = c.end
						offsets[t][p] = o
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
			return nil
		}

		co.untilOffset = true
		if c.untilOffset > -1 {
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
		}
		co.untilOffsets = offsets
	}

	if isConsumerOffsets {
		co.buildConsumerOffsetsFormatFn()
	} else if isTransactionState {
		co.buildTransactionStateFormatFn()
	} else if c.format == jsonFormatName {
		// The bare word "json" is reserved: it selects JSON record output
		// rather than being read as a format string. Matched exactly, so
		// -f 'json%v' remains an ordinary format.
		co.buildJSONFormatFn(c.shareGroup != "")
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

	select {
	case <-sigs:
	case <-co.done:
		// Finished on its own; nothing left to wait for.
		cl.Close()
		return nil
	}

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

	// shareAck is the per-record ack status applied when consuming from
	// a share group. Only AckRelease and AckReject need explicit marking;
	// AckAccept is the default (applied automatically on the next poll).
	shareAck kgo.AckStatus

	pbd *pbDecoder

	dec           *serde.Decoder
	decodeValue   bool
	decodeKey     bool
	decodeErrSeen map[string]struct{} // dedupes per-record decode error messages

	ctx    context.Context
	cancel func()
	quit   uint32
	done   chan struct{}

	format func(*kgo.Record, *kgo.FetchPartition)
}

// srDecode decodes b from the Schema Registry wire format to JSON. If b is not
// SR-framed it is returned unchanged; on any other error the original bytes are
// returned so output is never silently dropped. Each distinct error message is
// printed to stderr only once, so a systemic failure (e.g. the registry being
// unreachable) does not emit one line per record.
func (co *consumeOutput) srDecode(b []byte, what string) []byte {
	json, err := co.dec.Decode(b)
	if err != nil {
		if !errors.Is(err, sr.ErrBadHeader) {
			msg := fmt.Sprintf("unable to schema-decode %s: %v", what, err)
			if co.decodeErrSeen == nil {
				co.decodeErrSeen = make(map[string]struct{})
			}
			if _, seen := co.decodeErrSeen[msg]; !seen {
				co.decodeErrSeen[msg] = struct{}{}
				fmt.Fprintln(os.Stderr, msg)
			}
		}
		return b
	}
	return json
}

// stop signals the consume loop to finish and unblocks run. It mirrors what
// the signal handler does, and exists so that finishing normally -- reaching
// --num, hitting --timeout, or consuming every requested offset -- unwinds
// through the same path as an interrupt rather than calling os.Exit from
// inside a fetch callback. os.Exit also made the command impossible to test in
// process, since it took the test binary with it.
func (co *consumeOutput) stop() {
	atomic.StoreUint32(&co.quit, 1)
	co.cancel()
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
			co.stop()
			return
		}

		// Check timeout: exit if no records received within the duration.
		if co.timeout > 0 && time.Since(lastRecordTime) > co.timeout {
			co.stop()
			return
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

		// Bound the poll by whatever is left of --timeout. PollFetches
		// blocks until records arrive, so without a deadline the loop
		// never returns to the timeout check above and the flag never
		// fired at all.
		pollCtx := co.ctx
		var (
			pollDeadline bool
			cancelPoll   func()
		)
		if co.timeout > 0 {
			pollCtx, cancelPoll = context.WithTimeout(co.ctx, co.timeout-time.Since(lastRecordTime))
			pollDeadline = true
		}
		fetches := co.cl.PollFetches(pollCtx)
		if cancelPoll != nil {
			cancelPoll() // released per iteration, not deferred to function exit
		}

		if idleTimer != nil {
			idleTimer.Stop()
		}
		if co.timeout > 0 && fetches.NumRecords() > 0 {
			lastRecordTime = time.Now()
		}
		// Override the default AckAccept for share groups before
		// the next poll implicitly accepts the fetched records.
		// MarkAcks with no records applies to all records from the
		// last poll that haven't been explicitly marked.
		if co.shareAck != 0 && co.shareAck != kgo.AckAccept && fetches.NumRecords() > 0 {
			co.cl.MarkAcks(co.shareAck)
		}
		fetches.EachError(func(t string, p int32, err error) {
			// Our own poll deadline is not a fetch failure.
			if pollDeadline && errors.Is(err, context.DeadlineExceeded) {
				return
			}
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
				// Already finished: the fetch callbacks cannot be
				// broken out of, so drop the rest of the batch
				// rather than printing past --num.
				if atomic.LoadUint32(&co.quit) != 0 {
					return
				}

				// The end offset is exclusive.
				if partEndOffset != -1 && r.Offset >= partEndOffset {
					return
				}

				// Control records are kept only so that a partition can
				// reach its end offset; they are not data. Drop them
				// here, before any per-record accounting: --num,
				// --num-per-partition, and -G all ran after the old
				// check, so a kept marker consumed a per-partition slot
				// and was matched against grep filters.
				if r.Attrs.IsControl() && !co.printControlRecords {
					return
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

				co.num++
				if co.pbd != nil {
					r.Value, _ = co.pbd.jsonString(r.Value)
				}
				// Schema Registry decode: schema binary -> JSON. Records
				// that are not SR-framed are left as-is; other errors are
				// reported but the raw bytes are still printed.
				if co.dec != nil {
					if co.decodeKey && r.Key != nil {
						r.Key = co.srDecode(r.Key, "key")
					}
					if co.decodeValue && r.Value != nil {
						r.Value = co.srDecode(r.Value, "value")
					}
				}
				co.format(r, &p.FetchPartition)

				if co.num == co.max {
					co.stop()
				}
			})

			// Mark the partition finished once we have consumed
			// through partEndOffset-1. The previous condition waited
			// to see a record AT partEndOffset, which never arrives
			// when the end is the high watermark -- no record exists
			// there yet -- so the command hung after printing its
			// range.
			if partEndOffset != -1 {
				if n := len(p.Records); n > 0 && p.Records[n-1].Offset+1 >= partEndOffset {
					delete(offsetsRemaining[p.Topic], p.Partition)
					if len(offsetsRemaining[p.Topic]) == 0 {
						delete(offsetsRemaining, p.Topic)
					}
					co.cl.PauseFetchPartitions(map[string][]int32{p.Topic: {p.Partition}})
				}
			}
		})

		// Fix C: re-check here, not only at the top of the loop. Once the
		// last requested offset is consumed every partition is paused, so
		// the next poll blocks forever and the top-of-loop check is never
		// reached again.
		if len(co.untilOffsets) != 0 && len(offsetsRemaining) == 0 {
			co.stop()
			return
		}
	}
}
