package consume

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/kcl/out"
)

/////////////////////////
// __transaction_state //
/////////////////////////

// from object TransactionLog
func (co *consumeOutput) buildTransactionStateFormatFn() {
	var out []byte
	co.format = func(r *kgo.Record, _ *kgo.FetchPartition) {
		out = out[:0]
		out = co.formatTransactionState(out, r)
		os.Stdout.Write(out)
	}
}

func (co *consumeOutput) formatTransactionState(out []byte, r *kgo.Record) []byte {
	orig := out
	out, corrupt := appendInternalTopicRecordStart(out, r)
	if corrupt {
		return out
	}

	var keep bool
	switch v := r.Key[1]; v {
	case 0:
		out, keep = co.formatTransactionStateV0(out, r)
	default:
		out = append(out, "(unknown transaction state key format version "...)
		out = append(out, r.Key[1])
		out = append(out, ')')
	}
	if !keep {
		return orig
	}

	out = append(out, "\n\n"...)
	return out
}

func (co *consumeOutput) formatTransactionStateV0(dst []byte, r *kgo.Record) ([]byte, bool) {
	{
		var k kmsg.TxnMetadataKey
		if err := k.ReadFrom(r.Key); err != nil {
			return append(dst, fmt.Sprintf("TxnMetadataKey (could not decode %d bytes)\n", len(r.Key))...), co.group == ""
		}

		if co.group != "" && co.group != k.TransactionalID {
			return dst, false
		}

		dst = append(dst, fmt.Sprintf("TxnMetadataKey(%d) %s\n", k.Version, k.TransactionalID)...)
	}
	{
		var v kmsg.TxnMetadataValue
		if err := v.ReadFrom(r.Value); err != nil {
			return append(dst, fmt.Sprintf("TxnMetadataValue (could not decode %d bytes)\n", len(r.Value))...), co.group == ""
		}

		w := bytes.NewBuffer(dst)
		fmt.Fprintf(w, "TxnMetadataValue(%d)\n", v.Version)

		tw := out.BeginTabWriteTo(w)
		fmt.Fprintf(tw, "\tProducerID\t%d\n", v.ProducerID)
		fmt.Fprintf(tw, "\tProducerEpoch\t%d\n", v.ProducerEpoch)
		fmt.Fprintf(tw, "\tTimeoutMillis\t%d\n", v.TimeoutMillis)
		fmt.Fprintf(tw, "\tState\t%s\n", v.State.String())

		sort.Slice(v.Topics, func(i, j int) bool { return v.Topics[i].Topic < v.Topics[j].Topic })
		var sb strings.Builder
		for _, topic := range v.Topics {
			sort.Slice(topic.Partitions, func(i, j int) bool { return topic.Partitions[i] < topic.Partitions[j] })
			fmt.Fprintf(&sb, "%s=>%v ", topic.Topic, topic.Partitions)
		}
		fmt.Fprintf(tw, "\tTopics\t%s\n", sb.String())

		fmt.Fprintf(tw, "\tLastUpdateTimestamp\t%d\n", v.LastUpdateTimestamp)
		fmt.Fprintf(tw, "\tStartTimestamp\t%d\n", v.StartTimestamp)
		tw.Flush()

		return w.Bytes(), true
	}
}
