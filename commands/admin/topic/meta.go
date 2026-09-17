package topic

import (
	"bytes"
	"cmp"
	"encoding/hex"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/out"
)

// errorCells are the ERROR and MESSAGE cells of a result row: the Kafka error
// and the broker's message, both "" when the item succeeded.
func errorCells(code int16, message *string) (string, string) {
	var errStr, msg string
	if err := kerr.ErrorForCode(code); err != nil {
		errStr = err.Error()
	}
	if message != nil {
		msg = *message
	}
	return errStr, msg
}

// topicIDCell is a topic id as a table cell: Unknown for the zero id, which
// is what a broker too old to have ids, or a validate-only create, answers.
func topicIDCell(id [16]byte) any {
	if id == [16]byte{} {
		return out.Unknown
	}
	return hex.EncodeToString(id[:])
}

// SortTopics sorts topics by name, a topic we have only an ID for last, and
// every topic's partitions by partition number. Kafka answers in whatever
// order it pleases, and two runs of the same command disagreed.
func SortTopics(topics []kmsg.MetadataResponseTopic) {
	slices.SortFunc(topics, func(l, r kmsg.MetadataResponseTopic) int {
		switch {
		case l.Topic != nil && r.Topic != nil:
			return strings.Compare(*l.Topic, *r.Topic)
		case l.Topic != nil:
			return -1
		case r.Topic != nil:
			return 1
		}
		return bytes.Compare(l.TopicID[:], r.TopicID[:])
	})
	for i := range topics {
		slices.SortFunc(topics[i].Partitions, func(l, r kmsg.MetadataResponseTopicPartition) int {
			return cmp.Compare(l.Partition, r.Partition)
		})
	}
}

// rowMaps is rows as JSON objects under keys, for a document that carries
// more than one table or a table under a key of its own.
func rowMaps(keys []string, rows [][]any) []map[string]any {
	maps := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		m := make(map[string]any, len(keys))
		for i, k := range keys {
			m[k] = row[i]
		}
		maps = append(maps, m)
	}
	return maps
}
