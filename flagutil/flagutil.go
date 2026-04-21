package flagutil

import (
	"fmt"
	"strconv"
	"strings"
)

// SplitTopicPartitionEntries expands comma-separated plain topic names
// while leaving topic:partitions entries intact. Use when a flag accepts
// either plain topic lists (--topics foo,bar) or per-partition scopes
// (--topics foo:0,1) and the flag type is StringArray (no auto-splitting
// on comma). Entries containing ':' are passed through verbatim.
func SplitTopicPartitionEntries(raw []string) []string {
	var out []string
	for _, entry := range raw {
		if strings.Contains(entry, ":") {
			out = append(out, entry)
			continue
		}
		for _, t := range strings.Split(entry, ",") {
			if t != "" {
				out = append(out, t)
			}
		}
	}
	return out
}

// ParseTopicPartitions parses a topic:pa,rt,it,io,ns flag.
//
// Repeated entries for the same topic merge: given "foo:0" and "foo:3",
// the result is {"foo": [0, 3]}. A bare topic entry (no colon) means
// "all partitions" and wins over any per-partition entries for that
// topic; if every entry for a topic is bare, the topic maps to nil.
func ParseTopicPartitions(list []string) (map[string][]int32, error) {
	tps := make(map[string][]int32)
	allPartitions := make(map[string]bool)
	for _, item := range list {
		split := strings.SplitN(item, ":", 2)
		if len(split[0]) == 0 {
			return nil, fmt.Errorf("item %q invalid empty topic", item)
		}
		if len(split) == 1 {
			allPartitions[split[0]] = true
			if _, ok := tps[split[0]]; !ok {
				tps[split[0]] = nil
			}
			continue
		}

		strParts := strings.Split(split[1], ",")
		for _, strPart := range strParts {
			part, err := strconv.ParseInt(strPart, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("item %q part %q parse err %w", item, strPart, err)
			}
			tps[split[0]] = append(tps[split[0]], int32(part))
		}
	}
	// A bare-topic entry (all partitions) wins over per-partition scopes.
	for t := range allPartitions {
		tps[t] = nil
	}
	return tps, nil
}

// ParseTopicPartitionReplicas parses a list of the following, spaces trimmed:
//
//	topic: 4->3,2,1 ; 5->3,2,1
func ParseTopicPartitionReplicas(list []string) (map[string]map[int32][]int32, error) {
	tprs := make(map[string]map[int32][]int32)
	for _, item := range list {
		tps := strings.SplitN(item, ":", 2)
		if len(tps) != 2 {
			return nil, fmt.Errorf("%q invalid empty topic", item)
		}

		topic := strings.TrimSpace(tps[0])
		prs := make(map[int32][]int32)
		tprs[topic] = prs

		for _, partitionReplicasRaw := range strings.Split(tps[1], ";") {
			partitionReplicas := strings.SplitN(partitionReplicasRaw, "->", 2)
			if len(partitionReplicas) != 2 {
				return nil, fmt.Errorf("%q invalid partition->replicas bit %q", item, partitionReplicasRaw)
			}
			partition, err := strconv.Atoi(strings.TrimSpace(partitionReplicas[0]))
			if err != nil {
				return nil, fmt.Errorf("%q invalid partition in %q", item, partitionReplicasRaw)
			}
			p := int32(partition)
			prs[p] = nil
			for _, r := range strings.Split(partitionReplicas[1], ",") {
				r = strings.TrimSpace(r)
				if len(r) == 0 {
					continue
				}
				replica, err := strconv.Atoi(r)
				if err != nil {
					return nil, fmt.Errorf("%q invalid replica in %q", item, partitionReplicasRaw)
				}
				prs[p] = append(prs[p], int32(replica))
			}
			if len(prs[p]) == 0 {
				return nil, fmt.Errorf("%q has no replicas specified", item)
			}
		}
		if len(prs) == 0 {
			return nil, fmt.Errorf("%q has no partitions specified", item)
		}
	}
	return tprs, nil
}
