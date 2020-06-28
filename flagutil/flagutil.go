package flagutil

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseTopicPartitions parses a topic:pa,rt,it,io,ns flag.
func ParseTopicPartitions(list []string) (map[string][]int32, error) {
	tps := make(map[string][]int32)
	for _, item := range list {
		split := strings.SplitN(item, ":", 2)
		if len(split[0]) == 0 {
			return nil, fmt.Errorf("item %q invalid empty topic", item)
		}
		if len(split) == 1 {
			tps[split[0]] = nil
			continue
		}

		strParts := strings.Split(split[1], ",")
		i32Parts := make([]int32, 0, len(strParts))

		for _, strPart := range strParts {
			part, err := strconv.ParseInt(strPart, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("item %q part %q parse err %w", item, strPart, err)
			}
			i32Parts = append(i32Parts, int32(part))
		}
		tps[split[0]] = i32Parts
	}
	return tps, nil
}

// ParseTopicPartitionReplicas parses a list of the following, spaces trimmed:
//
//     topic: 4->3,2,1 ; 5->3,2,1
func ParseTopicPartitionReplicas(list []string) (map[string]map[int32][]int32, error) {
	tprs := make(map[string]map[int32][]int32)
	for _, item := range list {
		tps := strings.SplitN(item, ":", 2)
		if len(tps) != 2 {
			return nil, fmt.Errorf("item %q invalid empty topic", item)
		}

		topic := strings.TrimSpace(tps[0])
		prs := make(map[int32][]int32)
		tprs[topic] = prs

		for _, partitionReplicasRaw := range strings.Split(tps[1], ";") {
			partitionReplicas := strings.SplitN(partitionReplicasRaw, "->", 2)
			if len(partitionReplicas) != 2 {
				return nil, fmt.Errorf("item %q invalid partition->replicas bit %q", item, partitionReplicasRaw)
			}
			partition, err := strconv.Atoi(strings.TrimSpace(partitionReplicas[0]))
			if err != nil {
				return nil, fmt.Errorf("item %q invalid partition in %q", item, partitionReplicasRaw)
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
					return nil, fmt.Errorf("item %q invalid replica in %q", item, partitionReplicasRaw)
				}
				prs[p] = append(prs[p], int32(replica))
			}
		}
	}
	return tprs, nil
}
