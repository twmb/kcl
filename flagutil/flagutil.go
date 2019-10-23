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
