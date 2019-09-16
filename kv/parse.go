package kv

import (
	"fmt"
	"strings"
)

type KV struct {
	K string
	V string
}

func Parse(in []string) ([]KV, error) {
	var kvs []KV
	for _, pair := range in {
		pair = strings.TrimSpace(pair)
		if strings.IndexByte(pair, '=') == -1 {
			return nil, fmt.Errorf("pair %q missing '=' delim", pair)
		}
		rawKV := strings.Split(pair, "=")
		if len(rawKV) != 2 {
			return nil, fmt.Errorf("pair %q contains too many '='s", pair)
		}
		k, v := strings.TrimSpace(rawKV[0]), strings.TrimSpace(rawKV[1])
		if len(k) == 0 || len(v) == 0 {
			return nil, fmt.Errorf("pair %q contains an empty key or val", pair)
		}
		kvs = append(kvs, KV{k, v})
	}
	return kvs, nil
}
