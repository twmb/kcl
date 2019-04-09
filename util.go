package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/kgo/kmsg"
)

type kv struct{ k, v string }

func parseKVs(in string) ([]kv, error) {
	var kvs []kv
	in = strings.TrimSpace(in)
	if len(in) == 0 {
		return nil, nil
	}
	for _, pair := range strings.Split(in, ",") {
		if strings.IndexByte(pair, '=') == -1 {
			return nil, fmt.Errorf("pair %q missing comma key,val delim", pair)
		}
		rawKV := strings.Split(pair, "=")
		if len(rawKV) != 2 {
			return nil, fmt.Errorf("pair %q contains too many commas", pair)
		}
		k, v := strings.TrimSpace(rawKV[0]), strings.TrimSpace(rawKV[1])
		if len(k) == 0 || len(v) == 0 {
			return nil, fmt.Errorf("pair %q contains an empty key or val", pair)
		}
		kvs = append(kvs, kv{k, v})
	}
	return kvs, nil
}

func maybeDie(err error, msg string, args ...interface{}) {
	if err != nil {
		die(msg, args...)
	}
}

func die(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func dumpJSON(resp kmsg.Response) {
	out, err := json.MarshalIndent(resp, "", "  ")
	maybeDie(err, "unable to json marshal response: %v", err)
	fmt.Printf("%s\n", out)
}
