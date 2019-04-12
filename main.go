package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/twmb/kgo"
	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

func init()     { reinitTW() }
func reinitTW() { tw = tabwriter.NewWriter(os.Stdout, 6, 4, 2, ' ', 0) }

var (
	tw *tabwriter.Writer // used for nice output; init and reinit in util

	client *kgo.Client // used for all requests

	root = cobra.Command{
		Use:   "kcl",
		Short: "Kafka Command Line command for commanding Kafka on the command line",
	}
)

func main() {
	cobra.OnInitialize(func() {
		if err := load(); err != nil {
			fmt.Printf("unable to load: %v", err)
			os.Exit(1)
		}
	})

	if err := root.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// requestor can either be a kgo.Client or kgo.Broker.
type requestor interface {
	Request(kmsg.Request) (kmsg.Response, error)
}

type kv struct{ k, v string }

func parseKVs(in []string) ([]kv, error) {
	var kvs []kv
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

func dumpAndDie(d interface{}, msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	dumpJSON(d)
	os.Exit(1)
}

func dumpJSON(resp interface{}) {
	out, err := json.MarshalIndent(resp, "", "  ")
	maybeDie(err, "unable to json marshal response: %v", err)
	fmt.Printf("%s\n", out)
}

func errAndMsg(code int16, msg *string) {
	if err := kerr.ErrorForCode(code); err != nil {
		additional := ""
		if msg != nil {
			additional = ": " + *msg
		}
		fmt.Fprintf(os.Stderr, "%s%s\n", err, additional)
		return
	}
	fmt.Println("OK")
}
