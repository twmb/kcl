package produce

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

// These tests drive the real cobra command against an in-process kfake
// cluster, with stdin and stdout swapped for pipes.

func newCluster(t *testing.T, topics map[string]int32) (*kfake.Cluster, *kgo.Client) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	for topic, parts := range topics {
		if _, err := kadm.NewClient(cl).CreateTopic(t.Context(), parts, 1, nil, topic); err != nil {
			t.Fatal(err)
		}
	}
	return c, cl
}

// runKCL executes "kcl <args>" with stdin as its input and returns what it
// wrote to stdout and the error Execute returned.
func runKCL(t *testing.T, addrs []string, stdin string, args ...string) ([]byte, error) {
	t.Helper()

	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	kcl := client.New(root)
	root.AddCommand(Command(kcl))
	root.SetArgs(append(args, "--no-config-file", "-B", strings.Join(addrs, ",")))

	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldIn, oldOut := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = inR, outW
	defer func() { os.Stdin, os.Stdout = oldIn, oldOut }()

	// Feed stdin and drain stdout concurrently: a pipe holds 64KB, and
	// either side blocking would deadlock the command.
	var (
		wg  sync.WaitGroup
		buf bytes.Buffer
	)
	wg.Add(2)
	go func() { defer wg.Done(); io.WriteString(inW, stdin); inW.Close() }()
	go func() { defer wg.Done(); io.Copy(&buf, outR) }()

	runErr := root.Execute()
	outW.Close()
	wg.Wait()
	inR.Close()
	outR.Close()
	return buf.Bytes(), runErr
}

func decodeObjects(t *testing.T, b []byte) []map[string]any {
	t.Helper()
	var objs []map[string]any
	dec := json.NewDecoder(bytes.NewReader(b))
	for dec.More() {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			t.Fatalf("output is not JSON objects (%v): %s", err, b)
		}
		objs = append(objs, m)
	}
	return objs
}

// readAll consumes every record in topic, ordered by partition then offset.
func readAll(t *testing.T, addrs []string, topic string, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var recs []*kgo.Record
	deadline := time.Now().Add(10 * time.Second)
	for len(recs) < n && time.Now().Before(deadline) {
		fs := cl.PollFetches(t.Context())
		if err := fs.Err(); err != nil {
			t.Fatal(err)
		}
		fs.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
	}
	if len(recs) != n {
		t.Fatalf("read %d records from %s, want %d", len(recs), topic, n)
	}
	return recs
}

// TestProduceKey pins the precedence: -k fills in a key only where the input
// set none, so a %k in the format wins.
func TestProduceKey(t *testing.T) {
	tests := []struct {
		name  string
		args  []string
		stdin string
		want  []byte // nil is a nil key
	}{
		{"flag on a plain line", []string{"-k", "fk"}, "v\n", []byte("fk")},
		{"empty flag is an empty key", []string{"-k", ""}, "v\n", []byte{}},
		{"%k wins over the flag", []string{"-k", "fk", "-f", "%k %v\n"}, "a v\n", []byte("a")},
		{"no key at all", nil, "v\n", nil},
	}
	topics := map[string]int32{}
	for i := range tests {
		topics[strings.ReplaceAll(tests[i].name, " ", "-")] = 1
	}
	c, _ := newCluster(t, topics)
	addrs := c.ListenAddrs()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := strings.ReplaceAll(tt.name, " ", "-")
			if _, err := runKCL(t, addrs, tt.stdin, append([]string{"produce", topic}, tt.args...)...); err != nil {
				t.Fatalf("produce: %v", err)
			}
			r := readAll(t, addrs, topic, 1)[0]
			if (r.Key == nil) != (tt.want == nil) || !bytes.Equal(r.Key, tt.want) {
				t.Errorf("key = %#v, want %#v", r.Key, tt.want)
			}
			if string(r.Value) != "v" {
				t.Errorf("value = %q, want v", r.Value)
			}
		})
	}
}

// TestProduceNoTopicBeforeStdin pins that the missing topic is reported
// before stdin is read: stdin here is a pipe nothing writes to, so a produce
// that read it first would hang.
func TestProduceNoTopicBeforeStdin(t *testing.T) {
	c, _ := newCluster(t, nil)

	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	kcl := client.New(root)
	root.AddCommand(Command(kcl))
	root.SetArgs([]string{"produce", "--no-config-file", "-B", strings.Join(c.ListenAddrs(), ",")})

	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer inR.Close()
	defer inW.Close()
	oldIn := os.Stdin
	os.Stdin = inR
	defer func() { os.Stdin = oldIn }()

	done := make(chan error, 1)
	go func() { done <- root.Execute() }()
	select {
	case err := <-done:
		if err == nil || out.ExitCode(err) != out.ExitUsage || !strings.Contains(err.Error(), "no topic") {
			t.Errorf("error = %v, want a usage error naming the missing topic", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("produce read stdin before checking for a topic")
	}
}

// TestProduceInputErrors pins the exit code and the wording: an input that
// does not parse is a usage error that names the input format, so someone
// who typed --format json meaning the output learns it is -f here.
func TestProduceInputErrors(t *testing.T) {
	c, _ := newCluster(t, map[string]int32{"t": 1})
	addrs := c.ListenAddrs()

	tests := []struct {
		name  string
		args  []string
		stdin string
		want  string
	}{
		{"--format json is the input format", []string{"--format", "json"}, "x\n", `input format "json"`},
		{"bad layout", []string{"-f", "%q"}, "x\n", `input format "%q"`},
		{"truncated sized input", []string{"-f", "%V{big32}%v"}, "abc", `input format "%V{big32}%v": unexpected EOF`},
		{"bad output layout", []string{"-o", "%q"}, "x\n", `output format "%q"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runKCL(t, addrs, tt.stdin, append([]string{"produce", "t"}, tt.args...)...)
			if err == nil {
				t.Fatalf("expected an error, got none:\n%s", got)
			}
			if code := out.ExitCode(err); code != out.ExitUsage {
				t.Errorf("exit code = %d, want %d", code, out.ExitUsage)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestLayoutParses(t *testing.T) {
	tests := []struct {
		layout string
		verb   byte
		want   bool
	}{
		{`%v\n`, 't', false},
		{`%t %v\n`, 't', true},
		{`%%t %v\n`, 't', false},
		{`%T{big16}%t%V{big32}%v`, 't', true},
		{`%K{big32}%k%V{big32}%v%H{big16}%h{%K{big16}%k%V{big16}%v}`, 'k', true},
		{`%V{big32}%v%H{big16}%h{%K{big16}%k%V{big16}%v}`, 'k', false},
		{`%v{re[%t]}\n`, 't', false},
		{`%k{base64} %v`, 'k', true},
		{`json`, 't', false},
		{`%`, 't', false},
	}
	for _, tt := range tests {
		if got := layoutParses(tt.layout, tt.verb); got != tt.want {
			t.Errorf("layoutParses(%q, %q) = %v, want %v", tt.layout, tt.verb, got, tt.want)
		}
	}
}
