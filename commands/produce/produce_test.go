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
	"github.com/twmb/kcl/commands/consume"
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
	root.AddCommand(Command(kcl), consume.Command(kcl))
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

// TestProduceJSONRoundTrip pins that "consume -f json | produce -f json"
// reproduces a record exactly: a null key stays null rather than becoming
// empty, bytes that are not UTF-8 travel through the _base64 fields, headers
// come back in order, and the partition and timestamp are kept.
func TestProduceJSONRoundTrip(t *testing.T) {
	c, cl := newCluster(t, map[string]int32{"src": 2, "dst": 2})
	addrs := c.ListenAddrs()

	ts := time.UnixMilli(1755645291123)
	want := []*kgo.Record{
		{Topic: "src", Partition: 1, Timestamp: ts, Key: nil, Value: []byte{0xff, 0xfe}, Headers: []kgo.RecordHeader{
			{Key: "h1", Value: []byte("v1")},
			{Key: "raw", Value: []byte{0xff}},
		}},
		{Topic: "src", Partition: 0, Timestamp: ts.Add(time.Second), Key: []byte("k"), Value: []byte("plain")},
	}
	pcl, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	if res := pcl.ProduceSync(t.Context(), want...); res.FirstErr() != nil {
		t.Fatal(res.FirstErr())
	}
	pcl.Close()
	_ = cl

	dump, err := runKCL(t, addrs, "", "consume", "src", "-o", "start", "-n", "2", "-f", "json")
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if !bytes.Contains(dump, []byte(`"key":null`)) || !bytes.Contains(dump, []byte(`"value_base64":"//4="`)) {
		t.Fatalf("consume dump lacks the null key or the base64 value: %s", dump)
	}

	got, err := runKCL(t, addrs, string(dump), "produce", "dst", "-f", "json", "-o", "json")
	if err != nil {
		t.Fatalf("produce: %v\n%s", err, got)
	}
	for _, o := range decodeObjects(t, got) {
		if o["error"] != "" || o["topic"] != "dst" {
			t.Errorf("produced object = %v", o)
		}
	}

	recs := readAll(t, addrs, "dst", 2)
	byPart := map[int32]*kgo.Record{}
	for _, r := range recs {
		byPart[r.Partition] = r
	}
	for _, w := range want {
		g := byPart[w.Partition]
		if g == nil {
			t.Fatalf("no record on partition %d", w.Partition)
		}
		if (g.Key == nil) != (w.Key == nil) || !bytes.Equal(g.Key, w.Key) {
			t.Errorf("partition %d key = %v, want %v", w.Partition, g.Key, w.Key)
		}
		if !bytes.Equal(g.Value, w.Value) {
			t.Errorf("partition %d value = %v, want %v", w.Partition, g.Value, w.Value)
		}
		if len(g.Headers) != len(w.Headers) {
			t.Fatalf("partition %d has %d headers, want %d", w.Partition, len(g.Headers), len(w.Headers))
		}
		for i := range w.Headers {
			if g.Headers[i].Key != w.Headers[i].Key || !bytes.Equal(g.Headers[i].Value, w.Headers[i].Value) {
				t.Errorf("partition %d header %d = %v, want %v", w.Partition, i, g.Headers[i], w.Headers[i])
			}
		}
		if !g.Timestamp.Equal(w.Timestamp) {
			t.Errorf("partition %d timestamp = %v, want %v", w.Partition, g.Timestamp, w.Timestamp)
		}
	}
}

// TestProduceKey pins the precedence: -k fills in a key only where the input
// set none, so a %k in the format or a key in the object wins, and a null key
// in the object is the same as none. Without -k, null stays null.
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
		{"json key wins over the flag", []string{"-k", "fk", "-f", "json"}, `{"value":"v","key":"a"}`, []byte("a")},
		{"json without a key takes the flag", []string{"-k", "fk", "-f", "json"}, `{"value":"v"}`, []byte("fk")},
		{"json null key takes the flag", []string{"-k", "fk", "-f", "json"}, `{"value":"v","key":null}`, []byte("fk")},
		{"json null key stays null", []string{"-f", "json"}, `{"value":"v","key":null}`, nil},
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

// TestProduceOutputJSON pins the -o json document: one object per record,
// error "" on success, and on a failure the error text with a null offset,
// every record still attempted, and exit 1.
func TestProduceOutputJSON(t *testing.T) {
	c, _ := newCluster(t, map[string]int32{"t": 1})
	addrs := c.ListenAddrs()

	got, err := runKCL(t, addrs, "a\nb\n", "produce", "t", "-o", "json")
	if err != nil {
		t.Fatalf("produce: %v\n%s", err, got)
	}
	objs := decodeObjects(t, got)
	if len(objs) != 2 {
		t.Fatalf("got %d objects, want 2: %s", len(objs), got)
	}
	for i, o := range objs {
		for _, k := range []string{"topic", "partition", "offset", "timestamp", "error"} {
			if _, ok := o[k]; !ok {
				t.Errorf("object %d lacks %s: %v", i, k, o)
			}
		}
		if o["topic"] != "t" || o["partition"] != float64(0) || o["offset"] != float64(i) || o["error"] != "" {
			t.Errorf("object %d = %v", i, o)
		}
	}

	// Partition 5 of a one-partition topic fails every record.
	got, err = runKCL(t, addrs, "a\nb\n", "produce", "t", "-o", "json", "-p", "5")
	if err == nil {
		t.Fatalf("expected an error, got none:\n%s", got)
	}
	if code := out.ExitCode(err); code != out.ExitError {
		t.Errorf("exit code = %d, want %d", code, out.ExitError)
	}
	if err != out.ErrSilent {
		t.Errorf("error = %v, want ErrSilent (the objects already said what failed)", err)
	}
	objs = decodeObjects(t, got)
	if len(objs) != 2 {
		t.Fatalf("got %d objects, want 2 (every record is attempted): %s", len(objs), got)
	}
	for i, o := range objs {
		if o["error"] == "" || o["offset"] != nil || o["timestamp"] != nil {
			t.Errorf("failed object %d = %v", i, o)
		}
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
		{"json unknown field", []string{"-f", "json"}, `{"vallue":"x"}`, `unknown field "/vallue"`},
		{"json not an object", []string{"-f", "json"}, `[1]`, `want an object`},
		{"json both key forms", []string{"-f", "json"}, `{"key":"a","key_base64":"YQ=="}`, `both key and key_base64`},
		{"json bad base64", []string{"-f", "json"}, `{"value_base64":"!!"}`, `value_base64`},
		{"json wrong type", []string{"-f", "json"}, `{"partition":"x"}`, `field "/partition": got a JSON string, want a number`},
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

	// An object without a topic, and none given, is also a usage error;
	// this one can only be found once the object is read.
	_, err := runKCL(t, addrs, `{"value":"x"}`, "produce", "-f", "json")
	if err == nil || out.ExitCode(err) != out.ExitUsage || !strings.Contains(err.Error(), "no topic") {
		t.Errorf("error = %v, want a usage error naming the missing topic", err)
	}
}

// TestProduceJSONTopicAndPartition pins the two precedence rules of -f json:
// a topic on the command line applies to every object, and an object's
// partition is honored unless -p is given.
func TestProduceJSONTopicAndPartition(t *testing.T) {
	c, _ := newCluster(t, map[string]int32{"a": 3, "b": 3})
	addrs := c.ListenAddrs()

	in := `{"topic":"a","partition":2,"value":"x"}` + "\n" + `{"topic":"a","value":"y"}` + "\n"

	// No topic given: the object's topic and partition are used.
	got, err := runKCL(t, addrs, in, "produce", "-f", "json", "-o", "json")
	if err != nil {
		t.Fatalf("produce: %v\n%s", err, got)
	}
	for _, o := range decodeObjects(t, got) {
		if o["topic"] != "a" || o["error"] != "" {
			t.Errorf("object = %v", o)
		}
	}
	// The object that named partition 2 is there; the other went where
	// the partitioner put it, which may also be 2.
	for _, r := range readAll(t, addrs, "a", 2) {
		if string(r.Value) == "x" && r.Partition != 2 {
			t.Errorf("record x is on partition %d, want 2", r.Partition)
		}
	}

	// A topic given overrides the object's; -p overrides its partition.
	got, err = runKCL(t, addrs, in, "produce", "b", "-p", "1", "-f", "json", "-o", "json")
	if err != nil {
		t.Fatalf("produce: %v\n%s", err, got)
	}
	for _, o := range decodeObjects(t, got) {
		if o["topic"] != "b" || o["partition"] != float64(1) {
			t.Errorf("object = %v, want topic b partition 1", o)
		}
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
