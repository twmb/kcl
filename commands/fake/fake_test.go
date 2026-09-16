package fake

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		in   string
		want kfake.LogLevel
		err  bool
	}{
		{"", kfake.LogLevelNone, false},
		{"none", kfake.LogLevelNone, false},
		{"NONE", kfake.LogLevelNone, false},
		{"error", kfake.LogLevelError, false},
		{"warn", kfake.LogLevelWarn, false},
		{"info", kfake.LogLevelInfo, false},
		{"debug", kfake.LogLevelDebug, false},
		{"DEBUG", kfake.LogLevelDebug, false},
		{"trace", 0, true},
		{"verbose", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseLogLevel(tt.in)
			if tt.err {
				if err == nil {
					t.Errorf("parseLogLevel(%q) expected error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLogLevel(%q) unexpected error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("parseLogLevel(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseBrokerConfigs(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want map[string]string
		err  bool
	}{
		{"nil", nil, nil, false},
		{"empty", []string{}, nil, false},
		{"single", []string{"foo=bar"}, map[string]string{"foo": "bar"}, false},
		{"multiple", []string{"foo=bar", "baz=qux"}, map[string]string{"foo": "bar", "baz": "qux"}, false},
		{"empty value", []string{"foo="}, map[string]string{"foo": ""}, false},
		{"embedded equals", []string{"foo=a=b=c"}, map[string]string{"foo": "a=b=c"}, false},
		{"no equals", []string{"foo"}, nil, true},
		{"mixed valid+invalid", []string{"foo=bar", "baz"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBrokerConfigs(tt.in)
			if tt.err {
				if err == nil {
					t.Errorf("expected error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseSeedTopics(t *testing.T) {
	tests := []struct {
		name  string
		given bool
		in    []string
		want  []seedTopic
		err   bool
	}{
		{"nil", false, nil, nil, false},
		{"bare name", true, []string{"foo"}, []seedTopic{{"foo", -1}}, false},
		{"name:partitions", true, []string{"foo:3"}, []seedTopic{{"foo", 3}}, false},
		{"multiple repeatable", true, []string{"foo:3", "bar:2"}, []seedTopic{{"foo", 3}, {"bar", 2}}, false},
		{"non-int partitions", true, []string{"foo:abc"}, nil, true},
		{"zero partitions", true, []string{"foo:0"}, nil, true},
		{"negative partitions", true, []string{"foo:-1"}, nil, true},
		{"missing partition count", true, []string{"foo:"}, nil, true},
		{"empty", true, []string{""}, nil, true},
		{"empty among others", true, []string{"foo:3", ""}, nil, true},
		{"empty name", true, []string{":3"}, nil, true},
		// pflag drops the value of --seed-topic '' rather than passing an
		// empty entry, so the flag being given with nothing in it is only
		// visible as given with an empty list.
		{"given but empty", true, nil, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSeedTopics(tt.given, tt.in)
			if tt.err {
				if err == nil {
					t.Errorf("expected error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckFlagPairs(t *testing.T) {
	tests := []struct {
		name      string
		blackhole bool
		seedDemo  bool
		synthetic bool
		batch     bool
		err       string
	}{
		{"none", false, false, false, false, ""},
		{"blackhole alone", true, false, false, false, ""},
		{"seed demo alone", false, true, false, false, ""},
		{"blackhole and seed demo", true, true, false, false, "--seed-demo produces records, which --blackhole-produce would drop"},
		{"synthetic alone", false, false, true, false, ""},
		{"synthetic with a batch", false, false, true, true, ""},
		{"a batch alone", false, false, false, true, "--synthetic-batch shapes the batch --synthetic-fetch serves; give --synthetic-fetch too"},
		// A synthetic fetch writes the log, so the demo topics come up
		// with their records and are consumed from the canned batch.
		{"synthetic and seed demo", false, true, true, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkFlagPairs(tt.blackhole, tt.seedDemo, tt.synthetic, tt.batch)
			if tt.err == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || err.Error() != tt.err {
				t.Errorf("error = %v, want %q", err, tt.err)
			}
		})
	}
}

// A blackholed cluster answers a produce the way a cluster that stored the
// records would: the offsets advance, so ListOffsets ends at what we sent,
// and there is nothing to fetch back.
func TestBlackholeProduce(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "foo"), kfake.BlackholeProduce())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const n = 5
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := context.Background()

	for i := range n {
		r := &kgo.Record{Topic: "foo", Value: []byte(strings.Repeat("x", i+1))}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	req := kmsg.NewPtrListOffsetsRequest()
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = "foo"
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition = 0
	rp.Timestamp = -1 // the end offset
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response shape %+v", resp)
	}
	if got := resp.Topics[0].Partitions[0].Offset; got != n {
		t.Errorf("end offset = %d, want %d", got, n)
	}

	ccl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("foo"),
		kgo.ConsumeResetOffset(kgo.NewOffset().At(0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer ccl.Close()
	pctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if fs := ccl.PollFetches(pctx); fs.NumRecords() != 0 {
		t.Errorf("fetched %d records from a blackholed cluster, want 0", fs.NumRecords())
	}
}

func TestParseSyntheticBatch(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want kfake.SyntheticBatch
		err  string
	}{
		{"unset", "", kfake.SyntheticBatch{}, ""},
		{"records", "records=10", kfake.SyntheticBatch{Records: 10}, ""},
		{"bytes", "bytes=20", kfake.SyntheticBatch{RecordBytes: 20}, ""},
		{"random", "random=0.5", kfake.SyntheticBatch{RandomFrac: 0.5}, ""},
		{"random all", "random=1", kfake.SyntheticBatch{RandomFrac: 1}, ""},
		{"compression", "compression=lz4", kfake.SyntheticBatch{Compression: kgo.Lz4Compression()}, ""},
		{"compression any case", "compression=ZSTD", kfake.SyntheticBatch{Compression: kgo.ZstdCompression()}, ""},
		{"compression none", "compression=none", kfake.SyntheticBatch{Compression: kgo.NoCompression()}, ""},
		{"zeroes", "records=0,bytes=0,random=0", kfake.SyntheticBatch{}, ""},
		{
			"every key",
			"records=1000,bytes=100,random=0.5,compression=gzip",
			kfake.SyntheticBatch{Records: 1000, RecordBytes: 100, RandomFrac: 0.5, Compression: kgo.GzipCompression()},
			"",
		},
		{"no equals", "records", kfake.SyntheticBatch{}, `invalid --synthetic-batch "records": want KEY=VALUE`},
		{"unknown key", "nope=1", kfake.SyntheticBatch{}, `invalid --synthetic-batch key "nope": want records, bytes, random, compression`},
		{"records not a number", "records=ten", kfake.SyntheticBatch{}, `invalid --synthetic-batch "records=ten"`},
		{"records negative", "records=-1", kfake.SyntheticBatch{}, `invalid --synthetic-batch "records=-1": must be >= 0`},
		{"bytes negative", "bytes=-1", kfake.SyntheticBatch{}, `invalid --synthetic-batch "bytes=-1": must be >= 0`},
		{"random not a number", "random=half", kfake.SyntheticBatch{}, `invalid --synthetic-batch "random=half"`},
		{"random too big", "random=1.5", kfake.SyntheticBatch{}, `invalid --synthetic-batch "random=1.5": must be in [0,1]`},
		{"random negative", "random=-0.5", kfake.SyntheticBatch{}, `invalid --synthetic-batch "random=-0.5": must be in [0,1]`},
		{"random not a number at all", "random=NaN", kfake.SyntheticBatch{}, `invalid --synthetic-batch "random=NaN": must be in [0,1]`},
		{"unknown codec", "compression=lzo", kfake.SyntheticBatch{}, `invalid --synthetic-batch "compression=lzo": must be none, gzip, snappy, lz4, or zstd`},
		{"one key bad", "records=10,nope=1", kfake.SyntheticBatch{}, `invalid --synthetic-batch key "nope"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSyntheticBatch(tt.in)
			if tt.err != "" {
				if err == nil {
					t.Fatalf("expected an error containing %q, got %+v", tt.err, got)
				}
				if !strings.Contains(err.Error(), tt.err) {
					t.Errorf("error = %v, want it to contain %q", err, tt.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

// A synthetic cluster answers a fetch from any offset, so a consumer can run
// as long as you like without producing anything to read.
func TestSyntheticFetch(t *testing.T) {
	batch, err := parseSyntheticBatch("records=10,bytes=20")
	if err != nil {
		t.Fatal(err)
	}
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "foo"), kfake.SyntheticFetch(batch))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("foo"),
		kgo.ConsumeResetOffset(kgo.NewOffset().At(1000000)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fs := cl.PollRecords(ctx, 5)
	if err := fs.Err(); err != nil {
		t.Fatalf("fetching at 1000000: %v", err)
	}
	if fs.NumRecords() != 5 {
		t.Fatalf("fetched %d records, want 5", fs.NumRecords())
	}
	r := fs.Records()[0]
	if r.Offset < 1000000 {
		t.Errorf("first offset = %d, want at least 1000000", r.Offset)
	}
	if len(r.Value) != 20 {
		t.Errorf("value is %d bytes, want 20", len(r.Value))
	}
}

func TestParseSASLUsers(t *testing.T) {
	// Set predictable env vars for the env-expansion test.
	t.Setenv("TEST_KCL_USER", "alice")
	t.Setenv("TEST_KCL_PASS", "secret")
	t.Setenv("TEST_KCL_EMPTY", "")

	tests := []struct {
		name string
		in   []string
		want []saslUser
		err  string
	}{
		{
			name: "plain literal",
			in:   []string{"plain:alice:pw"},
			want: []saslUser{{"PLAIN", "alice", "pw"}},
		},
		{
			name: "uppercase mechanism accepted",
			in:   []string{"PLAIN:alice:pw"},
			want: []saslUser{{"PLAIN", "alice", "pw"}},
		},
		{
			name: "scram-sha-256",
			in:   []string{"scram-sha-256:bob:pw"},
			want: []saslUser{{"SCRAM-SHA-256", "bob", "pw"}},
		},
		{
			name: "scram-sha-512",
			in:   []string{"scram-sha-512:bob:pw"},
			want: []saslUser{{"SCRAM-SHA-512", "bob", "pw"}},
		},
		{
			name: "env expansion on user and pass",
			in:   []string{"plain:$TEST_KCL_USER:$TEST_KCL_PASS"},
			want: []saslUser{{"PLAIN", "alice", "secret"}},
		},
		{
			name: "env expansion with ${braces}",
			in:   []string{"plain:${TEST_KCL_USER}:${TEST_KCL_PASS}"},
			want: []saslUser{{"PLAIN", "alice", "secret"}},
		},
		{
			name: "multiple entries",
			in:   []string{"plain:admin:adminpw", "scram-sha-256:user:userpw"},
			want: []saslUser{
				{"PLAIN", "admin", "adminpw"},
				{"SCRAM-SHA-256", "user", "userpw"},
			},
		},
		{
			name: "unknown mechanism",
			in:   []string{"bogus:a:b"},
			err:  "mechanism",
		},
		{
			name: "too few fields",
			in:   []string{"plain:alice"},
			err:  "MECHANISM:USER:PASS",
		},
		{
			name: "empty user after expansion",
			in:   []string{"plain:$TEST_KCL_EMPTY:pw"},
			err:  "non-empty after env expansion",
		},
		{
			name: "empty password after expansion",
			in:   []string{"plain:alice:$TEST_KCL_EMPTY"},
			err:  "non-empty after env expansion",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSASLUsers(tt.in)
			if tt.err != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil (result=%v)", tt.err, got)
				}
				if !strings.Contains(err.Error(), tt.err) {
					t.Errorf("expected error containing %q, got %v", tt.err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestSASLEnvIndependence guards against a past bug where we
// shadowed os.Environ during testing; just confirms that env var
// expansion picks up the real process env.
func TestSASLEnvIndependence(t *testing.T) {
	// Skip if the user happens to have these set in their shell.
	if os.Getenv("KCLFAKETEST_X") != "" || os.Getenv("KCLFAKETEST_Y") != "" {
		t.Skip("KCLFAKETEST_X or _Y already set in env; skipping")
	}
	t.Setenv("KCLFAKETEST_X", "u")
	t.Setenv("KCLFAKETEST_Y", "p")
	got, err := parseSASLUsers([]string{"plain:$KCLFAKETEST_X:$KCLFAKETEST_Y"})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if got[0].user != "u" || got[0].pass != "p" {
		t.Errorf("env expansion failed: %+v", got[0])
	}
}
