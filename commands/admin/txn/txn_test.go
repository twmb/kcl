package txn

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func runTxn(t *testing.T, addr, format string, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs(append([]string{"--no-config-file", "-B", addr, "--format", format, "txn"}, args...))

	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	return string(b), err
}

// seededCluster holds one record produced to t:1 in a transaction that stays
// open for the test, so that the partition has an active producer and the
// transactional ID "tid" describes.
func seededCluster(t *testing.T) string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "t"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	addr := c.ListenAddrs()[0]

	pcl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.TransactionalID("tid"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pcl.Close)
	if err := pcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := pcl.ProduceSync(context.Background(), &kgo.Record{Topic: "t", Partition: 1, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// TestDescribeProducers pins that the positional TOPIC:P reaches the request
// (it used to be dropped, so every describe answered nothing), that the
// producer columns carry the PRODUCER- prefix, and that ERROR is last.
func TestDescribeProducers(t *testing.T) {
	addr := seededCluster(t)

	raw, err := runTxn(t, addr, "json", "describe-producers", "t:1")
	if err != nil {
		t.Fatalf("describe-producers: %v\n%s", err, raw)
	}
	var doc struct {
		Producers []struct {
			Topic      string `json:"topic"`
			Partition  int32  `json:"partition"`
			ProducerID *int64 `json:"producer_id"`
			Epoch      *int16 `json:"producer_epoch"`
			Error      string `json:"error"`
		} `json:"producers"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if len(doc.Producers) != 1 || doc.Producers[0].Partition != 1 || doc.Producers[0].ProducerID == nil || doc.Producers[0].Error != "" {
		t.Fatalf("doc = %s, want one producer on t:1", raw)
	}

	// A bare topic is every partition; partition 0 has no producer and
	// prints nothing, so the one row is still partition 1.
	raw, err = runTxn(t, addr, "awk", "describe-producers", "t")
	if err != nil {
		t.Fatal(err)
	}
	rows := strings.Split(strings.TrimSuffix(raw, "\n"), "\n")
	if len(rows) != 1 {
		t.Fatalf("awk = %q, want one row", raw)
	}
	fields := strings.Split(rows[0], "\t")
	if len(fields) != len(producersHeaders) || fields[0] != "t" || fields[1] != "1" || fields[len(fields)-1] != "-" {
		t.Errorf("awk row = %q, want t, 1, ..., - across %d fields", rows[0], len(producersHeaders))
	}

	if _, err := runTxn(t, addr, "json", "describe-producers", "t:x"); out.ExitCode(err) != out.ExitUsage {
		t.Errorf("t:x: err = %v, want exit 2", err)
	}
}

// TestDescribe pins the TOPICS cell of a described transaction in every
// format, and the list row of the same transaction.
func TestDescribe(t *testing.T) {
	addr := seededCluster(t)
	raw, err := runTxn(t, addr, "json", "describe", "tid")
	if err != nil {
		t.Fatalf("describe: %v\n%s", err, raw)
	}
	var doc struct {
		Transactions []struct {
			ID     string `json:"transactional_id"`
			State  string `json:"state"`
			Start  *int64 `json:"start_timestamp"`
			Topics []struct {
				Topic      string  `json:"topic"`
				Partitions []int32 `json:"partitions"`
			} `json:"topics"`
			Error string `json:"error"`
		} `json:"transactions"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if len(doc.Transactions) != 1 || doc.Transactions[0].ID != "tid" || doc.Transactions[0].Error != "" || doc.Transactions[0].Start == nil {
		t.Fatalf("doc = %s", raw)
	}
	if topics := doc.Transactions[0].Topics; len(topics) != 1 || topics[0].Topic != "t" || len(topics[0].Partitions) != 1 || topics[0].Partitions[0] != 1 {
		t.Errorf("topics = %+v, want t:1", topics)
	}

	awk, err := runTxn(t, addr, "awk", "describe", "tid")
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Split(strings.TrimSuffix(awk, "\n"), "\t")
	if len(fields) != len(describeHeaders) || fields[6] != "t:1" || fields[7] != "-" {
		t.Errorf("awk row = %q, want TOPICS t:1 and ERROR - across %d fields", awk, len(describeHeaders))
	}

	raw, err = runTxn(t, addr, "awk", "list")
	if err != nil {
		t.Fatal(err)
	}
	fields = strings.Split(strings.TrimSuffix(raw, "\n"), "\t")
	if len(fields) != len(listHeaders) || fields[1] != "tid" || fields[4] != "-" {
		t.Errorf("list awk row = %q, want tid with no error", raw)
	}
}

// TestDescribeUnknown pins an unknown transactional ID: its row carries the
// error with every other cell unknown, and the command exits 1.
func TestDescribeUnknown(t *testing.T) {
	addr := seededCluster(t)
	raw, err := runTxn(t, addr, "json", "describe", "nosuch")
	if code := out.ExitCode(err); err == nil || code != out.ExitError {
		t.Fatalf("err = %v (exit %d), want a silent exit 1\n%s", err, code, raw)
	}
	var doc struct {
		Transactions []struct {
			ID         string `json:"transactional_id"`
			ProducerID *int64 `json:"producer_id"`
			Topics     any    `json:"topics"`
			Error      string `json:"error"`
		} `json:"transactions"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if len(doc.Transactions) != 1 || doc.Transactions[0].ID != "nosuch" || doc.Transactions[0].ProducerID != nil || doc.Transactions[0].Topics != nil || doc.Transactions[0].Error != "TRANSACTIONAL_ID_NOT_FOUND" {
		t.Errorf("doc = %s", raw)
	}
	awk, _ := runTxn(t, addr, "awk", "describe", "nosuch")
	if fields := strings.Split(strings.TrimSuffix(awk, "\n"), "\t"); len(fields) != len(describeHeaders) {
		t.Errorf("awk row = %q, want %d fields", awk, len(describeHeaders))
	}
}

func TestTxnTopics(t *testing.T) {
	ts := txnTopics{
		{Topic: "zed", Partitions: []int32{2}},
		{Topic: "foo", Partitions: []int32{1, 0}},
		{Topic: "empty"},
	}
	if got, want := ts.String(), "empty:;foo:0,1;zed:2"; got != want {
		t.Errorf("String = %q, want %q", got, want)
	}
	raw, err := json.Marshal(ts)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(raw), `[{"topic":"empty","partitions":[]},{"topic":"foo","partitions":[0,1]},{"topic":"zed","partitions":[2]}]`; got != want {
		t.Errorf("JSON = %s, want %s", got, want)
	}
	if got := txnTopics(nil).String(); got != "" {
		t.Errorf("no topics = %q, want empty", got)
	}
}

// TestListSorted pins the list order by transactional ID; kfake answers
// nothing for a cluster with no transactions, so the rows are built from
// a response by hand.
func TestProducerRowsSorted(t *testing.T) {
	resp := &kmsg.DescribeProducersResponse{Topics: []kmsg.DescribeProducersResponseTopic{
		{Topic: "b", Partitions: []kmsg.DescribeProducersResponseTopicPartition{
			{Partition: 1, ActiveProducers: []kmsg.DescribeProducersResponseTopicPartitionActiveProducer{{ProducerID: 9}, {ProducerID: 3}}},
			{Partition: 0, ErrorCode: 3},
		}},
		{Topic: "a", Partitions: []kmsg.DescribeProducersResponseTopicPartition{
			{Partition: 0, ActiveProducers: []kmsg.DescribeProducersResponseTopicPartitionActiveProducer{{ProducerID: 1}}},
		}},
	}}
	var got []string
	for _, r := range producerRows(resp) {
		got = append(got, strings.Join([]string{r[0].(string), toString(r[1]), toString(r[2])}, ":"))
	}
	want := "a:0:1 b:0:- b:1:3 b:1:9"
	if strings.Join(got, " ") != want {
		t.Errorf("rows = %v, want %s", got, want)
	}
}

func toString(v any) string {
	if s, ok := v.(interface{ String() string }); ok {
		return s.String()
	}
	b, _ := json.Marshal(v)
	return string(b)
}
