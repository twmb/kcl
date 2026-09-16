package fake

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/twmb/kcl/serde"
)

// seedRecordCount is how many demo records are produced to each demo topic.
const seedRecordCount = 5

// demoSchemas all describe the same logical shape — {id: string, count: int} —
// so the three encodings can be compared directly.
const (
	demoAvroSchema  = `{"type":"record","name":"Demo","fields":[{"name":"id","type":"string"},{"name":"count","type":"int"}]}`
	demoJSONSchema  = `{"type":"object","properties":{"id":{"type":"string"},"count":{"type":"integer"}},"required":["id","count"]}`
	demoProtoSchema = "syntax = \"proto3\";\nmessage Demo {\n  string id = 1;\n  int32 count = 2;\n}\n"
)

// seedHintFlags are the flags a hint needs to reach this cluster. kcl with
// nothing configured talks to a broker on 9092 and a registry on 8081, so a
// cluster on those needs no flags and one on any other port does; the hint
// is there to paste, and a hint that dials the wrong cluster is worse than
// no hint. The registry flag goes only on a hint that decodes.
func seedHintFlags(brokerAddrs []string, registryURL string) (broker, registry string) {
	if len(brokerAddrs) > 0 && !strings.HasSuffix(brokerAddrs[0], ":"+strconv.Itoa(defaultBrokerPort)) {
		broker = " -B " + brokerAddrs[0]
	}
	if registryURL != "" && !strings.HasSuffix(registryURL, ":"+strconv.Itoa(defaultRegistryPort)) {
		registry = " -R " + registryURL
	}
	return broker, registry
}

// seedDemo creates demo topics, registers a schema of each type, and produces
// records: SR-encoded for the avro/proto/json topics, and plain JSON (no
// schema) for demo-plain. It prints a summary to stderr.
func seedDemo(brokerAddrs []string, registryURL string) error {
	ctx := context.Background()

	scl, err := sr.NewClient(sr.URLs(registryURL))
	if err != nil {
		return fmt.Errorf("unable to create registry client: %v", err)
	}

	kcl, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddrs...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return fmt.Errorf("unable to create kafka client: %v", err)
	}
	defer kcl.Close()

	type demo struct {
		topic  string
		typ    sr.SchemaType
		schema string // empty means no schema (plain JSON)
	}
	demos := []demo{
		{"demo-avro", sr.TypeAvro, demoAvroSchema},
		{"demo-proto", sr.TypeProtobuf, demoProtoSchema},
		{"demo-json", sr.TypeJSON, demoJSONSchema},
		{"demo-plain", 0, ""},
	}

	topics := make([]string, len(demos))
	for i, d := range demos {
		topics[i] = d.topic
	}
	adm := kadm.NewClient(kcl)
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, topics...); err != nil {
		return fmt.Errorf("unable to create demo topics: %v", err)
	}

	brokerFlag, registryFlag := seedHintFlags(brokerAddrs, registryURL)

	fmt.Fprintln(os.Stderr, "seeded demo data:")
	for _, d := range demos {
		var (
			enc *serde.Encoder
			id  int
		)
		if d.schema != "" {
			ss, err := scl.CreateSchema(ctx, d.topic+"-value", sr.Schema{Schema: d.schema, Type: d.typ})
			if err != nil {
				return fmt.Errorf("unable to register %s schema: %v", d.topic, err)
			}
			id = ss.ID
			enc, err = serde.NewEncoder(scl, "", false, serde.Spec{ID: id})
			if err != nil {
				return fmt.Errorf("unable to build %s encoder: %v", d.topic, err)
			}
		}

		for i := 0; i < seedRecordCount; i++ {
			j := []byte(fmt.Sprintf(`{"id":"id-%d","count":%d}`, i, i))
			value := j
			if enc != nil {
				value, err = enc.Encode(nil, j)
				if err != nil {
					return fmt.Errorf("unable to encode %s record: %v", d.topic, err)
				}
			}
			if res := kcl.ProduceSync(ctx, &kgo.Record{Topic: d.topic, Value: value}); res.FirstErr() != nil {
				return fmt.Errorf("unable to produce to %s: %v", d.topic, res.FirstErr())
			}
		}

		if d.schema != "" {
			fmt.Fprintf(os.Stderr, "  %-10s %d records, %s schema id %d (subject %s-value)\n", d.topic, seedRecordCount, d.typ, id, d.topic)
			fmt.Fprintf(os.Stderr, "             kcl%s%s consume %s -o start --decode=value\n", brokerFlag, registryFlag, d.topic)
		} else {
			fmt.Fprintf(os.Stderr, "  %-10s %d records, plain JSON (no schema)\n", d.topic, seedRecordCount)
			fmt.Fprintf(os.Stderr, "             kcl%s consume %s -o start\n", brokerFlag, d.topic)
		}
	}
	return nil
}
