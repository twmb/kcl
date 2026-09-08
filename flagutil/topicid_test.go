package flagutil

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestParseTopicIDAndMaybe(t *testing.T) {
	for _, test := range []struct {
		in    string
		ok    bool
		first byte
	}{
		{in: "15fc1bf40a5c1c3cdd363ec28f5c0c69", ok: true, first: 0x15},
		{in: "15fc1bf4-0a5c-1c3c-dd36-3ec28f5c0c69", ok: true, first: 0x15},
		{in: "15FC1BF40A5C1C3CDD363EC28F5C0C69", ok: true, first: 0x15},
		{in: "15FC1BF4-0A5C-1C3C-DD36-3EC28F5C0C69", ok: true, first: 0x15},
		{in: "urn:uuid:15fc1bf4-0a5c-1c3c-dd36-3ec28f5c0c69", ok: true, first: 0x15},
		{in: "{15fc1bf4-0a5c-1c3c-dd36-3ec28f5c0c69}", ok: true, first: 0x15},
		{in: "00000000-0000-0000-0000-000000000000", ok: true},
		{in: "mytopic"},
		{in: ""},
		{in: "15fc1bf40a5c1c3cdd363ec28f5c0c6"},   // 31
		{in: "15fc1bf40a5c1c3cdd363ec28f5c0c699"}, // 33
		{in: "zzfc1bf40a5c1c3cdd363ec28f5c0c69"},  // not hex
	} {
		t.Run(test.in, func(t *testing.T) {
			id, err := ParseTopicID(test.in)
			if (err == nil) != test.ok {
				t.Fatalf("err = %v, want ok %v", err, test.ok)
			}
			if got := MaybeTopicID(test.in); got != test.ok {
				t.Errorf("MaybeTopicID = %v, want %v", got, test.ok)
			}
			if test.ok && id[0] != test.first {
				t.Errorf("id[0] = %#x, want %#x", id[0], test.first)
			}
		})
	}
}

// TestResolveTopics pins that a name beats an id, an id resolves to its
// name, and an argument that is neither is reported.
func TestResolveTopics(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "plain"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// The id of "plain", and a topic named after that id.
	req := kmsg.NewPtrMetadataRequest()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	var plainID string
	for _, tp := range resp.Topics {
		if tp.Topic != nil && *tp.Topic == "plain" {
			plainID = hexID(tp.TopicID)
		}
	}
	if plainID == "" {
		t.Fatal("no id for the seeded topic")
	}

	create := kmsg.NewPtrCreateTopicsRequest()
	for _, name := range []string{plainID, "0123456789abcdef0123456789abcdef"} {
		ct := kmsg.NewCreateTopicsRequestTopic()
		ct.Topic = name
		ct.NumPartitions = 1
		ct.ReplicationFactor = 1
		create.Topics = append(create.Topics, ct)
	}
	if _, err := create.RequestWith(ctx, cl); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name    string
		args    []string
		want    []string
		wantErr string
	}{
		{name: "plain names are untouched", args: []string{"plain", "other"}, want: []string{"plain", "other"}},
		{name: "an id resolves to its name", args: []string{"0123456789abcdef0123456789abcdef"}, want: []string{"0123456789abcdef0123456789abcdef"}},
		{name: "a name that looks like an id wins", args: []string{plainID}, want: []string{plainID}},
		{name: "mixed", args: []string{"plain", plainID}, want: []string{"plain", plainID}},
		{name: "neither", args: []string{"ffffffffffffffffffffffffffffffff"}, wantErr: "no topic named"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ResolveTopics(ctx, cl, test.args)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("got %v, want %v", got, test.want)
			}
		})
	}
}

func hexID(id [16]byte) string {
	const hexc = "0123456789abcdef"
	b := make([]byte, 0, 32)
	for _, c := range id {
		b = append(b, hexc[c>>4], hexc[c&0xf])
	}
	return string(b)
}
