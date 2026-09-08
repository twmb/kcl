package flagutil

import (
	"context"
	"fmt"
	"uuid"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ParseTopicID returns the 16 bytes of a topic id, which may be written as
// bare hex, the dashed 8-4-4-4-12 form, in either case, and with a urn:uuid:
// prefix or braces around it.
func ParseTopicID(s string) ([16]byte, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return [16]byte{}, fmt.Errorf("not a topic id: %v", err)
	}
	return id, nil
}

// MaybeTopicID reports whether s could be a topic id. A topic name may look
// exactly like one, since Kafka allows the same characters in a name.
func MaybeTopicID(s string) bool {
	_, err := ParseTopicID(s)
	return err == nil
}

// ResolveTopics maps topic arguments to topic names. An argument is a name;
// one shaped like a topic id that names no existing topic is looked up as an
// id instead. A name always wins over an id, so a flag is the way to mean
// the id when a topic is named after one.
//
// The cluster is only asked when an argument is shaped like an id, so the
// ordinary case costs nothing.
func ResolveTopics(ctx context.Context, cl kmsg.Requestor, args []string) ([]string, error) {
	var ambiguous bool
	for _, a := range args {
		if MaybeTopicID(a) {
			ambiguous = true
			break
		}
	}
	if !ambiguous {
		return args, nil
	}

	req := kmsg.NewPtrMetadataRequest() // nil Topics: every topic
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("unable to list topics to resolve topic ids: %v", err)
	}
	names := make(map[string]bool, len(resp.Topics))
	byID := make(map[[16]byte]string, len(resp.Topics))
	for _, t := range resp.Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil || t.Topic == nil {
			continue
		}
		names[*t.Topic] = true
		byID[t.TopicID] = *t.Topic
	}

	out := make([]string, 0, len(args))
	for _, a := range args {
		if names[a] || !MaybeTopicID(a) {
			out = append(out, a)
			continue
		}
		id, _ := ParseTopicID(a)
		name, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("no topic named %q, and no topic with that id", a)
		}
		out = append(out, name)
	}
	return out, nil
}
