package fake

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"uuid"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Rule is one fault in JSON, mirroring kfake.Fault. Every selector is an AND
// filter and an unset selector matches everything, so a rule with nothing but
// an error faults every request that can carry one.
type Rule struct {
	Keys       []string `json:"keys,omitzero"`       // request names or numbers
	Nodes      []int32  `json:"nodes,omitzero"`      // broker IDs the request arrived at
	Topic      string   `json:"topic,omitzero"`      //
	TopicID    string   `json:"topic_id,omitzero"`   // a uuid in any usual form
	Partitions []int32  `json:"partitions,omitzero"` //
	Group      string   `json:"group,omitzero"`      //
	TxnID      string   `json:"txn_id,omitzero"`     //
	Resource   string   `json:"resource,omitzero"`   // a config resource, quota entity, SCRAM user, log dir, feature, member, or ACL name
	TopLevel   bool     `json:"top_level,omitzero"`  // fault the response's top-level error code rather than its entities

	Error string `json:"error,omitzero"` // error name or code; unset is UNKNOWN_SERVER_ERROR
	Count int    `json:"count,omitzero"` // requests to fault; unset is one, -1 is until removed
}

// fault converts r into what kfake takes.
func (r Rule) fault() (kfake.Fault, error) {
	f := kfake.Fault{
		Nodes:      r.Nodes,
		Topic:      r.Topic,
		Partitions: r.Partitions,
		Group:      r.Group,
		TxnID:      r.TxnID,
		Resource:   r.Resource,
		TopLevel:   r.TopLevel,
		Count:      r.Count,
	}
	for _, k := range r.Keys {
		key, err := parseRequestKey(k)
		if err != nil {
			return f, err
		}
		f.Keys = append(f.Keys, key)
	}
	if r.TopicID != "" {
		id, err := uuid.Parse(r.TopicID)
		if err != nil {
			return f, fmt.Errorf("topic_id %q: %v", r.TopicID, err)
		}
		f.TopicID = id
	}
	if r.Error != "" {
		e, err := parseErrorCode(r.Error)
		if err != nil {
			return f, err
		}
		f.Err = e
	}
	return f, nil
}

// requestKeys maps a lowercased request name to its key. kmsg only goes the
// other way, so we walk the keys once and reverse it.
var requestKeys = func() map[string]kmsg.Key {
	m := make(map[string]kmsg.Key)
	for k := range int16(256) {
		name := kmsg.NameForKey(k)
		if name == "" || name == "Unknown" {
			continue
		}
		m[strings.ToLower(name)] = kmsg.Key(k)
	}
	return m
}()

// parseRequestKey takes a request name (fetch, Metadata) or its number.
func parseRequestKey(s string) (kmsg.Key, error) {
	if k, ok := requestKeys[strings.ToLower(s)]; ok {
		return k, nil
	}
	n, err := strconv.ParseInt(s, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("unknown request %q", s)
	}
	if kmsg.NameForKey(int16(n)) == "Unknown" {
		return 0, fmt.Errorf("unknown request key %d", n)
	}
	return kmsg.Key(n), nil
}

// errorCodes maps an error name to its code. kerr only goes the other way,
// and an unrecognized code answers UNKNOWN_SERVER_ERROR rather than nil, so
// we keep only the codes that answer to themselves.
var errorCodes = func() map[string]int16 {
	m := make(map[string]int16)
	for c := int16(-1); c < 256; c++ {
		e := kerr.TypedErrorForCode(c)
		if e == nil || e.Code != c {
			continue
		}
		m[strings.ToUpper(e.Message)] = c
	}
	return m
}()

// errorAliases are names Kafka uses that kerr does not. Kafka renamed
// NOT_LEADER_FOR_PARTITION to NOT_LEADER_OR_FOLLOWER in 2.6 and kerr kept the
// original name; the other renames of that era kerr already tracks.
var errorAliases = map[string]int16{
	"NOT_LEADER_OR_FOLLOWER": 6,
}

// parseErrorCode takes an error name (UNKNOWN_TOPIC_ID) or its code.
func parseErrorCode(s string) (*kerr.Error, error) {
	up := strings.ToUpper(s)
	if c, ok := errorCodes[up]; ok {
		return kerr.TypedErrorForCode(c), nil
	}
	if c, ok := errorAliases[up]; ok {
		return kerr.TypedErrorForCode(c), nil
	}
	n, err := strconv.ParseInt(s, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("unknown error %q", s)
	}
	e := kerr.TypedErrorForCode(int16(n))
	if e == nil || e.Code != int16(n) {
		return nil, fmt.Errorf("unknown error code %d", n)
	}
	return e, nil
}

// faultSet is one Fault call: the rules we installed and the handle to them.
type faultSet struct {
	ID    int    `json:"id"`
	Rules []Rule `json:"rules"`
	Hits  int    `json:"hits"`
	Left  int    `json:"left"` // -1 if a rule in the set is unlimited

	h *kfake.FaultHandle
}

// remaining is the budget left across the set. A hit spends one unit from
// whichever rule matched, so what the set has left is its total count less
// its hits. An exhausted set reads 0 and an unlimited one reads -1, which
// tells apart a spent count:3 from a live count:-1 that has fired 3 times.
func (s *faultSet) remaining() int {
	var total int
	for _, r := range s.Rules {
		switch {
		case r.Count < 0:
			return -1
		case r.Count == 0:
			total++ // kfake reads an unset count as one
		default:
			total += r.Count
		}
	}
	return max(0, total-s.Hits)
}

// faults tracks what a control endpoint has installed so that a caller can
// list, wait on, and remove faults by an ID we hand back.
type faults struct {
	c *kfake.Cluster

	mu   sync.Mutex
	next int
	sets map[int]*faultSet
}

func newFaults(c *kfake.Cluster) *faults {
	return &faults{c: c, sets: make(map[int]*faultSet)}
}

func (fs *faults) add(rules []Rule) (*faultSet, error) {
	if len(rules) == 0 {
		return nil, fmt.Errorf("no rules given")
	}
	kfs := make([]kfake.Fault, 0, len(rules))
	for i, r := range rules {
		f, err := r.fault()
		if err != nil {
			return nil, fmt.Errorf("rule %d: %v", i+1, err)
		}
		kfs = append(kfs, f)
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.next++
	set := &faultSet{ID: fs.next, Rules: rules, h: fs.c.Fault(kfs...)}
	fs.sets[set.ID] = set
	return set, nil
}

func (fs *faults) get(id int) *faultSet {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.sets[id]
}

// list returns the installed sets in ID order, each with its current hits.
func (fs *faults) list() []*faultSet {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := make([]*faultSet, 0, len(fs.sets))
	for id := 1; id <= fs.next; id++ {
		if set, ok := fs.sets[id]; ok {
			set.Hits = set.h.Hits()
			set.Left = set.remaining()
			out = append(out, set)
		}
	}
	return out
}

func (fs *faults) remove(id int) bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	set, ok := fs.sets[id]
	if !ok {
		return false
	}
	set.h.Remove()
	delete(fs.sets, id)
	return true
}

func (fs *faults) removeAll() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n := len(fs.sets)
	for id, set := range fs.sets {
		set.h.Remove()
		delete(fs.sets, id)
	}
	return n
}

// faultRoutes adds the fault endpoints to mux.
func faultRoutes(mux *http.ServeMux, fs *faults) {
	mux.HandleFunc("POST /faults", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Rules []Rule `json:"rules"`
		}
		if err := json.UnmarshalRead(r.Body, &req, json.RejectUnknownMembers(true)); err != nil {
			controlWriteErr(w, http.StatusBadRequest, err)
			return
		}
		set, err := fs.add(req.Rules)
		if err != nil {
			controlWriteErr(w, http.StatusBadRequest, err)
			return
		}
		controlWrite(w, http.StatusOK, set)
	})

	mux.HandleFunc("GET /faults", func(w http.ResponseWriter, _ *http.Request) {
		controlWrite(w, http.StatusOK, map[string]any{"faults": fs.list()})
	})

	mux.HandleFunc("DELETE /faults", func(w http.ResponseWriter, _ *http.Request) {
		controlWrite(w, http.StatusOK, map[string]any{"removed": fs.removeAll()})
	})

	mux.HandleFunc("DELETE /faults/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil || !fs.remove(id) {
			controlWriteErr(w, http.StatusNotFound, fmt.Errorf("no fault %s", r.PathValue("id")))
			return
		}
		controlWrite(w, http.StatusOK, map[string]any{"removed": 1})
	})

	// Wait blocks until the fault has answered enough requests, which is how
	// a caller out of process can pace itself against one.
	mux.HandleFunc("POST /faults/{id}/wait", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		set := fs.get(id)
		if err != nil || set == nil {
			controlWriteErr(w, http.StatusNotFound, fmt.Errorf("no fault %s", r.PathValue("id")))
			return
		}
		req := struct {
			Hits    int    `json:"hits"`
			Timeout string `json:"timeout"`
		}{Hits: 1, Timeout: "30s"}
		if err := json.UnmarshalRead(r.Body, &req, json.RejectUnknownMembers(true)); err != nil {
			controlWriteErr(w, http.StatusBadRequest, err)
			return
		}
		timeout, err := time.ParseDuration(req.Timeout)
		if err != nil {
			controlWriteErr(w, http.StatusBadRequest, fmt.Errorf("timeout %q: %v", req.Timeout, err))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := set.h.Wait(ctx, req.Hits); err != nil {
			controlWriteErr(w, http.StatusRequestTimeout, fmt.Errorf("waiting for %d hit(s), have %d: %v", req.Hits, set.h.Hits(), err))
			return
		}
		controlWrite(w, http.StatusOK, map[string]any{"hits": set.h.Hits()})
	})
}
