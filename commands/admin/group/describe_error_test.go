package group

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/out"
)

// TestDescribeOffsetFetchError pins that a group whose OffsetFetch the
// coordinator refuses as a whole is an errored group on both describe
// paths: its summary carries the error with the state and members the
// broker described, it has no lag rows rather than rows with nothing
// committed and a full log of lag, and the command exits 1.
func TestDescribeOffsetFetchError(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	if _, err := adm.CreateTopic(ctx, 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	produceN(t, cl, "t", 5)
	commitAt(t, adm, "auth", "t", 0, 3)
	joinGroup(t, c, "auth848", true, "t")
	h := c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.OffsetFetch}, Err: kerr.GroupAuthorizationFailed, Count: -1})
	defer h.Remove()

	for _, path := range []struct {
		name  string
		group string
		state string
		args  []string
	}{
		{name: "classic", group: "auth", state: "Empty"},
		{name: "consumer", group: "auth848", state: "Stable", args: []string{"--consumer-protocol"}},
	} {
		t.Run(path.name, func(t *testing.T) {
			t.Run("no lag rows", func(t *testing.T) {
				stdout, err := runDescribe(t, c, append([]string{path.group, "--format", "awk"}, path.args...)...)
				if err != out.ErrSilent {
					t.Fatalf("err = %v, want ErrSilent", err)
				}
				if stdout != "" {
					t.Errorf("want no lag row, got:\n%s", stdout)
				}
			})

			t.Run("summary carries the error", func(t *testing.T) {
				args := append([]string{"group", "describe", path.group, "--section", "summary", "--format", "awk"}, path.args...)
				stdout, err := runGroup(t, c, "", args...)
				if err != out.ErrSilent {
					t.Fatalf("err = %v, want ErrSilent", err)
				}
				checkAwkFields(t, stdout, args...)
				rows := awkRows(stdout)
				if len(rows) != 1 {
					t.Fatalf("got %d rows, want 1:\n%s", len(rows), stdout)
				}
				// GROUP COORDINATOR STATE BALANCER MEMBERS TOTAL-LAG ERROR MESSAGE
				row := rows[0]
				if row[0] != path.group || row[2] != path.state || row[5] != "-" || row[6] != "GROUP_AUTHORIZATION_FAILED" || row[7] != "-" {
					t.Errorf("row = %q, want %s %s with no total lag and GROUP_AUTHORIZATION_FAILED", row, path.group, path.state)
				}
			})

			t.Run("json", func(t *testing.T) {
				stdout, err := runDescribe(t, c, append([]string{path.group, "--format", "json"}, path.args...)...)
				if err != out.ErrSilent {
					t.Fatalf("err = %v, want ErrSilent", err)
				}
				var doc struct {
					Groups []struct {
						State    any              `json:"state"`
						Members  []map[string]any `json:"members"`
						TotalLag any              `json:"total_lag"`
						Lag      []map[string]any `json:"lag"`
						Error    string           `json:"error"`
						Message  string           `json:"message"`
					} `json:"groups"`
				}
				if err := unmarshalJSON(stdout, &doc); err != nil {
					t.Fatalf("stdout is not JSON: %v\n%s", err, stdout)
				}
				if len(doc.Groups) != 1 {
					t.Fatalf("unexpected document: %s", stdout)
				}
				g := doc.Groups[0]
				if g.Error != "GROUP_AUTHORIZATION_FAILED" || g.Message != "" || g.State != path.state || g.TotalLag != nil || g.Lag == nil || len(g.Lag) != 0 {
					t.Errorf("group = %+v, want the error, state %s, total_lag null, and lag []", g, path.state)
				}
			})

			t.Run("text", func(t *testing.T) {
				stdout, err := runDescribe(t, c, append([]string{path.group}, path.args...)...)
				if err != out.ErrSilent {
					t.Fatalf("err = %v, want ErrSilent", err)
				}
				if !strings.Contains(stdout, "STATE        "+path.state+"\n") || !strings.Contains(stdout, "ERROR        GROUP_AUTHORIZATION_FAILED\n") {
					t.Errorf("want the state and the error, got:\n%s", stdout)
				}
				if strings.Contains(stdout, "CURRENT-OFFSET") || strings.Contains(stdout, "TOTAL-LAG") {
					t.Errorf("want no lag table and no total, got:\n%s", stdout)
				}
			})
		})
	}
}

// TestDescribeMissingGroupSummary pins that a group the broker could not
// describe has an unknown state, balancer, and member count on its summary
// row, rather than an empty state and 0 members.
func TestDescribeMissingGroupSummary(t *testing.T) {
	c, _ := newTestCluster(t)

	args := []string{"group", "describe", "nope", "--section", "summary", "--format", "awk"}
	stdout, err := runGroup(t, c, "", args...)
	if err != out.ErrSilent {
		t.Fatalf("err = %v, want ErrSilent", err)
	}
	checkAwkFields(t, stdout, args...)
	rows := awkRows(stdout)
	if len(rows) != 1 || !slices.Equal(rows[0][2:], []string{"-", "-", "-", "-", "GROUP_ID_NOT_FOUND", "-"}) {
		t.Errorf("rows = %q, want nope with every cell after COORDINATOR unknown but the error", rows)
	}

	stdout, err = runGroup(t, c, "", "group", "describe", "nope", "--format", "json")
	if err != out.ErrSilent {
		t.Fatalf("err = %v, want ErrSilent", err)
	}
	doc := parseJSON(t, stdout)
	g := doc["groups"].([]any)[0].(map[string]any)
	if g["state"] != nil || g["balancer"] != nil || g["error"] != "GROUP_ID_NOT_FOUND" {
		t.Errorf("group = %v, want state and balancer null with GROUP_ID_NOT_FOUND", g)
	}
	if members, ok := g["members"].([]any); !ok || len(members) != 0 {
		t.Errorf("members = %v, want []", g["members"])
	}
}

// TestDescribeListOffsetsError pins that a partition whose leader answers
// its log offsets with an error carries it in ERROR on its lag row, with
// the lag unknown, and the command exits 1; the other partitions are
// whole. A --by rollup keeps its shape and sums the lag it knows, and the
// command still exits 1.
func TestDescribeListOffsetsError(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	if _, err := adm.CreateTopic(ctx, 2, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	produceTo(t, c, "t", 5, 0, 1)
	commitAt(t, adm, "lo", "t", 0, 2)
	commitAt(t, adm, "lo", "t", 1, 2)
	h := c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Topic: "t", Partitions: []int32{1}, Err: kerr.LeaderNotAvailable, Count: -1})
	defer h.Remove()

	t.Run("awk", func(t *testing.T) {
		args := []string{"group", "describe", "lo", "--format", "awk"}
		stdout, err := runGroup(t, c, "", args...)
		if err != out.ErrSilent {
			t.Fatalf("err = %v, want ErrSilent\n%s", err, stdout)
		}
		checkAwkFields(t, stdout, args...)
		rows := awkRows(stdout)
		if len(rows) != 2 {
			t.Fatalf("got %d rows, want 2:\n%s", len(rows), stdout)
		}
		// GROUP TOPIC PARTITION CURRENT-OFFSET LOG-START-OFFSET LOG-END-OFFSET LAG ... ERROR MESSAGE
		if whole := rows[0]; whole[2] != "0" || whole[5] != "5" || whole[6] != "3" || whole[12] != "-" || whole[13] != "-" {
			t.Errorf("row 0 = %q, want partition 0 with end 5, lag 3, and no error", whole)
		}
		if errored := rows[1]; errored[2] != "1" || errored[3] != "2" || errored[5] != "-" || errored[6] != "-" || errored[12] != "LEADER_NOT_AVAILABLE" || errored[13] != "-" {
			t.Errorf("row 1 = %q, want partition 1 committed at 2 with no end, no lag, and LEADER_NOT_AVAILABLE", errored)
		}
	})

	t.Run("json", func(t *testing.T) {
		stdout, err := runDescribe(t, c, "lo", "--format", "json")
		if err != out.ErrSilent {
			t.Fatalf("err = %v, want ErrSilent", err)
		}
		var doc struct {
			Groups []struct {
				Error    string           `json:"error"`
				TotalLag any              `json:"total_lag"`
				Lag      []map[string]any `json:"lag"`
			} `json:"groups"`
		}
		if err := unmarshalJSON(stdout, &doc); err != nil {
			t.Fatalf("stdout is not JSON: %v\n%s", err, stdout)
		}
		if len(doc.Groups) != 1 || len(doc.Groups[0].Lag) != 2 {
			t.Fatalf("unexpected document: %s", stdout)
		}
		g := doc.Groups[0]
		if g.Error != "" || g.TotalLag != float64(3) {
			t.Errorf("group error %q, total_lag %v; want no group error and the lag we know, 3", g.Error, g.TotalLag)
		}
		if whole := g.Lag[0]; whole["error"] != "" || whole["message"] != "" || whole["lag"] != float64(3) {
			t.Errorf("lag[0] = %v, want no error and lag 3", whole)
		}
		if errored := g.Lag[1]; errored["error"] != "LEADER_NOT_AVAILABLE" || errored["message"] != "" || errored["lag"] != nil || errored["log_end_offset"] != nil {
			t.Errorf("lag[1] = %v, want LEADER_NOT_AVAILABLE with lag and log_end_offset null", errored)
		}
	})

	t.Run("text", func(t *testing.T) {
		stdout, err := runDescribe(t, c, "lo")
		if err != out.ErrSilent {
			t.Fatalf("err = %v, want ErrSilent", err)
		}
		var header string
		for line := range strings.SplitSeq(stdout, "\n") {
			if strings.HasPrefix(line, "TOPIC  PARTITION") {
				header = line
			}
		}
		if fields := strings.Fields(header); len(fields) < 2 || !slices.Equal(fields[len(fields)-2:], []string{"ERROR", "MESSAGE"}) || !strings.Contains(stdout, "LEADER_NOT_AVAILABLE") {
			t.Errorf("want the error under a trailing ERROR MESSAGE, got:\n%s", stdout)
		}
	})

	t.Run("by topic keeps its shape", func(t *testing.T) {
		args := []string{"group", "describe", "lo", "--by", "topic", "--format", "awk"}
		stdout, err := runGroup(t, c, "", args...)
		if err != out.ErrSilent {
			t.Fatalf("err = %v, want ErrSilent", err)
		}
		checkAwkFields(t, stdout, args...)
		if rows, want := awkRows(stdout), [][]string{{"lo", "t", "2", "3"}}; !slices.EqualFunc(rows, want, slices.Equal) {
			t.Errorf("rows = %q, want %q", rows, want)
		}
	})
}

// TestDescribeConsumerProtocolAllShardsFail pins that --consumer-protocol
// fails as the classic path does when no broker answered
// ConsumerGroupDescribe: an error and exit 1, not an empty groups document
// and exit 0.
func TestDescribeConsumerProtocolAllShardsFail(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(context.Background(), 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	joinGroup(t, c, "g848", true, "t")

	// The connection is closed on every ConsumerGroupDescribe, which the
	// client retries until retry_timeout runs out.
	c.ControlKey(int16(kmsg.ConsumerGroupDescribe), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		return nil, errors.New("closed"), true
	})

	stdout, err := runDescribe(t, c, "g848", "--consumer-protocol", "--format", "json", "-X", "retry_timeout=1s")
	if err == nil || err == out.ErrSilent || out.ExitCode(err) != out.ExitError {
		t.Fatalf("err = %v, want an error naming the failed requests", err)
	}
	if stdout != "" {
		t.Errorf("want nothing on stdout, got:\n%s", stdout)
	}
}

// TestDescribeRegexConsumerProtocol pins that -r under --consumer-protocol
// matches consumer groups only: a classic group the pattern matches is not
// described, since ConsumerGroupDescribe would answer GROUP_ID_NOT_FOUND
// for it and fail the command.
func TestDescribeRegexConsumerProtocol(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	if _, err := adm.CreateTopic(ctx, 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	commitAt(t, adm, "rx-classic", "t", 0, 0)
	joinGroup(t, c, "rx-848", true, "t")

	args := []string{"group", "describe", "-r", "rx-.*", "--consumer-protocol", "--section", "summary", "--format", "awk"}
	stdout, err := runGroup(t, c, "", args...)
	if err != nil {
		t.Fatalf("err = %v\n%s", err, stdout)
	}
	checkAwkFields(t, stdout, args...)
	if rows := awkRows(stdout); len(rows) != 1 || rows[0][0] != "rx-848" || rows[0][6] != "-" {
		t.Errorf("rows = %q, want rx-848 alone with no error", rows)
	}
}

// TestDescribeMembersSkipsOffsets pins that --section members issues no
// OffsetFetch and no ListOffsets in text and awk, where nothing of theirs
// prints, and still does in JSON, which prints every section.
func TestDescribeMembersSkipsOffsets(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	produceN(t, cl, "t", 1)

	// The member fetches the record before the counting starts, so that
	// its own OffsetFetch and ListOffsets, issued once it is assigned,
	// are behind it.
	m, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumerGroup("mem"), kgo.ConsumeTopics("t"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for got := 0; got < 1; {
		fs := m.PollFetches(pollCtx)
		if err := fs.Err(); err != nil {
			t.Fatalf("group mem: %v", err)
		}
		got += fs.NumRecords()
	}

	for _, test := range []struct {
		name string
		args []string
		hits bool
	}{
		{"members awk", []string{"--section", "members", "--format", "awk"}, false},
		{"members text", []string{"--section", "members"}, false},
		{"members json", []string{"--section", "members", "--format", "json"}, true},
		{"summary awk", []string{"--section", "summary", "--format", "awk"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.OffsetFetch, kmsg.ListOffsets}, Observe: true, Count: -1})
			defer h.Remove()
			stdout, err := runDescribe(t, c, append([]string{"mem"}, test.args...)...)
			if err != nil {
				t.Fatal(err)
			}
			if stdout == "" {
				t.Error("nothing on stdout")
			}
			if got := h.Hits() > 0; got != test.hits {
				t.Errorf("offset requests issued: %v, want %v", got, test.hits)
			}
		})
	}
}
