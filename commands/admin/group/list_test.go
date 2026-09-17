package group

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

func unmarshalJSON(s string, v any) error {
	return json.Unmarshal([]byte(s), v)
}

// TestListSortedFiltered pins that group list sorts by group rather than by
// the order brokers answer in, names the group under "group", and that
// --state and --type, and their old names, keep the right rows.
func TestListSortedFiltered(t *testing.T) {
	c, cl := newTestCluster(t)
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(t.Context(), 1, 1, nil, "t"); err != nil {
		t.Fatal(err)
	}
	produceN(t, cl, "t", 1)
	commitAt(t, adm, "list-b", "t", 0, 1)
	commitAt(t, adm, "list-a", "t", 0, 1)
	joinGroup(t, c, "list-live", false, "t")

	for _, test := range []struct {
		name string
		args []string
		want []string
	}{
		{"all, sorted", nil, []string{"list-a", "list-b", "list-live"}},
		{"--state empty", []string{"--state", "empty"}, []string{"list-a", "list-b"}},
		{"--state stable", []string{"--state", "Stable"}, []string{"list-live"}},
		{"two states", []string{"--state", "stable", "--state", "empty"}, []string{"list-a", "list-b", "list-live"}},
		{"-f, the old name", []string{"-f", "stable"}, []string{"list-live"}},
		{"--type classic", []string{"--type", "classic"}, []string{"list-a", "list-b", "list-live"}},
		{"--type share", []string{"--type", "share"}, nil},
		{"--type-filter, the old name", []string{"--type-filter", "share"}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := append([]string{"group", "list", "--format", "awk"}, test.args...)
			stdout, err := runGroup(t, c, "", args...)
			if err != nil {
				t.Fatal(err)
			}
			checkAwkFields(t, stdout, args...)
			var got []string
			for _, row := range awkRows(stdout) {
				got = append(got, row[1])
			}
			if !slices.Equal(got, test.want) {
				t.Errorf("groups = %q, want %q", got, test.want)
			}
		})
	}

	t.Run("json", func(t *testing.T) {
		stdout, err := runGroup(t, c, "", "group", "list", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		doc := parseJSON(t, stdout)
		groups := doc["groups"].([]any)
		if len(groups) != 3 {
			t.Fatalf("want 3 groups: %s", stdout)
		}
		first := groups[0].(map[string]any)
		if first["group"] != "list-a" || first["error"] != "" {
			t.Errorf("first row = %v, want group list-a with an empty error", first)
		}
		if _, ok := first["group_id"]; ok {
			t.Errorf("group_id is still a key: %s", stdout)
		}
	})
}
