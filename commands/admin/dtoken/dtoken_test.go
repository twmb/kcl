package dtoken

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// kfake does not answer delegation token requests, so the rows are checked
// against responses built by hand.

func TestCreateRow(t *testing.T) {
	resp := &kmsg.CreateDelegationTokenResponse{
		PrincipalType: "User", PrincipalName: "alice",
		IssueTimestamp: 1000, ExpiryTimestamp: 2000, MaxTimestamp: 3000,
		TokenID: "tok1", HMAC: []byte("secret"),
	}
	row := createRow(resp)
	if len(row) != len(tokenHeaders) {
		t.Fatalf("row has %d cells, headers %d", len(row), len(tokenHeaders))
	}
	if row[0] != "User:alice" || row[4] != "tok1" || row[5] != "c2VjcmV0" {
		t.Errorf("row = %v", row)
	}
	for _, i := range []int{1, 2, 3} {
		if _, ok := row[i].(string); !ok || row[i] == "" {
			t.Errorf("cell %d = %v, want a formatted time", i, row[i])
		}
	}
}

func TestDescribeRow(t *testing.T) {
	for _, test := range []struct {
		name     string
		renewers []kmsg.DescribeDelegationTokenResponseTokenDetailRenewer
		wantText string
		wantJSON string
	}{
		{"no renewers is the owner", nil, "User:alice", `["User:alice"]`},
		{"two renewers", []kmsg.DescribeDelegationTokenResponseTokenDetailRenewer{
			{PrincipalType: "User", PrincipalName: "bob"},
			{PrincipalType: "User", PrincipalName: "carol"},
		}, "User:bob,User:carol", `["User:bob","User:carol"]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			detail := &kmsg.DescribeDelegationTokenResponseTokenDetail{
				PrincipalType: "User", PrincipalName: "alice",
				TokenID: "tok1", HMAC: []byte("secret"),
				Renewers: test.renewers,
			}
			row := describeRow(detail)
			if len(row) != len(describeHeaders) {
				t.Fatalf("row has %d cells, headers %d", len(row), len(describeHeaders))
			}
			if got := fmt.Sprint(row[6]); got != test.wantText {
				t.Errorf("RENEWERS text = %q, want %q", got, test.wantText)
			}
			raw, err := json.Marshal(row[6])
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != test.wantJSON {
				t.Errorf("RENEWERS JSON = %s, want %s", raw, test.wantJSON)
			}
		})
	}
}

func TestExpiryRow(t *testing.T) {
	row := expiryRow(0, 5000)
	if len(row) != len(expiryHeaders) || row[1] != "" || row[2] != "" || row[0] == "" {
		t.Errorf("clean row = %v", row)
	}
	row = expiryRow(62, 0) // DELEGATION_TOKEN_NOT_FOUND
	if fmt.Sprint(row[0]) != "-" || row[1] != "DELEGATION_TOKEN_NOT_FOUND" {
		t.Errorf("failed row = %v", row)
	}
	if raw, _ := json.Marshal(row[0]); string(raw) != "null" {
		t.Errorf("failed EXPIRY JSON = %s, want null", raw)
	}
}

func TestParsePrincipal(t *testing.T) {
	for _, test := range []struct {
		in, typ, name string
	}{
		{"alice", "User", "alice"},
		{"User:alice", "User", "alice"},
		{"Group:ops", "Group", "ops"},
		{"User:a:b", "User", "a:b"},
	} {
		typ, name := parsePrincipal(test.in)
		if typ != test.typ || name != test.name {
			t.Errorf("parsePrincipal(%q) = %q, %q; want %q, %q", test.in, typ, name, test.typ, test.name)
		}
	}
}

// The headers have no spaces or punctuation, so the JSON keys derived from
// them are plain identifiers.
func TestHeaders(t *testing.T) {
	for _, h := range slices.Concat(describeHeaders, expiryHeaders) {
		if strings.ContainsAny(h, " ()") || strings.ToUpper(h) != h {
			t.Errorf("header %q is not an upper case hyphenated word", h)
		}
	}
}
