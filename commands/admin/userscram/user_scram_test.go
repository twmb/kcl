package userscram

import (
	"encoding/json"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func TestSplitPassword(t *testing.T) {
	for _, test := range []struct {
		in       string
		pairs    []string
		password string
		ok       bool
	}{
		{"user=a,mechanism=m,password=p", []string{"user=a", "mechanism=m"}, "p", true},
		{"user=a,mechanism=m,password=a,b=c\"", []string{"user=a", "mechanism=m"}, "a,b=c\"", true},
		{"user=a,PASSWORD=x,y", []string{"user=a"}, "x,y", true},
		{"password=,", nil, ",", true},
		{"password=p,user=a", nil, "p,user=a", true},
		{"user=a,mechanism=m", []string{"user=a", "mechanism=m"}, "", false},
		{"user=a,mypassword=p", []string{"user=a", "mypassword=p"}, "", false},
	} {
		t.Run(test.in, func(t *testing.T) {
			pairs, password, ok := splitPassword(test.in)
			if !slices.Equal(pairs, test.pairs) || password != test.password || ok != test.ok {
				t.Errorf("got (%q, %q, %v), want (%q, %q, %v)", pairs, password, ok, test.pairs, test.password, test.ok)
			}
		})
	}
}

func runUser(t *testing.T, addr, format string, args ...string) (string, error) {
	t.Helper()
	root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
	cl := client.New(root)
	root.AddCommand(Command(cl))
	root.SetArgs(append([]string{"--no-config-file", "-B", addr, "--format", format, "user"}, args...))

	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := root.Execute()
	w.Close()
	os.Stdout = old
	b, _ := io.ReadAll(r)
	return string(b), err
}

// TestAlterAndList pins the alter result shape and exit code, and the list
// order and its error row.
func TestAlterAndList(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]

	for _, user := range []string{"zed", "alice"} {
		raw, err := runUser(t, addr, "awk", "alter", "--set", "user="+user+",mechanism=scram-sha-256,password=pw")
		if err != nil {
			t.Fatalf("alter %s: %v\n%s", user, err, raw)
		}
		if got := strings.TrimSuffix(raw, "\n"); got != user+"\t-\t-" {
			t.Errorf("alter %s awk = %q", user, got)
		}
	}

	raw, err := runUser(t, addr, "json", "list")
	if err != nil {
		t.Fatalf("list: %v\n%s", err, raw)
	}
	var doc struct {
		Credentials []struct {
			User       string `json:"user"`
			Mechanism  string `json:"mechanism"`
			Iterations *int32 `json:"iterations"`
			Error      string `json:"error"`
			Message    string `json:"message"`
		} `json:"credentials"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if len(doc.Credentials) != 2 || doc.Credentials[0].User != "alice" || doc.Credentials[1].User != "zed" {
		t.Errorf("list = %s, want alice then zed", raw)
	}
	if doc.Credentials[0].Mechanism != "SCRAM-SHA-256" || doc.Credentials[0].Iterations == nil || *doc.Credentials[0].Iterations != 4096 {
		t.Errorf("alice = %+v", doc.Credentials[0])
	}

	// An unknown user is a row with its error, and the exit is 1.
	raw, err = runUser(t, addr, "json", "list", "--user", "nosuch")
	if code := out.ExitCode(err); err == nil || code != out.ExitError {
		t.Fatalf("list nosuch: err = %v (exit %d), want a silent exit 1\n%s", err, code, raw)
	}
	doc.Credentials = nil
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	if len(doc.Credentials) != 1 || doc.Credentials[0].Error != "RESOURCE_NOT_FOUND" || doc.Credentials[0].Iterations != nil {
		t.Errorf("list nosuch = %s, want one RESOURCE_NOT_FOUND row with unknown iterations", raw)
	}
	if strings.Contains(doc.Credentials[0].Error, ":") {
		t.Errorf("error = %q, want the bare name; the message is its own key", doc.Credentials[0].Error)
	}

	c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.AlterUserSCRAMCredentials}, Resource: "zed", Err: kerr.InvalidRequest})
	raw, err = runUser(t, addr, "json", "alter", "--del", "user=zed,mechanism=scram-sha-256")
	if code := out.ExitCode(err); err == nil || code != out.ExitError {
		t.Fatalf("faulted alter: err = %v (exit %d), want a silent exit 1\n%s", err, code, raw)
	}
	if !strings.Contains(raw, `"error":"INVALID_REQUEST"`) {
		t.Errorf("faulted alter doc = %s", raw)
	}

	for _, set := range []string{
		"user=a,mechanism=scram-sha-256,iterations=x,password=p",
		"user=a,mechanism=scram-sha-256,salt=zz,password=p",
		"user=a,mechanism=bogus,password=p",
	} {
		_, err := runUser(t, addr, "json", "alter", "--set", set)
		if code := out.ExitCode(err); err == nil || code != out.ExitUsage {
			t.Errorf("--set %q: err = %v (exit %d), want exit 2", set, err, code)
		}
	}
}
