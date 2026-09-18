package out

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"golang.org/x/term"
)

func TestErrorDocAndExitCode(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		command  string
		wantCode int
		wantKeys []string
	}{
		{name: "plain error", err: errors.New("boom"), wantCode: ExitError, wantKeys: []string{"error", "code", "_version"}},
		{name: "usage error", err: Errf(ExitUsage, "bad flag"), wantCode: ExitUsage, wantKeys: []string{"error", "code", "_version"}},
		{name: "with a command", err: errors.New("boom"), command: "topic.list", wantCode: ExitError, wantKeys: []string{"error", "code", "_command", "_version"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ExitCode(test.err); got != test.wantCode {
				t.Errorf("ExitCode = %d, want %d", got, test.wantCode)
			}
			doc := ErrorDoc(test.err, test.command)
			if len(doc) != len(test.wantKeys) {
				t.Errorf("doc = %v, want keys %v", doc, test.wantKeys)
			}
			for _, k := range test.wantKeys {
				if _, ok := doc[k]; !ok {
					t.Errorf("doc lacks %q: %v", k, doc)
				}
			}
			if doc["code"] != test.wantCode || doc["error"] != test.err.Error() {
				t.Errorf("doc = %v", doc)
			}
		})
	}
}

func TestCommandName(t *testing.T) {
	for _, test := range []struct {
		path string
		exp  string
	}{
		{"kcl", ""},
		{"kcl topic", "topic"},
		{"kcl topic list", "topic.list"},
		{"kcl registry schema get", "registry.schema.get"},
		{"", ""},
	} {
		t.Run(test.path, func(t *testing.T) {
			if got := CommandName(test.path); got != test.exp {
				t.Errorf("CommandName(%q) = %q != exp %q", test.path, got, test.exp)
			}
		})
	}
}

func TestConfirm(t *testing.T) {
	for _, test := range []struct {
		name     string
		stdin    string
		terminal bool
		want     Answer
		wantOut  string
	}{
		{"yes", "y\n", true, Yes, "Apply? [y/N] "},
		{"YES", "Yes\n", true, Yes, "Apply? [y/N] "},
		{"no", "n\n", true, No, "Apply? [y/N] "},
		{"enter", "\n", true, No, "Apply? [y/N] "},
		{"anything else", "sure\n", true, No, "Apply? [y/N] "},
		{"yes without a newline", "y", true, Yes, "Apply? [y/N] "},
		{"end of input", "", true, NotATerminal, "Apply? [y/N] no (end of input)\n"},
		{"not a terminal", "y\n", false, NotATerminal, "Apply? [y/N] no (stdin is not a terminal)\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var w bytes.Buffer
			got := confirm(strings.NewReader(test.stdin), &w, "Apply?", test.terminal)
			if got != test.want {
				t.Errorf("answer = %v, want %v", got, test.want)
			}
			if w.String() != test.wantOut {
				t.Errorf("stderr = %q, want %q", w.String(), test.wantOut)
			}
		})
	}
	// A pipe is what a script hands us, and /dev/null is a character
	// device; neither is a terminal.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	if term.IsTerminal(int(r.Fd())) {
		t.Error("a pipe reports as a terminal")
	}
	null, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer null.Close()
	if term.IsTerminal(int(null.Fd())) {
		t.Error("/dev/null reports as a terminal")
	}
}

// TestErrorCells pins the two cells of a result: ERROR is the bare kerr name
// and MESSAGE the text the broker attached, each "" when there is none.
func TestErrorCells(t *testing.T) {
	msg := "the text"
	empty := ""
	for _, test := range []struct {
		name string
		code int16
		err  error
		msg  *string
		want string
		cell string
		text string
	}{
		{"none", 0, nil, nil, "", "", ""},
		{"kafka error", 3, kerr.UnknownTopicOrPartition, &msg, "UNKNOWN_TOPIC_OR_PARTITION", "UNKNOWN_TOPIC_OR_PARTITION", "the text"},
		{"wrapped", 3, fmt.Errorf("describing: %w", kerr.UnknownTopicOrPartition), &empty, "UNKNOWN_TOPIC_OR_PARTITION", "UNKNOWN_TOPIC_OR_PARTITION", ""},
		{"not a kafka error", 0, errors.New("dial tcp: refused"), nil, "", "dial tcp: refused", ""},
		{"unknown code", 32000, nil, nil, "UNKNOWN_SERVER_ERROR", "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ErrName(test.code); got != test.want {
				t.Errorf("ErrName(%d) = %q, want %q", test.code, got, test.want)
			}
			if got := ErrCell(test.err); got != test.cell {
				t.Errorf("ErrCell(%v) = %q, want %q", test.err, got, test.cell)
			}
			if got := BrokerMessage(test.msg); got != test.text {
				t.Errorf("BrokerMessage = %q, want %q", got, test.text)
			}
		})
	}
	if got := BrokerErr(kerr.NotController, &msg); got.Error() != "NOT_CONTROLLER: This is not the correct controller for this cluster. (the text)" || !errors.Is(got, kerr.NotController) {
		t.Errorf("BrokerErr = %v", got)
	}
	if got := BrokerErr(kerr.NotController, nil); got != kerr.NotController {
		t.Errorf("BrokerErr with no message = %v, want the error itself", got)
	}
}
