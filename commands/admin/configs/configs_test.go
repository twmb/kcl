package configs

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/twmb/kcl/out"
)

func TestPromptAlterLoss(t *testing.T) {
	for _, test := range []struct {
		name     string
		in       string
		proceed  bool
		exitCode int
		want     string
	}{
		{"yes", "y\n", true, 0, ""},
		{"yes long", "YES\n", true, 0, ""},
		{"no", "n\n", false, out.ExitError, "Aborting."},
		{"retry then yes", "huh\ny\n", true, 0, `unrecognized input "huh"`},
		{"eof", "", false, out.ExitError, "Aborting."},
		{"eof after garbage", "huh", false, out.ExitError, "Aborting."},
		{"eof after blank line", "\n", false, out.ExitError, "Aborting."},
	} {
		t.Run(test.name, func(t *testing.T) {
			var w bytes.Buffer
			err := promptAlterLoss(strings.NewReader(test.in), &w)
			if test.proceed {
				if err != nil {
					t.Errorf("got err %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Fatal("got nil err, want an abort")
				}
				if code := out.ExitCode(err); code != test.exitCode {
					t.Errorf("got exit code %d, want %d", code, test.exitCode)
				}
			}
			if test.want != "" && !strings.Contains(w.String(), test.want) {
				t.Errorf("got output %q, want it to contain %q", w.String(), test.want)
			}
		})
	}
}

// errReader stands in for a closed or broken stdin: the prompt must give up
// rather than loop on it.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read fail") }

func TestPromptAlterLossReadError(t *testing.T) {
	var w bytes.Buffer
	err := promptAlterLoss(errReader{}, &w)
	if err == nil {
		t.Fatal("got nil err, want an abort")
	}
	if code := out.ExitCode(err); code != out.ExitError {
		t.Errorf("got exit code %d, want %d", code, out.ExitError)
	}
	if !strings.Contains(w.String(), "Aborting.") {
		t.Errorf("got output %q, want it to contain %q", w.String(), "Aborting.")
	}
}

func TestAlterError(t *testing.T) {
	brokerMsg := "topic does not exist"
	for _, test := range []struct {
		name      string
		code      int16
		brokerMsg *string
		wantErr   string
		wantMsg   string
	}{
		{"ok", 0, nil, "OK", ""},
		{"ok ignores broker message", 0, &brokerMsg, "OK", ""},
		{"named code", 3, nil, "UNKNOWN_TOPIC_OR_PARTITION", "This server does not host this topic-partition."},
		{"broker message wins", 3, &brokerMsg, "UNKNOWN_TOPIC_OR_PARTITION", brokerMsg},
		{"unknown code", 9999, nil, "UNKNOWN_SERVER_ERROR", "The server experienced an unexpected error when processing the request."},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotErr, gotMsg := alterError(test.code, test.brokerMsg)
			if gotErr != test.wantErr {
				t.Errorf("got error %q, want %q", gotErr, test.wantErr)
			}
			if gotMsg != test.wantMsg {
				t.Errorf("got message %q, want %q", gotMsg, test.wantMsg)
			}
		})
	}
}
