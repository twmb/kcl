package out

import (
	"errors"
	"testing"
)

func TestErrorDocAndExitCode(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		command  string
		wantCode int
		wantKeys []string
	}{
		{name: "plain error", err: errors.New("boom"), wantCode: ExitError, wantKeys: []string{"error", "code"}},
		{name: "usage error", err: Errf(ExitUsage, "bad flag"), wantCode: ExitUsage, wantKeys: []string{"error", "code"}},
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
