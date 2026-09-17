package configs

import (
	"testing"
)

func TestAlterError(t *testing.T) {
	brokerMsg := "topic does not exist"
	for _, test := range []struct {
		name      string
		code      int16
		brokerMsg *string
		wantErr   string
		wantMsg   string
	}{
		{"ok", 0, nil, "", ""},
		{"ok ignores broker message", 0, &brokerMsg, "", ""},
		{"named code", 3, nil, "UNKNOWN_TOPIC_OR_PARTITION", ""},
		{"broker message", 3, &brokerMsg, "UNKNOWN_TOPIC_OR_PARTITION", brokerMsg},
		{"unknown code", 9999, nil, "UNKNOWN_SERVER_ERROR", ""},
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
