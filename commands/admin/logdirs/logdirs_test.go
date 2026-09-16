package logdirs

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestHumanSize(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{0, "0B"},
		{500, "500B"},
		{1024, "1.0KB"},
		{1536, "1.5KB"},
		{1048576, "1.0MB"},
		{1073741824, "1.0GB"},
		{1099511627776, "1.0TB"},
		{5368709120, "5.0GB"},
	}
	for _, tt := range tests {
		got := humanSize(tt.bytes)
		if got != tt.want {
			t.Errorf("humanSize(%d) = %q, want %q", tt.bytes, got, tt.want)
		}
	}
}

func TestFormatSize(t *testing.T) {
	for _, test := range []struct {
		name  string
		bytes int64
		human bool
		want  string
		json  string
	}{
		{"zero", 0, false, "0", "0"},
		{"bytes", 5368709120, false, "5368709120", "5368709120"},
		{"human", 5368709120, true, "5.0GB", `"5.0GB"`},
		// A broker below Kafka 3.3 does not report the volume size and
		// sends -1 for it.
		{"unreported", -1, false, "-", "null"},
		{"unreported human", -1, true, "-", "null"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := formatSize(test.bytes, test.human)
			if s := fmt.Sprint(got); s != test.want {
				t.Errorf("formatSize(%d, %v) = %q, want %q", test.bytes, test.human, s, test.want)
			}
			raw, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if string(raw) != test.json {
				t.Errorf("formatSize(%d, %v) JSON = %s, want %s", test.bytes, test.human, raw, test.json)
			}
		})
	}
}
