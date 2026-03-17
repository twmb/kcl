package offsetparse

import (
	"testing"
	"time"
)

var testNow = time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

func pos(kind PositionKind, value, delta int64) Position {
	return Position{Kind: kind, Value: value, Delta: delta}
}

func pptr(kind PositionKind, value, delta int64) *Position {
	p := pos(kind, value, delta)
	return &p
}

func dateMillis(year int, month time.Month, day, hour, min, sec int) int64 {
	return time.Date(year, month, day, hour, min, sec, 0, time.UTC).UnixMilli()
}

func TestParseOffsetKeywords(t *testing.T) {
	tests := []struct {
		input string
		start Position
		end   *Position
	}{
		{"start", pos(KindStart, 0, 0), nil},
		{"end", pos(KindEnd, 0, 0), nil},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input, testNow)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkSpec(t, tt.input, got, tt.start, tt.end)
		})
	}
}

func TestParseStartEndDelta(t *testing.T) {
	tests := []struct {
		input string
		start Position
	}{
		{"start+5", pos(KindStart, 0, 5)},
		{"start+0", pos(KindStart, 0, 0)},
		{"start+100", pos(KindStart, 0, 100)},
		{"end-3", pos(KindEnd, 0, -3)},
		{"end-0", pos(KindEnd, 0, 0)},
		{"end-100", pos(KindEnd, 0, -100)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input, testNow)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkSpec(t, tt.input, got, tt.start, nil)
		})
	}
}

func TestParseRelativeOffsets(t *testing.T) {
	tests := []struct {
		input string
		start Position
	}{
		{"+10", pos(KindRelative, 10, 0)},
		{"+0", pos(KindRelative, 0, 0)},
		{"+1000", pos(KindRelative, 1000, 0)},
		{"-5", pos(KindRelative, -5, 0)},
		{"-0", pos(KindRelative, 0, 0)},
		{"-1000", pos(KindRelative, -1000, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input, testNow)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkSpec(t, tt.input, got, tt.start, nil)
		})
	}
}

func TestParseExactOffsets(t *testing.T) {
	tests := []struct {
		input string
		start Position
	}{
		{"0", pos(KindExact, 0, 0)},
		{"100", pos(KindExact, 100, 0)},
		{"999999999", pos(KindExact, 999999999, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input, testNow)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkSpec(t, tt.input, got, tt.start, nil)
		})
	}
}

func TestParseOffsetRanges(t *testing.T) {
	tests := []struct {
		input string
		start Position
		end   *Position
	}{
		// N: is alias for N
		{"100:", pos(KindExact, 100, 0), nil},

		// :N
		{":200", pos(KindStart, 0, 0), pptr(KindExact, 200, 0)},

		// N:M
		{"100:200", pos(KindExact, 100, 0), pptr(KindExact, 200, 0)},
		{"0:100", pos(KindExact, 0, 0), pptr(KindExact, 100, 0)},

		// N:end
		{"100:end", pos(KindExact, 100, 0), pptr(KindEnd, 0, 0)},
		{"0:end", pos(KindExact, 0, 0), pptr(KindEnd, 0, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input, testNow)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkSpec(t, tt.input, got, tt.start, tt.end)
		})
	}
}

func TestParseUntilEnd(t *testing.T) {
	tests := []struct {
		input string
		start Position
		end   *Position
	}{
		{":end", pos(KindStart, 0, 0), pptr(KindEnd, 0, 0)},
		{":end+5", pos(KindStart, 0, 0), pptr(KindEnd, 0, 5)},
		{":end+0", pos(KindStart, 0, 0), pptr(KindEnd, 0, 0)},
		{":end-3", pos(KindStart, 0, 0), pptr(KindEnd, 0, -3)},
		{":end-0", pos(KindStart, 0, 0), pptr(KindEnd, 0, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input, testNow)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkSpec(t, tt.input, got, tt.start, tt.end)
		})
	}
}

func TestParseTimestampNumeric(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int64
	}{
		{"13-digit millis", "@1700000000000", 1700000000000},
		{"19-digit nanos", "@1700000000000000000", 1700000000000},
		{"10-digit secs", "@1700000000", 1700000000000},
		{"9-digit secs", "@170000000", 170000000000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input, testNow)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkSpec(t, tt.input, got, pos(KindTimestamp, tt.want, 0), nil)
		})
	}
}

func TestParseTimestampDate(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"@2024-01-15", dateMillis(2024, 1, 15, 0, 0, 0)},
		{"@2000-01-01", dateMillis(2000, 1, 1, 0, 0, 0)},
		{"@1970-01-01", dateMillis(1970, 1, 1, 0, 0, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input, testNow)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkSpec(t, tt.input, got, pos(KindTimestamp, tt.want, 0), nil)
		})
	}
}

func TestParseTimestampRFC3339(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int64
	}{
		{
			"basic",
			"@2024-01-15T10:30:00Z",
			dateMillis(2024, 1, 15, 10, 30, 0),
		},
		{
			"fractional seconds",
			"@2024-01-15T10:30:00.500Z",
			time.Date(2024, 1, 15, 10, 30, 0, 500_000_000, time.UTC).UnixMilli(),
		},
		{
			"timezone offset",
			"@2024-01-15T10:30:00+05:30",
			time.Date(2024, 1, 15, 10, 30, 0, 0, time.FixedZone("", 5*3600+30*60)).UnixMilli(),
		},
		{
			"negative timezone offset",
			"@2024-01-15T10:30:00-04:00",
			time.Date(2024, 1, 15, 10, 30, 0, 0, time.FixedZone("", -4*3600)).UnixMilli(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input, testNow)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkSpec(t, tt.input, got, pos(KindTimestamp, tt.want, 0), nil)
		})
	}
}

func TestParseTimestampDuration(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int64
	}{
		{"1 hour ago", "@-1h", testNow.Add(-time.Hour).UnixMilli()},
		{"30 minutes ago", "@-30m", testNow.Add(-30 * time.Minute).UnixMilli()},
		{"7 days ago", "@-7d", testNow.Add(-7 * 24 * time.Hour).UnixMilli()},
		{"2d12h ago", "@-2d12h", testNow.Add(-60 * time.Hour).UnixMilli()},
		{"500ms ago", "@-500ms", testNow.Add(-500 * time.Millisecond).UnixMilli()},
		{"1s ago", "@-1s", testNow.Add(-time.Second).UnixMilli()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input, testNow)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkSpec(t, tt.input, got, pos(KindTimestamp, tt.want, 0), nil)
		})
	}
}

func TestParseTimestampRanges(t *testing.T) {
	jan15 := dateMillis(2024, 1, 15, 0, 0, 0)

	tests := []struct {
		name     string
		input    string
		wantS    Position
		wantE    *Position
	}{
		{
			"date:duration (T2 relative to T1)",
			"@2024-01-15:1h",
			pos(KindTimestamp, jan15, 0),
			pptr(KindTimestamp, jan15+int64(time.Hour/time.Millisecond), 0),
		},
		{
			"date:7d",
			"@2024-01-15:7d",
			pos(KindTimestamp, jan15, 0),
			pptr(KindTimestamp, jan15+int64(7*24*time.Hour/time.Millisecond), 0),
		},
		{
			"-48h:-24h both relative to now",
			"@-48h:-24h",
			pos(KindTimestamp, testNow.Add(-48*time.Hour).UnixMilli(), 0),
			pptr(KindTimestamp, testNow.Add(-24*time.Hour).UnixMilli(), 0),
		},
		{
			"-1m:end",
			"@-1m:end",
			pos(KindTimestamp, testNow.Add(-time.Minute).UnixMilli(), 0),
			pptr(KindEnd, 0, 0),
		},
		{
			"millis:millis range",
			"@1700000000000:1700000001000",
			pos(KindTimestamp, 1700000000000, 0),
			pptr(KindTimestamp, 1700000001000, 0),
		},
		{
			"seconds:seconds range",
			"@1700000000:1700000001",
			pos(KindTimestamp, 1700000000000, 0),
			pptr(KindTimestamp, 1700000001000, 0),
		},
		{
			"RFC3339:duration",
			"@2024-01-15T10:30:00Z:1h",
			pos(KindTimestamp, dateMillis(2024, 1, 15, 10, 30, 0), 0),
			pptr(KindTimestamp, dateMillis(2024, 1, 15, 11, 30, 0), 0),
		},
		{
			"RFC3339 with tz:duration",
			"@2024-01-15T10:30:00+05:30:2h",
			pos(KindTimestamp, time.Date(2024, 1, 15, 10, 30, 0, 0, time.FixedZone("", 5*3600+30*60)).UnixMilli(), 0),
			pptr(KindTimestamp, time.Date(2024, 1, 15, 10, 30, 0, 0, time.FixedZone("", 5*3600+30*60)).Add(2*time.Hour).UnixMilli(), 0),
		},
		{
			":timestamp (from start)",
			"@:2024-01-15",
			pos(KindStart, 0, 0),
			pptr(KindTimestamp, jan15, 0),
		},
		{
			":end (from start to end)",
			"@:end",
			pos(KindStart, 0, 0),
			pptr(KindEnd, 0, 0),
		},
		{
			"trailing colon alias for single",
			"@1700000000000:",
			pos(KindTimestamp, 1700000000000, 0),
			nil,
		},
		{
			"-1h:+1h (T2 relative to T1)",
			"@-1h:+1h",
			pos(KindTimestamp, testNow.Add(-time.Hour).UnixMilli(), 0),
			pptr(KindTimestamp, testNow.UnixMilli(), 0), // T1 + 1h = now
		},
		{
			"date:date range",
			"@2024-01-15:2024-01-16",
			pos(KindTimestamp, jan15, 0),
			pptr(KindTimestamp, dateMillis(2024, 1, 16, 0, 0, 0), 0),
		},
		{
			":-1h (from start to 1h ago)",
			"@:-1h",
			pos(KindStart, 0, 0),
			pptr(KindTimestamp, testNow.Add(-time.Hour).UnixMilli(), 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input, testNow)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkSpec(t, tt.input, got, tt.wantS, tt.wantE)
		})
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"invalid keyword", "invalid"},
		{"empty after @", "@"},
		{"invalid timestamp", "@notadate"},
		{"ambiguous digit count 5", "@12345"},
		{"ambiguous digit count 11", "@12345678901"},
		{"ambiguous digit count 14", "@12345678901234"},
		{"empty colon in @:", "@:"},
		{"invalid range start", "abc:100"},
		{"invalid range end", "100:abc"},
		{"invalid start+", "start+abc"},
		{"invalid end-", "end-abc"},
		{"invalid +N", "+abc"},
		{"invalid -N", "-abc"},
		{"invalid :N", ":abc"},
		{"invalid :end suffix", ":endx"},
		{"invalid :end number", ":end+abc"},
		{"bad timestamp range", "@badtimestamp:badtimestamp"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input, testNow)
			if err == nil {
				t.Errorf("Parse(%q) expected error, got nil", tt.input)
			}
		})
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"1h", time.Hour, false},
		{"30m", 30 * time.Minute, false},
		{"500ms", 500 * time.Millisecond, false},
		{"1s", time.Second, false},
		{"1d", 24 * time.Hour, false},
		{"7d", 7 * 24 * time.Hour, false},
		{"2d12h", 60 * time.Hour, false},
		{"1d30m", 24*time.Hour + 30*time.Minute, false},
		{"0.5d", 12 * time.Hour, false},
		{"1h30m", time.Hour + 30*time.Minute, false},
		{"100ms", 100 * time.Millisecond, false},
		{"1d1h1m1s", 25*time.Hour + time.Minute + time.Second, false},
		{"1d-1h", 23 * time.Hour, false}, // 24h + (-1h) = 23h

		// errors
		{"", 0, true},
		{"abc", 0, true},
		{"d", 0, true},      // no number before d
		{"xd", 0, true},     // invalid day number
		{"1d-abc", 0, true}, // invalid rest after days
		{"1x", 0, true},     // unknown unit
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseDuration(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseDuration(%q) expected error, got %v", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDuration(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("ParseDuration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestPositionKindString(t *testing.T) {
	tests := []struct {
		kind PositionKind
		want string
	}{
		{KindStart, "start"},
		{KindEnd, "end"},
		{KindExact, "exact"},
		{KindTimestamp, "timestamp"},
		{KindRelative, "relative"},
		{PositionKind(99), "PositionKind(99)"},
	}
	for _, tt := range tests {
		if got := tt.kind.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", int(tt.kind), got, tt.want)
		}
	}
}

func checkSpec(t *testing.T, input string, got Spec, wantStart Position, wantEnd *Position) {
	t.Helper()
	if got.Start != wantStart {
		t.Errorf("Parse(%q).Start = %+v, want %+v", input, got.Start, wantStart)
	}
	if wantEnd == nil {
		if got.End != nil {
			t.Errorf("Parse(%q).End = %+v, want nil", input, *got.End)
		}
	} else {
		if got.End == nil {
			t.Errorf("Parse(%q).End = nil, want %+v", input, *wantEnd)
		} else if *got.End != *wantEnd {
			t.Errorf("Parse(%q).End = %+v, want %+v", input, *got.End, *wantEnd)
		}
	}
}
