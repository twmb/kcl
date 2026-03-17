// Package offsetparse parses offset and timestamp specifications used by
// consume --offset, group seek --to, share-group seek --to, and
// topic trim-prefix --offset.
//
// The parser converts syntax into structured results. It resolves timestamps
// and durations to milliseconds but does NOT resolve offsets (that requires
// Kafka API calls like ListOffsets, which the caller performs).
package offsetparse

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PositionKind describes what kind of offset position this is.
type PositionKind int

const (
	KindStart     PositionKind = iota // "start" keyword
	KindEnd                           // "end" keyword
	KindExact                         // exact numeric offset
	KindTimestamp                     // resolved timestamp in milliseconds
	KindRelative                      // +N / -N relative delta (caller interprets)
)

func (k PositionKind) String() string {
	switch k {
	case KindStart:
		return "start"
	case KindEnd:
		return "end"
	case KindExact:
		return "exact"
	case KindTimestamp:
		return "timestamp"
	case KindRelative:
		return "relative"
	default:
		return fmt.Sprintf("PositionKind(%d)", int(k))
	}
}

// Position represents a single offset position.
type Position struct {
	Kind  PositionKind
	Value int64 // offset for Exact, millis for Timestamp, signed delta for Relative
	Delta int64 // additional relative delta for Start/End (e.g., start+5, end-3, :end+2)
}

// Spec is the parsed result of an offset specification.
type Spec struct {
	Start Position
	End   *Position // nil means no end bound
}

// Parse parses an offset specification string. The now parameter is used
// to resolve relative durations (e.g., -1h means 1 hour before now).
//
// Offset syntax:
//
//	start, end                  partition boundaries
//	start+N, end-N              boundary with delta
//	+N, -N                      relative offset (caller-dependent)
//	N                           exact offset
//	N:, :N, N:M, N:end          offset ranges
//	:end, :end+N, :end-N        until-end ranges
//
// Timestamp syntax (@ prefix):
//
//	@T                          single timestamp
//	@T:, @:T, @T1:T2           timestamp ranges
//
// Timestamp T can be: 19-digit nanoseconds, 13-digit milliseconds,
// 9-10 digit seconds, YYYY-MM-DD (UTC), RFC3339, -duration, +duration.
// Duration units: ns, us, ms, s, m, h, d.
func Parse(s string, now time.Time) (Spec, error) {
	if len(s) == 0 {
		return Spec{}, fmt.Errorf("empty offset specification")
	}
	if strings.HasPrefix(s, "@") {
		return parseTimestampSpec(s[1:], now)
	}
	return parseOffsetSpec(s)
}

func parseOffsetSpec(s string) (Spec, error) {
	switch s {
	case "start":
		return Spec{Start: Position{Kind: KindStart}}, nil
	case "end":
		return Spec{Start: Position{Kind: KindEnd}}, nil
	}

	// :end, :end+N, :end-N
	if strings.HasPrefix(s, ":end") {
		return parseUntilEnd(s)
	}

	// :N (until exact offset)
	if s[0] == ':' {
		n, err := strconv.ParseInt(s[1:], 10, 64)
		if err != nil {
			return Spec{}, fmt.Errorf("invalid until-offset %q: %w", s, err)
		}
		return Spec{
			Start: Position{Kind: KindStart},
			End:   &Position{Kind: KindExact, Value: n},
		}, nil
	}

	// start+N
	if strings.HasPrefix(s, "start+") {
		v, err := strconv.ParseInt(s[6:], 10, 64)
		if err != nil {
			return Spec{}, fmt.Errorf("invalid start+offset %q: %w", s, err)
		}
		return Spec{Start: Position{Kind: KindStart, Delta: v}}, nil
	}

	// end-N
	if strings.HasPrefix(s, "end-") {
		v, err := strconv.ParseInt(s[4:], 10, 64)
		if err != nil {
			return Spec{}, fmt.Errorf("invalid end-offset %q: %w", s, err)
		}
		return Spec{Start: Position{Kind: KindEnd, Delta: -v}}, nil
	}

	// +N (relative forward)
	if s[0] == '+' {
		v, err := strconv.ParseInt(s[1:], 10, 64)
		if err != nil {
			return Spec{}, fmt.Errorf("invalid relative offset %q: %w", s, err)
		}
		return Spec{Start: Position{Kind: KindRelative, Value: v}}, nil
	}

	// -N (relative backward)
	if s[0] == '-' {
		v, err := strconv.ParseInt(s[1:], 10, 64)
		if err != nil {
			return Spec{}, fmt.Errorf("invalid relative offset %q: %w", s, err)
		}
		return Spec{Start: Position{Kind: KindRelative, Value: -v}}, nil
	}

	// N:M, N:end, N:, or plain N
	if colonIdx := strings.IndexByte(s, ':'); colonIdx >= 0 {
		startStr := s[:colonIdx]
		endStr := s[colonIdx+1:]

		startN, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			return Spec{}, fmt.Errorf("invalid range start %q in %q: %w", startStr, s, err)
		}

		// N: is alias for N
		if endStr == "" {
			return Spec{Start: Position{Kind: KindExact, Value: startN}}, nil
		}

		if endStr == "end" {
			return Spec{
				Start: Position{Kind: KindExact, Value: startN},
				End:   &Position{Kind: KindEnd},
			}, nil
		}

		endN, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			return Spec{}, fmt.Errorf("invalid range end %q in %q: %w", endStr, s, err)
		}
		return Spec{
			Start: Position{Kind: KindExact, Value: startN},
			End:   &Position{Kind: KindExact, Value: endN},
		}, nil
	}

	// Plain N
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return Spec{}, fmt.Errorf("unable to parse offset %q: %w", s, err)
	}
	return Spec{Start: Position{Kind: KindExact, Value: n}}, nil
}

func parseUntilEnd(s string) (Spec, error) {
	if s == ":end" {
		return Spec{
			Start: Position{Kind: KindStart},
			End:   &Position{Kind: KindEnd},
		}, nil
	}

	rest := s[4:] // after ":end"
	if len(rest) < 2 {
		return Spec{}, fmt.Errorf("invalid :end offset %q: expected :end, :end+N, or :end-N", s)
	}

	op := rest[0]
	if op != '+' && op != '-' {
		return Spec{}, fmt.Errorf("invalid :end offset %q: expected + or - after :end", s)
	}

	v, err := strconv.ParseInt(rest[1:], 10, 64)
	if err != nil {
		return Spec{}, fmt.Errorf("invalid :end offset %q: %w", s, err)
	}

	delta := v
	if op == '-' {
		delta = -v
	}
	return Spec{
		Start: Position{Kind: KindStart},
		End:   &Position{Kind: KindEnd, Delta: delta},
	}, nil
}

func parseTimestampSpec(s string, now time.Time) (Spec, error) {
	if len(s) == 0 {
		return Spec{}, fmt.Errorf("empty timestamp after @")
	}

	// @:T → from start to timestamp T
	if s[0] == ':' {
		if len(s) == 1 {
			return Spec{}, fmt.Errorf("empty end timestamp in @:")
		}
		pos, err := parseTimestamp(s[1:], nil, now)
		if err != nil {
			return Spec{}, fmt.Errorf("invalid end timestamp in @:%s: %w", s[1:], err)
		}
		return Spec{
			Start: Position{Kind: KindStart},
			End:   &pos,
		}, nil
	}

	// Try parsing the whole string as a single timestamp.
	if pos, err := parseTimestamp(s, nil, now); err == nil {
		return Spec{Start: pos}, nil
	}

	// Try splitting at colons from right to left to find the range separator.
	// This handles RFC3339 timestamps that contain colons (e.g., 10:30:00Z)
	// by preferring the rightmost valid split point.
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] != ':' {
			continue
		}

		t1Str := s[:i]
		t2Str := s[i+1:]

		if t1Str == "" {
			continue
		}

		t1, err := parseTimestamp(t1Str, nil, now)
		if err != nil {
			continue
		}

		// @T: is alias for @T
		if t2Str == "" {
			return Spec{Start: t1}, nil
		}

		// For T2: positive durations are relative to T1 if T1 is a
		// concrete timestamp. Negative durations are always relative to now.
		var relativeTo *int64
		if t1.Kind == KindTimestamp {
			relativeTo = &t1.Value
		}

		t2, err := parseTimestamp(t2Str, relativeTo, now)
		if err != nil {
			continue
		}

		return Spec{Start: t1, End: &t2}, nil
	}

	return Spec{}, fmt.Errorf("unable to parse timestamp specification @%s", s)
}

func parseTimestamp(s string, relativeTo *int64, now time.Time) (Position, error) {
	if len(s) == 0 {
		return Position{}, fmt.Errorf("empty timestamp")
	}

	// "end" keyword (valid as T2 in ranges)
	if s == "end" {
		return Position{Kind: KindEnd}, nil
	}

	// Negative duration: -1h, -7d, -30m, etc. Always relative to now.
	if s[0] == '-' {
		d, err := ParseDuration(s[1:])
		if err == nil {
			return Position{Kind: KindTimestamp, Value: now.Add(-d).UnixMilli()}, nil
		}
	}

	// Explicit positive duration with + prefix: +1h. Relative to T1 if
	// available, otherwise relative to now.
	if s[0] == '+' {
		d, err := ParseDuration(s[1:])
		if err == nil {
			if relativeTo != nil {
				return Position{Kind: KindTimestamp, Value: time.UnixMilli(*relativeTo).Add(d).UnixMilli()}, nil
			}
			return Position{Kind: KindTimestamp, Value: now.Add(d).UnixMilli()}, nil
		}
	}

	// Numeric timestamps by digit count.
	if isAllDigits(s) {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return Position{}, fmt.Errorf("invalid numeric timestamp %q: %w", s, err)
		}
		switch l := len(s); {
		case l == 19: // nanoseconds
			return Position{Kind: KindTimestamp, Value: n / 1_000_000}, nil
		case l == 13: // milliseconds
			return Position{Kind: KindTimestamp, Value: n}, nil
		case l >= 9 && l <= 10: // seconds
			return Position{Kind: KindTimestamp, Value: n * 1000}, nil
		default:
			return Position{}, fmt.Errorf("ambiguous numeric timestamp %q: expected 9-10 digits (seconds), 13 digits (milliseconds), or 19 digits (nanoseconds)", s)
		}
	}

	// YYYY-MM-DD date (UTC)
	if len(s) == 10 && s[4] == '-' && s[7] == '-' {
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return Position{}, fmt.Errorf("invalid date %q: %w", s, err)
		}
		return Position{Kind: KindTimestamp, Value: t.UnixMilli()}, nil
	}

	// RFC3339 (with optional fractional seconds)
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return Position{Kind: KindTimestamp, Value: t.UnixMilli()}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return Position{Kind: KindTimestamp, Value: t.UnixMilli()}, nil
	}

	// Bare positive duration without + prefix: 1h, 30m, 7d.
	// As T2 in a range: relative to T1 if available.
	// As T1: relative to now (unusual but valid).
	if d, err := ParseDuration(s); err == nil {
		if relativeTo != nil {
			return Position{Kind: KindTimestamp, Value: time.UnixMilli(*relativeTo).Add(d).UnixMilli()}, nil
		}
		return Position{Kind: KindTimestamp, Value: now.Add(d).UnixMilli()}, nil
	}

	return Position{}, fmt.Errorf("unable to parse timestamp %q", s)
}

// ParseDuration parses a duration string, extending Go's time.ParseDuration
// with support for 'd' (days, where 1d = 24h). Composite durations like
// "2d12h30m" are supported.
func ParseDuration(s string) (time.Duration, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("empty duration")
	}
	if !strings.Contains(s, "d") {
		return time.ParseDuration(s)
	}

	dIdx := strings.IndexByte(s, 'd')
	dayStr := s[:dIdx]
	rest := s[dIdx+1:]

	if dayStr == "" {
		return 0, fmt.Errorf("invalid duration %q: no number before 'd'", s)
	}

	days, err := strconv.ParseFloat(dayStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid day value in duration %q: %w", s, err)
	}

	total := time.Duration(days * 24 * float64(time.Hour))
	if rest != "" {
		d, err := time.ParseDuration(rest)
		if err != nil {
			return 0, fmt.Errorf("invalid duration after days in %q: %w", s, err)
		}
		total += d
	}
	return total, nil
}

func isAllDigits(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
