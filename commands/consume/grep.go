package consume

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// grepFilter is a single -G filter.
type grepFilter struct {
	negate bool
	match  func(r *kgo.Record) bool
}

// parseGrepFilters parses -G flag values into compiled filters.
// Syntax: [!]prefix:REGEX
// Prefixes: k: (key), v: (value), hk: (header key), hv: (header value),
// h:NAME= (specific header value), t: (topic).
func parseGrepFilters(patterns []string) ([]grepFilter, error) {
	var filters []grepFilter
	for _, p := range patterns {
		f, err := parseOneGrep(p)
		if err != nil {
			return nil, fmt.Errorf("invalid -G %q: %w", p, err)
		}
		filters = append(filters, f)
	}
	return filters, nil
}

func parseOneGrep(pattern string) (grepFilter, error) {
	var f grepFilter
	s := pattern

	if strings.HasPrefix(s, "!") {
		f.negate = true
		s = s[1:]
	}

	// Try each prefix.
	switch {
	case strings.HasPrefix(s, "k:"):
		re, err := regexp.Compile(s[2:])
		if err != nil {
			return f, err
		}
		f.match = func(r *kgo.Record) bool {
			return re.Match(r.Key)
		}

	case strings.HasPrefix(s, "v:"):
		re, err := regexp.Compile(s[2:])
		if err != nil {
			return f, err
		}
		f.match = func(r *kgo.Record) bool {
			return re.Match(r.Value)
		}

	case strings.HasPrefix(s, "hk:"):
		re, err := regexp.Compile(s[3:])
		if err != nil {
			return f, err
		}
		f.match = func(r *kgo.Record) bool {
			for _, h := range r.Headers {
				if re.MatchString(h.Key) {
					return true
				}
			}
			return false
		}

	case strings.HasPrefix(s, "hv:"):
		re, err := regexp.Compile(s[3:])
		if err != nil {
			return f, err
		}
		f.match = func(r *kgo.Record) bool {
			for _, h := range r.Headers {
				if re.Match(h.Value) {
					return true
				}
			}
			return false
		}

	case strings.HasPrefix(s, "h:"):
		// h:NAME=REGEX — match a specific header's value.
		rest := s[2:]
		eqIdx := strings.IndexByte(rest, '=')
		if eqIdx < 0 {
			return f, fmt.Errorf("h: prefix requires NAME=REGEX format")
		}
		name := rest[:eqIdx]
		re, err := regexp.Compile(rest[eqIdx+1:])
		if err != nil {
			return f, err
		}
		f.match = func(r *kgo.Record) bool {
			for _, h := range r.Headers {
				if h.Key == name && re.Match(h.Value) {
					return true
				}
			}
			return false
		}

	case strings.HasPrefix(s, "t:"):
		re, err := regexp.Compile(s[2:])
		if err != nil {
			return f, err
		}
		f.match = func(r *kgo.Record) bool {
			return re.MatchString(r.Topic)
		}

	default:
		return f, fmt.Errorf("unknown prefix; use k:, v:, hk:, hv:, h:NAME=, or t:")
	}

	return f, nil
}

// matchAll returns true if the record matches all filters (AND logic).
func matchAll(filters []grepFilter, r *kgo.Record) bool {
	for _, f := range filters {
		m := f.match(r)
		if f.negate {
			m = !m
		}
		if !m {
			return false
		}
	}
	return true
}
