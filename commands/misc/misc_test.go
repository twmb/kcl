package misc

import "testing"

func TestSplitVersionGuess(t *testing.T) {
	for _, test := range []struct {
		name     string
		guess    string
		min, max string
	}{
		{"exact", "v3.7", "v3.7", "v3.7"},
		{"between", "between v1.0 and v1.1", "v1.0", "v1.1"},
		{"at least", "at least v4.0", "v4.0", ""},
		{"not even", "not even v0.8.0", "", "v0.8.0"},
		{"custom", "unknown custom version", "", ""},
		{"custom at least", "unknown custom version at least v2.6", "v2.6", ""},
		{"pre api versions", "0.9.0", "0.9.0", "0.9.0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			min, max := splitVersionGuess(test.guess)
			if min != test.min || max != test.max {
				t.Errorf("got %q, %q; want %q, %q", min, max, test.min, test.max)
			}
		})
	}
}
