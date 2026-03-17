package group

import "testing"

func TestParseLagFilter(t *testing.T) {
	tests := []struct {
		expr    string
		lag     int64
		want    bool
		wantErr bool
	}{
		// >N
		{">0", 1, true, false},
		{">0", 0, false, false},
		{">100", 50, false, false},
		{">100", 101, true, false},

		// >=N
		{">=0", 0, true, false},
		{">=100", 100, true, false},
		{">=100", 99, false, false},

		// <N
		{"<100", 50, true, false},
		{"<100", 100, false, false},

		// <=N
		{"<=100", 100, true, false},
		{"<=100", 101, false, false},

		// =N
		{"=0", 0, true, false},
		{"=0", 1, false, false},
		{"=100", 100, true, false},

		// Bare number (treated as >=N)
		{"1000", 1000, true, false},
		{"1000", 999, false, false},

		// Empty (nil function)
		{"", 0, false, false},

		// Errors
		{"abc", 0, false, true},
		{">abc", 0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			fn, err := parseLagFilter(tt.expr)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseLagFilter(%q) expected error", tt.expr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLagFilter(%q) unexpected error: %v", tt.expr, err)
			}
			if fn == nil {
				// Empty expr returns nil function.
				if tt.expr != "" {
					t.Errorf("parseLagFilter(%q) returned nil", tt.expr)
				}
				return
			}
			got := fn(tt.lag)
			if got != tt.want {
				t.Errorf("parseLagFilter(%q)(%d) = %v, want %v", tt.expr, tt.lag, got, tt.want)
			}
		})
	}
}
