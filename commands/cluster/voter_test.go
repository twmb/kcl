package cluster

import "testing"

func TestParseDirectoryID(t *testing.T) {
	tests := []struct {
		input string
		want  [16]byte
	}{
		{"", [16]byte{}},
		{"00000000000000000000000000000000", [16]byte{}},
		{"0102030405060708090a0b0c0d0e0f10", [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
		// UUID with dashes stripped.
		{"01020304-0506-0708-090a-0b0c0d0e0f10", [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
	}
	for _, tt := range tests {
		got := parseDirectoryID(tt.input)
		if got != tt.want {
			t.Errorf("parseDirectoryID(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
