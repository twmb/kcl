package group

import "testing"

func TestValidateSection(t *testing.T) {
	tests := []struct {
		section string
		wantErr bool
	}{
		{"", false},
		{"summary", false},
		{"lag", false},
		{"members", false},
		{"invalid", true},
		{"Summary", true},
		{"LAG", true},
	}

	for _, tt := range tests {
		t.Run(tt.section, func(t *testing.T) {
			err := validateSection(tt.section)
			if tt.wantErr && err == nil {
				t.Errorf("validateSection(%q) expected error", tt.section)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("validateSection(%q) unexpected error: %v", tt.section, err)
			}
		})
	}
}
