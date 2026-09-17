package cluster

import (
	"io"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

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
		got, err := parseDirectoryID(tt.input)
		if err != nil {
			t.Errorf("parseDirectoryID(%q) unexpected error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("parseDirectoryID(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

// TestControllerResult pins the one row an add or remove prints, against
// hand built responses, since kfake answers neither request: the controller
// id, the bare error name, the broker's text, and exit 1 on an error.
func TestControllerResult(t *testing.T) {
	msg := "voter 3 is not a member"
	for _, test := range []struct {
		name    string
		code    int16
		message *string
		want    string
		wantErr bool
	}{
		{"ok", 0, nil, "3\t-\t-\n", false},
		{"error", kerr.VoterNotFound.Code, &msg, "3\tVOTER_NOT_FOUND\tvoter 3 is not a member\n", true},
		{"error, no message", kerr.VoterNotFound.Code, nil, "3\tVOTER_NOT_FOUND\t-\n", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := &cobra.Command{Use: "kcl"}
			cl := client.New(root)
			if err := root.ParseFlags([]string{"--no-config-file", "--format", "awk"}); err != nil {
				t.Fatal(err)
			}

			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			old := os.Stdout
			os.Stdout = w
			resErr := controllerResult(cl, 3, test.code, test.message)
			w.Close()
			os.Stdout = old
			b, _ := io.ReadAll(r)
			if got := string(b); got != test.want {
				t.Errorf("row = %q, want %q", got, test.want)
			}
			if (resErr == out.ErrSilent) != test.wantErr || (resErr != nil && resErr != out.ErrSilent) {
				t.Errorf("err = %v, want ErrSilent %v", resErr, test.wantErr)
			}
		})
	}
}
