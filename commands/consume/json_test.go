package consume

import "testing"

// TestEncodeComponent pins the three-way split that the JSON output depends
// on: a nil component is JSON null, an empty-but-present one is "", and bytes
// that are not valid UTF-8 go to base64 rather than through a Go string, which
// would replace them with U+FFFD.
//
// The nil/empty distinction is not reachable through kcl's own produce (it
// sends nil for an empty value), but it is on the wire -- the record-batch
// decode returns nil for a null component and a non-nil zero-length slice for
// an empty one -- so a decoder must not collapse them.
func TestEncodeComponent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      []byte
		decoded bool
		wantRaw string
		wantB64 string
	}{
		{name: "nil is null", in: nil, wantRaw: "null"},
		{name: "empty is empty string", in: []byte{}, wantRaw: `""`},
		{name: "text", in: []byte("hello"), wantRaw: `"hello"`},
		{name: "text needing escapes", in: []byte(`a"b` + "\n"), wantRaw: `"a\"b\n"`},
		{name: "valid utf8 control bytes stay a string", in: []byte{0, 1, 2}, wantRaw: `"\u0000\u0001\u0002"`},
		{name: "invalid utf8 goes base64", in: []byte{0xff, 0xfe, 0xfd}, wantB64: "//79"},
		{name: "decoded json embeds raw", in: []byte(`{"a":1}`), decoded: true, wantRaw: `{"a":1}`},
		{name: "decoded but not json falls back", in: []byte("not json"), decoded: true, wantRaw: `"not json"`},
		{name: "decoded nil is still null", in: nil, decoded: true, wantRaw: "null"},
		{name: "not decoded json stays a string", in: []byte(`{"a":1}`), wantRaw: `"{\"a\":1}"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, b64 := encodeComponent(tc.in, tc.decoded)
			if got := string(raw); got != tc.wantRaw {
				t.Errorf("raw = %q, want %q", got, tc.wantRaw)
			}
			if b64 != tc.wantB64 {
				t.Errorf("base64 = %q, want %q", b64, tc.wantB64)
			}
			// Exactly one of the two is ever set, so a consumer can
			// branch on which field is present.
			if (len(raw) == 0) == (b64 == "") {
				t.Errorf("expected exactly one of raw/base64, got raw=%q base64=%q", raw, b64)
			}
		})
	}
}
