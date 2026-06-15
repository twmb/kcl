package produce

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/twmb/kcl/serde"
)

// parseSchemaSpec parses a --schema / --key-schema value into a serde.Spec.
//
// Grammar (resolves an existing schema; producing never registers):
//
//	topic[@VERSION]          TopicNameStrategy subject for the produce topic
//	NAME[@VERSION]           a subject (bare)
//	subject:NAME[@VERSION]   an explicit subject (escape hatch for odd names)
//	id:N                     a registered schema id
//
// Any form may carry a trailing #MESSAGE selecting the protobuf message in a
// multi-message schema. VERSION is a positive integer or "latest" (default).
//
// The last '@' separates the version, so a subject whose name itself contains
// '@' cannot be addressed via this spec (pathological; use the registry
// commands directly if you ever hit it).
func parseSchemaSpec(s string) (serde.Spec, error) {
	var spec serde.Spec
	if s == "" {
		return spec, fmt.Errorf("empty schema spec")
	}

	// A trailing #message selects the protobuf message (only meaningful for
	// protobuf schemas; ignored otherwise).
	main, msg, _ := strings.Cut(s, "#")
	spec.Message = msg

	switch {
	case strings.HasPrefix(main, "id:"):
		n, err := strconv.Atoi(main[len("id:"):])
		if err != nil || n <= 0 {
			return spec, fmt.Errorf("invalid %q: want id:N with a positive integer", s)
		}
		spec.ID = n

	case main == "topic" || strings.HasPrefix(main, "topic@"):
		spec.Topic = true
		_, spec.Version = splitVersion(main) // name is the "topic" keyword

	case strings.HasPrefix(main, "subject:"):
		name, ver := splitVersion(main[len("subject:"):])
		if name == "" {
			return spec, fmt.Errorf("invalid %q: subject: requires a name", s)
		}
		spec.Subject, spec.Version = name, ver

	default:
		name, ver := splitVersion(main)
		if name == "" {
			return spec, fmt.Errorf("invalid schema spec %q", s)
		}
		spec.Subject, spec.Version = name, ver
	}
	return spec, nil
}

// splitVersion splits "name@version" on the last '@'. A missing '@' yields an
// empty version (meaning latest).
func splitVersion(s string) (name, version string) {
	if i := strings.LastIndex(s, "@"); i >= 0 {
		return s[:i], s[i+1:]
	}
	return s, ""
}
