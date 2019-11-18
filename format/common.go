package format

import (
	"errors"
	"fmt"
	"strconv"
)

func parseSlash(format string) (byte, int, error) {
	if len(format) == 0 {
		return 0, 0, errors.New("invalid slash escape at end of delim string")
	}
	switch format[0] {
	case 't':
		return '\t', 1, nil
	case 'n':
		return '\n', 1, nil
	case 'r':
		return '\r', 1, nil
	case '\\':
		return '\\', 1, nil
	case 'x':
		if len(format) < 3 { // on x, need two more
			return 0, 0, errors.New("invalid non-terminated hex escape sequence at end of delim string")
		}
		hex := format[1:3]
		n, err := strconv.ParseInt(hex, 16, 8)
		if err != nil {
			return 0, 0, fmt.Errorf("unable to parse hex escape sequence %q: %v", hex, err)
		}
		return byte(n), 3, nil
	default:
		return 0, 0, fmt.Errorf("unknown slash escape sequence %q", format[:1])
	}
}
