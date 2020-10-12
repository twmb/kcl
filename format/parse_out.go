package format

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/twmb/frang/pkg/kgo"
	"github.com/twmb/go-strftime"
)

func ParseWriteFormat(format string, escape rune) (func([]byte, *kgo.Record) []byte, error) {
	var argFns []func([]byte, *kgo.Record) []byte
	var pieces [][]byte
	var piece []byte
	var escstr = string(escape) // for error messages
	var calls int64

	for len(format) > 0 {
		char, size := utf8.DecodeRuneInString(format)
		raw := format[:size]
		format = format[size:]
		switch char {
		default:
			piece = append(piece, raw...)

		case '\\':
			c, n, err := parseSlash(format)
			if err != nil {
				return nil, err
			}
			format = format[n:]
			piece = append(piece, c)

		case escape:
			if len(format) == 0 {
				return nil, fmt.Errorf("invalid escape sequence at end of format string")
			}
			nextChar, size := utf8.DecodeRuneInString(format)
			if nextChar == escape || nextChar == '{' {
				piece = append(piece, format[:size]...)
				format = format[size:]
				continue
			}
			openBrace := len(format) > 2 && format[1] == '{'
			var handledBrace bool

			pieces = append(pieces, piece) // always cut the piece, even if empty
			piece = []byte{}

			next := format[0]
			format = format[1:]
			if openBrace {
				format = format[1:]
			}

			switch next {
			case 'T', 'K', 'V', 'H', 'p', 'o', 'e', 'i':
				var numfn func([]byte, int64) []byte
				if handledBrace = openBrace; handledBrace {
					numfn2, n, err := parseWriteSize(format)
					if err != nil {
						return nil, err
					}
					format = format[n:]
					numfn = numfn2
				} else {
					numfn = writeNumAscii
				}
				switch next {
				case 'T':
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return numfn(out, int64(len(r.Topic))) })
				case 'K':
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return numfn(out, int64(len(r.Key))) })
				case 'V':
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return numfn(out, int64(len(r.Value))) })
				case 'H':
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return numfn(out, int64(len(r.Headers))) })
				case 'p':
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return numfn(out, int64(r.Partition)) })
				case 'o':
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return numfn(out, r.Offset) })
				case 'e':
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return numfn(out, int64(r.LeaderEpoch)) })
				case 'i':
					argFns = append(argFns, func(out []byte, _ *kgo.Record) []byte { return numfn(out, atomic.AddInt64(&calls, 1)) })
				}

			case 't', 'k', 'v':
				var appendFn func([]byte, []byte) []byte
				if handledBrace = openBrace; handledBrace {
					switch {
					case strings.HasPrefix(format, "base64}"):
						appendFn = appendBase64
						format = format[len("base64}"):]
					case strings.HasPrefix(format, "hex}"):
						appendFn = appendHex
						format = format[len("hex}"):]
					default:
						return nil, fmt.Errorf("unknown %s%s{ escape", escstr, string(next))
					}
				} else {
					appendFn = appendNormal
				}
				switch next {
				case 't':
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return appendFn(out, []byte(r.Topic)) })
				case 'k':
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return appendFn(out, []byte(r.Key)) })
				case 'v':
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return appendFn(out, []byte(r.Value)) })

				}

			case 'h':
				if !openBrace {
					return nil, fmt.Errorf("missing open brace sequence on %sh signifying how headers are written", escstr)
				}
				handledBrace = true
				braces := 1
				at := 0
				for braces != 0 && len(format[at:]) > 0 {
					switch format[at] {
					case '{':
						braces++
					case '}':
						braces--
					}
					at++
				}
				if braces > 0 {
					return nil, errors.New("invalid header specification: missing closing brace")
				}

				innerfn, err := ParseWriteFormat(format[:at-1], escape)
				format = format[at:]
				if err != nil {
					return nil, fmt.Errorf("invalid header specification: %v", err)
				}
				// Unlike parsing in, we do not care if the
				// specification uses more just %k and %v.
				reuse := new(kgo.Record)
				argFns = append(argFns, func(out []byte, r *kgo.Record) []byte {
					for _, header := range r.Headers {
						reuse.Key = []byte(header.Key)
						reuse.Value = header.Value
						out = innerfn(out, reuse)
					}
					return out
				})

			case 'd':
				if handledBrace = openBrace; handledBrace {
					switch {
					case strings.HasPrefix(format, "strftime"):
						tfmt, rem, err := nomOpenClose(format[len("strftime"):])
						if err != nil {
							return nil, fmt.Errorf("strftime parse err: %v", err)
						}
						if len(rem) == 0 || rem[0] != '}' {
							return nil, fmt.Errorf("%sd{strftime missing closing }", escstr)
						}
						format = rem[1:]
						argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strftime.AppendFormat(out, tfmt, r.Timestamp) })

					case strings.HasPrefix(format, "go"):
						tfmt, rem, err := nomOpenClose(format[len("go"):])
						if err != nil {
							return nil, fmt.Errorf("go parse err: %v", err)
						}
						if len(rem) == 0 || rem[0] != '}' {
							return nil, fmt.Errorf("%sd{go missing closing }", escstr)
						}
						format = rem[1:]
						argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return r.Timestamp.AppendFormat(out, tfmt) })

					default:
						numfn, n, err := parseWriteSize(format)
						if err != nil {
							return nil, fmt.Errorf("unknown %sd{ time specification", escstr)
						}
						format = format[n:]
						argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return numfn(out, int64(r.Timestamp.UnixNano())) })
					}
				} else {
					argFns = append(argFns, func(out []byte, r *kgo.Record) []byte { return strconv.AppendInt(out, r.Timestamp.UnixNano(), 10) })
				}

			default:
				return nil, fmt.Errorf("unknown escape sequence %s%s", escstr, string(format[:1]))
			}

			if openBrace && !handledBrace {
				return nil, fmt.Errorf("unhandled, unknown open brace %q", format)
			}
		}
	}

	if len(piece) > 0 {
		pieces = append(pieces, piece)
		argFns = append(argFns, func(out []byte, _ *kgo.Record) []byte { return out })
	}

	return func(out []byte, r *kgo.Record) []byte {
		for i, piece := range pieces {
			out = append(out, piece...)
			out = argFns[i](out, r)
		}
		return out
	}, nil
}

func appendNormal(dst, src []byte) []byte {
	return append(dst, src...)
}

func appendBase64(dst, src []byte) []byte {
	fin := append(dst, make([]byte, base64.RawStdEncoding.EncodedLen(len(src)))...)
	base64.RawStdEncoding.Encode(fin[len(dst):], src)
	return fin
}

func appendHex(dst, src []byte) []byte {
	fin := append(dst, make([]byte, hex.EncodedLen(len(src)))...)
	hex.Encode(fin[len(dst):], src)
	return fin
}

// nomOpenClose extracts a middle section from a string beginning with repeated
// delimiters and returns it as with remaining (past end delimiters) string.
func nomOpenClose(src string) (string, string, error) {
	if len(src) == 0 {
		return "", "", errors.New("empty format")
	}
	delim := src[0]
	openers := 1
	for openers < len(src) && src[openers] == delim {
		openers++
	}
	switch delim {
	case '{':
		delim = '}'
	case '[':
		delim = ']'
	case '(':
		delim = ')'
	}
	src = src[openers:]
	end := strings.Repeat(string(delim), openers)
	idx := strings.Index(src, end)
	if idx < 0 {
		return "", "", fmt.Errorf("missing end delim %q", end)
	}
	middle := src[:idx]
	return middle, src[idx+len(end):], nil
}

func parseWriteSize(format string) (func([]byte, int64) []byte, int, error) {
	braceEnd := strings.IndexByte(format, '}')
	if braceEnd == -1 {
		return nil, 0, errors.New("missing brace end } to close number size specification")
	}
	end := braceEnd + 1
	switch format = format[:braceEnd]; format {
	case "ascii":
		return writeNumAscii, end, nil
	case "big8", "b8":
		return writeNumB8, end, nil
	case "big4", "b4":
		return writeNumB4, end, nil
	case "big2", "b2":
		return writeNumB2, end, nil
	case "byte", "b":
		return writeNumB, end, nil
	case "little8", "l8":
		return writeNumL8, end, nil
	case "little4", "l4":
		return writeNumL4, end, nil
	case "little2", "l2":
		return writeNumL2, end, nil
	default:
		return nil, 0, fmt.Errorf("invalid output number format %q", format)
	}
}

func writeNumAscii(out []byte, n int64) []byte { return strconv.AppendInt(out, n, 10) }

func writeNumB8(out []byte, n int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(n))
	return append(out, buf[:]...)
}

func writeNumB4(out []byte, n int64) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(n))
	return append(out, buf[:]...)
}

func writeNumB2(out []byte, n int64) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(n))
	return append(out, buf[:]...)
}

func writeNumB(out []byte, n int64) []byte { return append(out, byte(n)) }

func writeNumL8(out []byte, n int64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(n))
	return append(out, buf[:]...)
}

func writeNumL4(out []byte, n int64) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(n))
	return append(out, buf[:]...)
}

func writeNumL2(out []byte, n int64) []byte {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(n))
	return append(out, buf[:]...)
}
