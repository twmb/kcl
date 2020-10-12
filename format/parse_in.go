package format

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/twmb/frang/pkg/kgo"
)

type Reader struct {
	r       io.Reader
	scanner *bufio.Scanner
	scanbuf []byte

	on *kgo.Record
	fn func(*Reader) error

	parseBits
	delimiter *delimiter
	scanmax   int
}

func NewReader(infmt string, escape rune, maxBuf int, reader io.Reader) (*Reader, error) {
	r := &Reader{r: reader, scanmax: maxBuf}
	if err := r.parseReadFormat(infmt, escape); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reader) ParsesTopic() bool {
	return r.parsesTopic()
}

func (r *Reader) Next() (*kgo.Record, error) {
	r.on = new(kgo.Record)
	err := r.fn(r)
	return r.on, err
}

func (r *Reader) SetReader(reader io.Reader) {
	r.r = reader
	if r.delimiter != nil {
		r.scanner = bufio.NewScanner(r.r)
		r.scanner.Buffer(r.scanbuf, r.scanmax)
		r.scanner.Split(r.delimiter.split)
	}
}

type parseBits uint8

func (p *parseBits) setParsesTopic()   { *p = *p | 1 }
func (p *parseBits) setParsesKey()     { *p = *p | 2 }
func (p *parseBits) setParsesValue()   { *p = *p | 4 }
func (p *parseBits) setParsesHeaders() { *p = *p | 8 }

func (p parseBits) parsesTopic() bool   { return p&1 != 0 }
func (p parseBits) parsesKey() bool     { return p&2 != 0 }
func (p parseBits) parsesValue() bool   { return p&4 != 0 }
func (p parseBits) parsesHeaders() bool { return p&8 != 0 }

func (r *Reader) parseReadFormat(format string, escape rune) error {
	var (
		// If we see any sized fields, we ensure that the size comes
		// before the field with sawXyz. Additionally, we ensure that
		// if we see a sized field, all fields are sized.
		sized         bool
		sawTopicSize  bool
		sawKeySize    bool
		sawValueSize  bool
		sawHeadersNum bool

		// If we are using sizes, we first read the size into a number,
		// and then later read that number of bytes. We need to capture
		// the first read into outer variables.
		topicSize  uint64
		keySize    uint64
		valueSize  uint64
		headersNum uint64

		// Until the end, we build both sized fns and delim fns.
		sizeFns  []func(*Reader) error
		delimFns []func([]byte, *kgo.Record)

		// Pieces contains intermediate bytes to read. When using
		// sizing, these should generally be empty. When using
		// delimiters, these correspond to our delimiters.
		pieces [][]byte
		// Piece is the delim we are currently working on.
		piece []byte

		escstr = string(escape) // for error messages
	)

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
				return err
			}
			format = format[n:]
			piece = append(piece, c)

		case escape:
			if len(format) == 0 {
				return fmt.Errorf("invalid escape sequence at end of format string")
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

			parseSize := func(dst *uint64, letter string) error {
				var fn func(*Reader) error
				var n int
				var err error
				if handledBrace = openBrace; handledBrace {
					fn, n, err = parseReadSize(format, dst, true)
				} else {
					fn, _, err = parseReadSize("ascii", dst, false)
				}
				if err != nil {
					return fmt.Errorf("unable to parse %s%s: %s", escstr, letter, err)
				}
				format = format[n:]
				sizeFns = append(sizeFns, fn)
				return nil
			}

			switch next {

			case 'T':
				sized, sawTopicSize = true, true
				if err := parseSize(&topicSize, "T"); err != nil {
					return err
				}

			case 't':
				r.setParsesTopic()
				delimFns = append(delimFns, func(in []byte, r *kgo.Record) { r.Topic = string(in) })
				if sized {
					if !sawTopicSize {
						return fmt.Errorf("missing topic size parsing %[1]sT before topic parsing %[1]st", escstr)
					}
					sizeFns = append(sizeFns, func(r *Reader) error {
						buf := make([]byte, topicSize)
						_, err := io.ReadFull(r.r, buf)
						r.on.Topic = string(buf)
						return err
					})
				}

			case 'K':
				sized, sawKeySize = true, true
				if err := parseSize(&keySize, "K"); err != nil {
					return err
				}

			case 'k':
				r.setParsesKey()
				delimFns = append(delimFns, func(in []byte, r *kgo.Record) { r.Key = in })
				if sized {
					if !sawKeySize {
						return fmt.Errorf("missing key size parsing %[1]sK before key parsing %[1]sk", escstr)
					}
					sizeFns = append(sizeFns, func(r *Reader) error {
						r.on.Key = make([]byte, keySize)
						_, err := io.ReadFull(r.r, r.on.Key)
						return err
					})
				}

			case 'V':
				sized, sawValueSize = true, true
				if err := parseSize(&valueSize, "V"); err != nil {
					return err
				}

			case 'v':
				r.setParsesValue()
				delimFns = append(delimFns, func(in []byte, r *kgo.Record) { r.Value = in })
				if sized {
					if !sawValueSize {
						return fmt.Errorf("missing value size parsing %[1]sV before value parsing %[1]sv", escstr)
					}
					sizeFns = append(sizeFns, func(r *Reader) error {
						r.on.Value = make([]byte, valueSize)
						_, err := io.ReadFull(r.r, r.on.Value)
						return err
					})
				}

			case 'H':
				sized, sawHeadersNum = true, true
				if err := parseSize(&headersNum, "H"); err != nil {
					return err
				}

			case 'h':
				if !sized {
					return errors.New("headers are only supported with sized specifications")
				}
				if !sawHeadersNum {
					return fmt.Errorf("missing header count num %[1]sH before header parsing %[1]sh", escstr)
				}
				if !openBrace {
					return fmt.Errorf("missing open brace sequence on %sh signifying how headers are encoded", escstr)
				}
				handledBrace = true
				r.setParsesHeaders()
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
					return errors.New("invalid header specification: missing closing brace")
				}

				inr := &Reader{r: r.r, on: new(kgo.Record)}
				if err := inr.parseReadFormat(format[:at-1], escape); err != nil {
					return fmt.Errorf("invalid header specification: %v", err)
				}
				format = format[at:]
				if inr.parsesTopic() || inr.parsesHeaders() {
					return errors.New("invalid header specification: internally specifies more than just a key and a value")
				}
				if inr.delimiter != nil {
					return errors.New("invalid header specification: internally uses delimiters, not sized fields")
				}
				sizeFns = append(sizeFns, func(r *Reader) error {
					for i := uint64(0); i < headersNum; i++ {
						if err := inr.fn(inr); err != nil {
							return err
						}
						r.on.Headers = append(r.on.Headers, kgo.RecordHeader{Key: string(inr.on.Key), Value: inr.on.Value})
					}
					return nil
				})

			default:
				return fmt.Errorf("unknown percent escape sequence %q", format[:1])
			}

			if openBrace && !handledBrace {
				return fmt.Errorf("unhandled, unknown open brace %q", format)
			}
		}
	}

	if sized {
		if r.parsesTopic() && !sawTopicSize ||
			r.parsesKey() && !sawKeySize ||
			r.parsesValue() && !sawValueSize ||
			r.parsesHeaders() && !sawHeadersNum {
			return errors.New("invalid mix of sized fields and unsized fields")
		}
		if sawTopicSize && !r.parsesTopic() ||
			sawKeySize && !r.parsesKey() ||
			sawValueSize && !r.parsesValue() ||
			sawHeadersNum && !r.parsesHeaders() {
			return errors.New("saw a field size specification without corresponding field")
		}

		if len(piece) > 0 {
			pieces = append(pieces, piece)
			sizeFns = append(sizeFns, func(*Reader) error { return nil })
		}

		r.fn = func(r *Reader) error {
			for i, piece := range pieces {
				if len(piece) > 0 {
					if _, err := io.ReadFull(r.r, piece); err != nil {
						return fmt.Errorf("unable to read piece: %v", err)
					}
				}
				if err := sizeFns[i](r); err != nil {
					return err
				}
			}
			return nil
		}

	} else {
		var leadingDelim bool
		if len(pieces) > 0 {
			if len(pieces[0]) != 0 {
				leadingDelim = true
				delimFns = append([]func([]byte, *kgo.Record){nil}, delimFns...)
			} else {
				pieces = pieces[1:]
			}
		}
		if len(piece) == 0 {
			return errors.New("invalid line missing trailing delimiter")
		}
		pieces = append(pieces, piece)
		d := &delimiter{delims: pieces}

		r.scanner = bufio.NewScanner(r.r)
		r.scanbuf = make([]byte, 0, r.scanmax)
		r.scanner.Buffer(r.scanbuf, r.scanmax)
		r.scanner.Split(d.split)
		r.delimiter = d
		r.fn = func(r *Reader) error {
			var scanned int
			for r.scanner.Scan() {
				if scanned == 0 && leadingDelim {
					if len(r.scanner.Bytes()) > 0 {
						return fmt.Errorf("invalid content %q before leading delimeter", r.scanner.Bytes())
					}
				} else {
					val := make([]byte, len(r.scanner.Bytes()))
					copy(val, r.scanner.Bytes())
					delimFns[scanned](val, r.on)
				}
				scanned++
				if scanned == len(d.delims) {
					return nil
				}
			}
			if r.scanner.Err() != nil {
				return r.scanner.Err()
			}
			return io.EOF
		}
	}

	return nil
}

type delimiter struct {
	delims  [][]byte
	atDelim int
}

func (d *delimiter) split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	delim := d.delims[d.atDelim]
	if i := bytes.Index(data, delim); i >= 0 {
		d.atDelim++
		if d.atDelim == len(d.delims) {
			d.atDelim = 0
		}
		return i + len(delim), data[0:i], nil
	}
	if atEOF {
		return 0, nil, fmt.Errorf("unfinished delim %q", delim)
	}
	return 0, nil, nil
}

func parseReadSize(format string, dst *uint64, needBrace bool) (func(*Reader) error, int, error) {
	var end int
	if needBrace {
		braceEnd := strings.IndexByte(format, '}')
		if braceEnd == -1 {
			return nil, 0, errors.New("missing brace end } to close number size specification")
		}
		format = format[:braceEnd]
		end = braceEnd + 1
	}

	var buf [8]byte
	switch format {
	case "b8", "big8":
		return func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			*dst = binary.BigEndian.Uint64(buf[:])
			return nil
		}, end, nil

	case "b4", "big4":
		return func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:4]); err != nil {
				return err
			}
			*dst = uint64(binary.BigEndian.Uint32(buf[:]))
			return nil
		}, end, nil

	case "b2", "big2":
		return func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:2]); err != nil {
				return err
			}
			*dst = uint64(binary.BigEndian.Uint16(buf[:]))
			return nil
		}, end, nil

	case "byte", "b":
		return func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:1]); err != nil {
				return err
			}
			*dst = uint64(buf[0])
			return nil
		}, end, nil

	case "l8", "little8":
		return func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			*dst = binary.LittleEndian.Uint64(buf[:])
			return nil
		}, end, nil

	case "l4", "little4":
		return func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:4]); err != nil {
				return err
			}
			*dst = uint64(binary.LittleEndian.Uint32(buf[:]))
			return nil
		}, end, nil

	case "l2", "little2":
		return func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:2]); err != nil {
				return err
			}
			*dst = uint64(binary.LittleEndian.Uint16(buf[:]))
			return nil
		}, end, nil

	case "ascii", "a":
		return func(r *Reader) error {
			peeker, ok := r.r.(bytePeeker)
			if !ok {
				r.r = &bytePeekWrapper{r: r.r}
				peeker = r.r.(bytePeeker)
			}
			rawNum := make([]byte, 0, 20)
			for i := 0; i < 21; i++ {
				next, err := peeker.Peek()
				if err != nil || next < '0' || next > '9' {
					if err != nil && len(rawNum) == 0 {
						return err
					}
					break
				}
				if i == 20 {
					return fmt.Errorf("still parsing ascii number in %s past max uint64 possible length of 20", rawNum)
				}
				rawNum = append(rawNum, next)
				peeker.SkipPeek()
			}
			parsed, err := strconv.ParseUint(string(rawNum), 10, 64)
			if err != nil {
				return err
			}
			*dst = parsed
			return nil
		}, end, nil

	default:
		num, err := strconv.Atoi(format)
		if err != nil {
			return nil, 0, fmt.Errorf("unrecognized number reading format %q", format)
		}
		if num <= 0 {
			return nil, 0, fmt.Errorf("invalid zero or negative number %q", format)
		}
		return func(r *Reader) error { *dst = uint64(num); return nil }, end, nil
	}
}

type bytePeeker interface {
	Peek() (byte, error)
	SkipPeek()
	io.Reader
}

type bytePeekWrapper struct {
	haspeek bool
	peek    byte
	r       io.Reader
}

func (b *bytePeekWrapper) Peek() (byte, error) {
	if b.haspeek {
		return b.peek, nil
	}
	var peek [1]byte
	_, err := io.ReadFull(b.r, peek[:])
	if err != nil {
		return 0, err
	}
	b.haspeek = true
	b.peek = peek[0]
	return b.peek, nil
}

func (b *bytePeekWrapper) SkipPeek() {
	b.haspeek = false
}

func (b *bytePeekWrapper) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.haspeek {
		b.haspeek = false
		p[n] = b.peek
		n++
	}
	nn, err := b.r.Read(p[n:])
	return n + nn, err
}
