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

	"github.com/twmb/kafka-go/pkg/kgo"
)

type Reader struct {
	r       io.Reader
	scanner *bufio.Scanner

	on *kgo.Record
	fn func(*Reader) error

	parseBits
	usesDelims bool
}

func NewReader(infmt string, escape byte, reader io.Reader) (*Reader, error) {
	r := &Reader{r: reader}
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

type parseBits uint8

func (p *parseBits) setParsesTopic()   { *p = *p | 1 }
func (p *parseBits) setParsesKey()     { *p = *p | 2 }
func (p *parseBits) setParsesValue()   { *p = *p | 4 }
func (p *parseBits) setParsesHeaders() { *p = *p | 8 }

func (p parseBits) parsesTopic() bool   { return p&1 != 0 }
func (p parseBits) parsesKey() bool     { return p&2 != 0 }
func (p parseBits) parsesValue() bool   { return p&4 != 0 }
func (p parseBits) parsesHeaders() bool { return p&8 != 0 }

func (r *Reader) parseReadFormat(format string, escape byte) error {
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
		b := format[0]
		format = format[1:]
		switch b {
		default:
			piece = append(piece, b)

		case '\\':
			if len(format) == 0 {
				return errors.New("invalid slash escape at end of delim string")
			}
			switch format[0] {
			case 't':
				piece = append(piece, '\t')
			case 'n':
				piece = append(piece, '\n')
			case 'r':
				piece = append(piece, '\r')
			case 'x':
				if len(format) < 3 { // on x, need two more
					return errors.New("invalid non-terminated hex escape sequence at end of delim string")
				}
				hex := format[1:3]
				n, err := strconv.ParseInt(hex, 16, 8)
				if err != nil {
					return fmt.Errorf("unable to parse hex escape sequence %q: %v", hex, err)
				}
				piece = append(piece, byte(n))
				format = format[2:] // two here, one below
			default:
				return fmt.Errorf("unknown slash escape sequence %q", format[:1])
			}
			format = format[1:]

		case escape:
			if len(format) == 0 {
				return fmt.Errorf("invalid escape sequence at end of format string")
			}
			if format[0] == escape {
				piece = append(piece, escape)
				format = format[1:]
				continue
			}
			openBrace := len(format) > 2 && format[1] == '{'
			var handledBrace bool

			// Always cut the piece, even if it is empty. We alternate piece, fn.
			pieces = append(pieces, piece)
			piece = []byte{}

			next := format[0]
			format = format[1:]
			if openBrace {
				format = format[1:]
			}

			switch next {

			case 'T':
				sized, sawTopicSize = true, true
				if handledBrace = openBrace; handledBrace {
					n, fn, err := parseReadSize(format, &topicSize)
					if err != nil {
						return fmt.Errorf("unable to parse %sT: %s", escstr, err)
					}
					format = format[n:]
					sizeFns = append(sizeFns, fn)
				} else {
					return fmt.Errorf("missing open brace sequence on %sT signifying how the topic size is encoded", escstr)
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
				if handledBrace = openBrace; handledBrace {
					n, fn, err := parseReadSize(format, &keySize)
					if err != nil {
						return fmt.Errorf("unable to parse %sK: %s", escstr, err)
					}
					format = format[n:]
					sizeFns = append(sizeFns, fn)
				} else {
					return fmt.Errorf("missing open brace sequence on %sK signifying how the key size is encoded", escstr)
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
				if handledBrace = openBrace; handledBrace {
					n, fn, err := parseReadSize(format, &valueSize)
					if err != nil {
						return fmt.Errorf("unable to parse %sV: %s", escstr, err)
					}
					format = format[n:]
					sizeFns = append(sizeFns, fn)
				} else {
					return fmt.Errorf("missing open brace sequence on %sV signifying how the value size is encoded", escstr)
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
				if handledBrace = openBrace; handledBrace {
					n, fn, err := parseReadSize(format, &headersNum)
					if err != nil {
						return fmt.Errorf("unable to parse %sH: %s", escstr, err)
					}
					format = format[n:]
					sizeFns = append(sizeFns, fn)
				} else {
					return fmt.Errorf("missing open brace sequence on %sH signifying how the header count num is encoded", escstr)
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
				if inr.usesDelims {
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
		if len(pieces) > 0 && len(pieces[0]) != 0 {
			return errors.New("invalid leading delimiter before topic, key, or value")
		}
		if len(piece) == 0 {
			return errors.New("invalid line missing trailing delimiter")
		}
		pieces = append(pieces[1:], piece)
		d := &delimer{delims: pieces}

		r.scanner = bufio.NewScanner(r.r)
		r.scanner.Split(d.split)
		r.usesDelims = true
		r.fn = func(r *Reader) error {
			var scanned int
			for r.scanner.Scan() {
				delimFns[scanned](r.scanner.Bytes(), r.on)
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

type delimer struct {
	delims  [][]byte
	atDelim int
}

func (d *delimer) split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(data, d.delims[d.atDelim]); i >= 0 {
		d.atDelim++
		if d.atDelim == len(d.delims) {
			d.atDelim = 0
		}
		return i + 1, data[0:i], nil
	}
	if atEOF {
		return 0, nil, fmt.Errorf("unfinished delim %q", d.delims[d.atDelim])
	}
	return 0, nil, nil
}

func parseReadSize(format string, dst *uint64) (int, func(*Reader) error, error) {
	braceEnd := strings.IndexByte(format, '}')
	if braceEnd == -1 {
		return 0, nil, errors.New("missing brace end } to close number size specification")
	}
	end := braceEnd + 1

	var buf [8]byte
	switch format = format[:braceEnd]; format {
	case "b8", "big8":
		return end, func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			*dst = binary.BigEndian.Uint64(buf[:])
			return nil
		}, nil

	case "b4", "big4":
		return end, func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:4]); err != nil {
				return err
			}
			*dst = uint64(binary.BigEndian.Uint32(buf[:]))
			return nil
		}, nil

	case "b2", "big2":
		return end, func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:2]); err != nil {
				return err
			}
			*dst = uint64(binary.BigEndian.Uint16(buf[:]))
			return nil
		}, nil

	case "byte", "b":
		return end, func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:1]); err != nil {
				return err
			}
			*dst = uint64(buf[0])
			return nil
		}, nil

	case "l8", "little8":
		return end, func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			*dst = binary.LittleEndian.Uint64(buf[:])
			return nil
		}, nil

	case "l4", "little4":
		return end, func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:4]); err != nil {
				return err
			}
			*dst = uint64(binary.LittleEndian.Uint32(buf[:]))
			return nil
		}, nil

	case "l2", "little2":
		return end, func(r *Reader) error {
			if _, err := io.ReadFull(r.r, buf[:2]); err != nil {
				return err
			}
			*dst = uint64(binary.LittleEndian.Uint16(buf[:]))
			return nil
		}, nil

	default:
		return end, nil, fmt.Errorf("unrecognized number reading format %q", format)
	}
}
