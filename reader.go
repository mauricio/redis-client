package redis_client

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	pkgerrors "github.com/pkg/errors"
	"io"
	"strconv"
)

const (
	defaultBufferLength = 10140
	typeSimpleString    = '+'
	typeErorr           = '-'
	typeInteger         = ':'
	typeBulkString      = '$'
	typeArray           = '*'
)

var (
	separator = []byte("\r\n")
)

type Reader struct {
	scanner *bufio.Scanner
}

func NewReader(r io.Reader) *Reader {
	scanner := bufio.NewScanner(bufio.NewReaderSize(r, defaultBufferLength))
	scanner.Split(redisSplitter)

	return &Reader{
		scanner: scanner,
	}
}

func (r *Reader) Read() (*Result, error) {
	return readRESP(r.scanner)
}

// redisSplitter splits a byte stream into full lines in the redis protocol format. it reads a whole item
// (string, bulk string, error, integer or array) and then returns it as a line. code reading from scanners
// created with this function can safely assume that if they got a line it's a full line that does not need
// any extra checking.
func redisSplitter(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// a valid redis message has at least 3 characters, if less than that ask for more stuff
	if len(data) < 3 {

		if atEOF {
			return 0, nil, fmt.Errorf("unexpected end of stream, a redis message needs at least 3 characters to be valid, actual content in base64: [%v]", base64.RawStdEncoding.EncodeToString(data))
		}

		return 0, nil, nil
	}

	found := bytes.Index(data, separator)
	// if we could not find a \r\n and the stream is at its end, this stream is broken and can't be recovered
	if found == -1 && atEOF {
		return 0, nil, fmt.Errorf("unexpected end of stream, there should have been a \\r\\n before the end, actual content in base64: [%v]", base64.RawStdEncoding.EncodeToString(data))
	}

	// if there is no \r\n we need to read more data, this means this result isn't finished yet
	if found == -1 {
		return 0, nil, nil
	}

	// bulk strings are special, they change the binary format from a terminator based one (\r\n to close messages)
	// to a length based one. so once we figure out this message is a bulk string (like `$6\r\nfoobar\r\n`),
	// we have to read the length and make sure there are at least length + 2 bytes after the first \r\n to
	// show that we do have the whole bulk string here. it is not safe to just find all \r\n in a bulk string
	// because there could be \r\n tokens as part of the string itself, so we always have to make sure we consume
	// the length and use it to read the whole value.
	if data[0] == typeBulkString {
		length, err := strconv.ParseInt(string(data[1:found]), 10, 64)
		if err != nil {
			return 0, nil, fmt.Errorf("message starts as bulk string but length is not a valid int, actual content in base64: [%v]", base64.RawStdEncoding.EncodeToString(data[0:found]))
		}

		// a -1 length means this is a null string and should be returned as such to clients, null and empty
		// strings are different things in redis. this is the only time we return a bulk string sign ($) here
		// as we'll use it as a marker for null strings. for someone reading from a scanner
		// there should be no difference between a simple or a bulk string as we have already
		// parsed the lengh and we'll return only the actual string contents.
		if length == -1 {
			return 5, []byte("$"), nil
		}

		// a 0 length means an empty string, an empty string is not the same as a null string on redis
		if length == 0 {
			return 6, []byte("+"), nil
		}

		// this is the position of the first \r\n + the expected length + 4 which is the \r\n twice we have on bulk strings
		expectedEnding := found + int(length) + 4
		if len(data) >= expectedEnding {
			// given here we already have all the information we need to return this as a string,
			// we don't return the length anymore, we return this as if it was a normal string.
			// now we set the first `\n` we have to `+` so the code parses it as a simple string
			// as we have already capped the returned slice do the length of the string.

			start := found + 1
			data[start] = '+'
			return expectedEnding, data[start : expectedEnding-2], nil
		}

		if atEOF {
			return 0, nil, fmt.Errorf("unexpected end of stream, stream ends before bulk string has ended, expected there to be %v total bytes but there were only %v, actual content in base64: %v", expectedEnding, len(data), base64.RawStdEncoding.EncodeToString(data))
		}

		return 0, nil, err
	}

	return found + 2, data[:found], nil
}

// readRESP reads from a scanner that was initiated with `redisSplitter`. it expects every scanned line to be
// a full line of a data type redis supports (unless it's an array, arrays start with the length of the array only).
func readRESP(r *bufio.Scanner) (*Result, error) {

	for r.Scan() {
		line := r.Text()
		switch line[0] {
		case typeSimpleString:
			// if a string, just remove the marker and return it
			return &Result{
				content: line[1:],
			}, nil
		case typeBulkString:
			// a bulk string is only returned if it is nil, otherwise it is turned as a simple string
			return &Result{
				content: nil,
			}, nil
		case typeErorr:
			// if an error just wrap the error and return it
			return &Result{
				content: errors.New(line[1:]),
			}, nil
		case typeInteger:
			content, err := strconv.ParseInt(line[1:], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse returned integer: %v (value: %v)", err, line)
			}
			return &Result{
				content: content,
			}, nil
		case typeArray:
			// the first thing to be done when we find an array is to find its length, if not `-1` we then
			// read items from the scanner until we've read all items on the array.
			length, err := strconv.ParseInt(line[1:], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse array length: %v (value: %v)", err, line)
			}

			if length == -1 {
				return &Result{content: nil}, nil
			}

			contents := make([]interface{}, 0, length)

			for x := int64(0); x < length; x++ {
				result, err := readRESP(r)
				if err != nil {
					return nil, pkgerrors.Wrapf(err, "failed to read item %v from array", x)
				}

				contents = append(contents, result.content)
			}

			return &Result{
				content: contents,
			}, nil
		}
	}

	if r.Err() == nil {
		return nil, errors.New("scanner was empty")
	}

	return nil, r.Err()
}
