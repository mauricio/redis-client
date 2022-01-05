package redis_client

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"strconv"
)

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		writer: w,
	}
}

func (w *Writer) write(messageType byte, contents ...[]byte) error {
	if _, err := w.writer.Write([]byte{messageType}); err != nil {
		return errors.Wrapf(err, "failed to write message type: %v", messageType)
	}

	for _, b := range contents {
		if _, err := w.writer.Write(b); err != nil {
			return errors.Wrapf(err, "failed to write bytes, content in base64: [%v]", base64.RawStdEncoding.EncodeToString(b))
		}
	}

	return nil
}

// WriteBytes checks if the byte array has a `\r\n` before deciding how it will write it. On a more complex client
// you could have specific methods to write safe strings that would be faster but given we're working with the general
// case here just assuming you can write any string to redis is a mistake, you have to be sure the string itself
// won't contain the terminator characters.
// Strings without a `\r\n` are written as simple strings and the ones with it go as a bulk string.
func (w *Writer) WriteBytes(value []byte) error {
	if value == nil {
		return w.WriteNil()
	}

	if bytes.Index(value, separator) >= 0 {
		return w.write(
			typeBulkString,
			[]byte(strconv.FormatInt(int64(len(value)), 10)),
			separator,
			value,
			separator,
		)
	} else {
		return w.write(typeSimpleString, value, separator)
	}
}

// WriteNil writes a nil bulk string
func (w *Writer) WriteNil() error {
	return w.write(typeBulkString, []byte("-1"), separator)
}

// WriteString writes a string as a byte array
func (w *Writer) WriteString(value string) error {
	stringBytes := []byte(value)

	return w.WriteBytes(stringBytes)
}

func (w *Writer) WriteInt64(v int64) error {
	return w.write(
		typeInteger,
		[]byte(strconv.FormatInt(v, 10)),
		separator)
}

// WriteArray writes an array that contains int8 to int64, strings, []byte, []interface{} or nil.
// Any other values inside the array will cause this method to return an error.
func (w *Writer) WriteArray(values []interface{}) error {
	if values == nil {
		return w.write(typeArray, []byte("-1"), separator)
	}

	if err := w.write(
		typeArray,
		[]byte(strconv.FormatInt(int64(len(values)), 10)),
		separator,
	); err != nil {
		return err
	}

	for _, v := range values {
		switch t := v.(type) {
		case int8:
			if err := w.WriteInt64(int64(t)); err != nil {
				return err
			}
		case int16:
			if err := w.WriteInt64(int64(t)); err != nil {
				return err
			}
		case int:
			if err := w.WriteInt64(int64(t)); err != nil {
				return err
			}
		case int32:
			if err := w.WriteInt64(int64(t)); err != nil {
				return err
			}
		case int64:
			if err := w.WriteInt64(t); err != nil {
				return err
			}
		case string:
			if err := w.WriteString(t); err != nil {
				return err
			}
		case []byte:
			if err := w.WriteBytes(t); err != nil {
				return err
			}
		case []interface{}:
			if err := w.WriteArray(t); err != nil {
				return err
			}
		case nil:
			if err := w.WriteNil(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported type: the value [%#v] is not supported by this client, supported types are int8 to int64, strings, []byte, nil, and []interface{} of these same types", v)
		}
	}

	return nil
}
