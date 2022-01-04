package redis_client

import (
	"github.com/pkg/errors"
	"io"
	"strconv"
)

type Writer struct {
	writer io.Writer
}

func (w *Writer) write(value interface{}, messageType byte, contents ...[]byte) error {
	if _, err := w.writer.Write([]byte{messageType}); err != nil {
		return errors.Wrapf(err, "failed to write message type: %v", messageType)
	}

	for _, b := range contents {
		if _, err := w.writer.Write(b); err != nil {
			return errors.Wrapf(err, "failed to write value: [%v]", value)
		}
	}

	return nil
}

func (w *Writer) WriteString(value string) error {
	stringBytes := []byte(value)

	return w.write(value,
		typeBulkString,
		[]byte(strconv.FormatInt(int64(len(value)), 10)),
		separator,
		stringBytes,
		separator,
	)
}

func (w *Writer) WriteInt64(v int64) error {
	return w.write(v,
		typeInteger,
		[]byte(strconv.FormatInt(v, 10)),
		separator)
}
