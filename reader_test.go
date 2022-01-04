package redis_client

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestReader_Read(t *testing.T) {
	tt := []struct {
		input  string
		result interface{}
		err    string
	}{
		{
			input:  "+OK\r\n",
			result: "OK",
		},
		{
			input:  "-ERR unknown command 'foobar'\r\n",
			result: errors.New("ERR unknown command 'foobar'"),
		},
		{
			input:  ":1000\r\n",
			result: int64(1000),
		},
		{
			input: "+\r",
			err:   "unexpected end of stream, a redis message needs at least 3 characters to be valid, actual content in base64: [Kw0]",
		},
		{
			input: "+BROKEN\r",
			err:   "unexpected end of stream, there should have been a \\r\\n before the end, actual content in base64: [K0JST0tFTg0]",
		},
		{
			input:  "$6\r\nfoobar\r\n",
			result: "foobar",
		},
		{
			input:  "$0\r\n\r\n",
			result: "",
		},
		{
			input:  "$-1\r\n",
			result: nil,
		},
		{
			input: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
			result: []interface{}{
				"foo",
				"bar",
			},
		},
		{
			input: "*3\r\n:1\r\n:2\r\n:3\r\n",
			result: []interface{}{
				int64(1),
				int64(2),
				int64(3),
			},
		},
		{
			input:  "*0\r\n",
			result: []interface{}{},
		},
		{
			input:  "*-1\r\n",
			result: nil,
		},
		{
			input: "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n",
			result: []interface{}{
				[]interface{}{
					int64(1),
					int64(2),
					int64(3),
				},
				[]interface{}{
					"Foo",
					errors.New("Bar"),
				},
			},
		},
	}

	for _, ts := range tt {
		t.Run(ts.input, func(t *testing.T) {
			r := NewReader(strings.NewReader(ts.input))
			result, err := r.Read()
			if ts.err != "" {
				assert.EqualError(t, err, ts.err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, ts.result, result.Content())
			}

			assert.False(t, r.scanner.Scan())
		})
	}

}
