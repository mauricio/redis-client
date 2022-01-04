package redis_client

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWriter_WriteArray(t *testing.T) {
	tt := []struct {
		name   string
		input  []interface{}
		output interface{}
		err    string
	}{
		{
			name: "a simple array",
			input: []interface{}{
				"foo",
				"bar",
				nil,
				10,
				int64(65),
			},
			output: []interface{}{
				"foo",
				"bar",
				nil,
				int64(10),
				int64(65),
			},
		},
		{
			name: "an array of arrays",
			input: []interface{}{
				[]interface{}{
					"nope",
					"yup",
				},
				10,
				[]interface{}{
					"honey",
					"1000",
				},
			},
			output: []interface{}{
				[]interface{}{
					"nope",
					"yup",
				},
				int64(10),
				[]interface{}{
					"honey",
					"1000",
				},
			},
		},
		{
			name:   "a nil array",
			input:  nil,
			output: nil,
		},
	}

	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			buffer := &bytes.Buffer{}

			writer := NewWriter(buffer)
			err := writer.WriteArray(ts.input)
			if ts.err != "" {
				assert.EqualError(t, err, ts.err)
			} else {
				require.NoError(t, err)

				reader := NewReader(buffer)
				result, err := reader.Read()
				require.NoError(t, err)
				assert.Equal(t, ts.output, result.Content())
			}
		})
	}

}
