package redis_client

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestResult_Int64(t *testing.T) {
	tt := []struct {
		name   string
		input  interface{}
		result int64
		err    string
	}{
		{
			name:   "a good int64",
			input:  int64(10),
			result: 10,
		},
		{
			name:  "a string instead of an int64",
			input: "some string",
			err:   "content is not an int64: \"some string\"",
		},
		{
			name:  "an error instead of an int64",
			input: errors.New("damn"),
			err:   "damn",
		},
	}

	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			r := &Result{
				content: ts.input,
			}

			value, err := r.Int64()

			if ts.err != "" {
				assert.EqualError(t, err, ts.err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, ts.result, value)
		})
	}
}

func TestResult_String(t *testing.T) {
	tt := []struct {
		name   string
		input  interface{}
		result string
		isNil  bool
		err    string
	}{
		{
			name:   "a good string",
			input:  "10",
			result: "10",
		},
		{
			name:   "a nil string",
			input:  nil,
			result: "",
			isNil:  true,
		},
		{
			name:  "an int64 instead of a string",
			input: int64(10),
			err:   "content is not an string: 10",
		},
		{
			name:  "an error instead of an string",
			input: errors.New("damn"),
			err:   "damn",
		},
	}

	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			r := &Result{
				content: ts.input,
			}

			value, isNil, err := r.String()

			if ts.err != "" {
				assert.EqualError(t, err, ts.err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, ts.result, value)
			assert.Equal(t, ts.isNil, isNil)
		})
	}
}

func TestResult_Slice(t *testing.T) {
	tt := []struct {
		name   string
		input  interface{}
		result []interface{}
		err    string
	}{
		{
			name: "a good array",
			input: []interface{}{
				"foo",
				"bar",
			},
			result: []interface{}{
				"foo",
				"bar",
			},
		},
		{
			name:   "a nil slice",
			input:  nil,
			result: nil,
		},
		{
			name:  "an int64 instead of an slice",
			input: int64(10),
			err:   "content is not a slice: 10",
		},
		{
			name:  "an error instead of an slice",
			input: errors.New("damn"),
			err:   "damn",
		},
	}

	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			r := &Result{
				content: ts.input,
			}

			value, err := r.Slice()

			if ts.err != "" {
				assert.EqualError(t, err, ts.err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, ts.result, value)
		})
	}
}
