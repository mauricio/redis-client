package redis_client

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type command struct {
	input  []interface{}
	output interface{}
}

func TestConnect(t *testing.T) {
	tt := []struct {
		name     string
		commands []command
	}{
		{
			name: "execute a set and a get",
			commands: []command{
				{
					input: []interface{}{
						"SET",
						"some-key",
						"Maurício",
					},
					output: "OK",
				},
				{
					input: []interface{}{
						"GET",
						"some-key",
					},
					output: "Maurício",
				},
			},
		},
		{
			name: "execute a get",
			commands: []command{
				{
					input: []interface{}{
						"GET",
						"some-other-key",
					},
					output: nil,
				},
			},
		},
		{
			name: "execute a set and get with UTF characters",
			commands: []command{
				{
					input: []interface{}{
						"SET",
						"対馬",
						"Tsushima",
					},
					output: "OK",
				},
				{
					input: []interface{}{
						"GET",
						"対馬",
					},
					output: "Tsushima",
				},
			},
		},
	}

	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			server, err := miniredis.Run()
			require.NoError(t, err)
			defer server.Close()

			client, err := Connect(context.Background(), server.Addr())
			require.NoError(t, err)

			defer client.Close()

			for _, c := range ts.commands {
				result, err := client.Send(c.input)
				require.NoError(t, err)

				assert.Equal(t, c.output, result.Content())
			}

		})
	}
}
