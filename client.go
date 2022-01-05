package redis_client

import (
	"context"
	"github.com/pkg/errors"
	"io"
	"net"
	"time"
)

var (
	_ io.Closer = &Client{}
)

type Client struct {
	conn   net.Conn
	reader *Reader
	writer *Writer
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Send(values []interface{}) (*Result, error) {
	if err := c.writer.WriteArray(values); err != nil {
		return nil, errors.Wrapf(err, "failed to execute operation: %v", values[0])
	}

	return c.reader.Read()
}

func Connect(ctx context.Context, address string) (*Client, error) {
	dialer := net.Dialer{
		Timeout:   time.Second * 5,
		KeepAlive: time.Second * 10,
	}

	conn, err := dialer.DialContext(ctx, "tcp4", address)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to %v", address)
	}

	return &Client{
		conn:   conn,
		reader: NewReader(conn),
		writer: NewWriter(conn),
	}, nil
}
