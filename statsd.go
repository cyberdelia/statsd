package statsd

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
)

type Client struct {
	Name string
	rw   *bufio.ReadWriter
}

func Dial(addr string) (*Client, error) {
	rw, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return newClient(addr, rw), nil
}

func newClient(name string, rw io.ReadWriter) *Client {
	c := new(Client)
	c.Name = name
	c.rw = bufio.NewReadWriter(bufio.NewReader(rw), bufio.NewWriter(rw))
	return c
}

func (c *Client) Increment(stat string, count int, rate float64) error {
	return c.Send(stat, rate, "%d|c", count)
}

func (c *Client) Decrement(stat string, count int, rate float64) error {
	return c.Increment(stat, -count, rate)
}

func (c *Client) Timing(stat string, delta int, rate float64) error {
	return c.Send(stat, rate, "%d|ms", delta)
}

func (c *Client) Send(stat string, rate float64, format string, args ...interface{}) error {
	if rate < 1 {
		if rand.Float64() < rate {
			format = fmt.Sprintf("%s|@%1.2f", format, rate)
		} else {
			return nil
		}
	}
	format = fmt.Sprintf("%s:%s", stat, format)
	_, err := fmt.Fprintf(c.rw, format, args...)
	return err
}
