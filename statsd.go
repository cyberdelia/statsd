// Client library for statsd.
package statsd

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
	"time"
)

// A statsd client representing a connection to a statsd server.
type Client struct {
	Name string
	rw   *bufio.ReadWriter
}

func millisecond(d time.Duration) int {
	return int(d.Seconds() * 1000)
}

// Dial connects to the given address on the given network using net.Dial and then returns a new Client for the connection.
func Dial(addr string) (*Client, error) {
	rw, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return newClient(addr, rw), nil
}

func DialTimeout(addr string, timeout time.Duration) (*Client, error) {
	rw, err := net.DialTimeout("udp", addr, timeout)
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

// Increment the counter for the given bucket
func (c *Client) Increment(stat string, count int, rate float64) error {
	return c.send(stat, rate, "%d|c", count)
}

// Decrement the counter for the given bucket
func (c *Client) Decrement(stat string, count int, rate float64) error {
	return c.Increment(stat, -count, rate)
}

// Record time spend for the given bucket
func (c *Client) Timing(stat string, delta int, rate float64) error {
	return c.send(stat, rate, "%d|ms", delta)
}

// Calculate time spend in given function and send it
func (c *Client) Time(stat string, rate float64, f func()) error {
	ts := time.Now()
	f()
	delta := millisecond(time.Now().Sub(ts))
	return c.Timing(stat, delta, rate)
}

// Record arbitrary values for the given bucket
func (c *Client) Gauge(stat string, value int, rate float64) error {
	return c.send(stat, rate, "%d|g", value)
}

func (c *Client) send(stat string, rate float64, format string, args ...interface{}) error {
	if rate < 1 {
		if rand.Float64() < rate {
			format = fmt.Sprintf("%s|@%1.2f", format, rate)
		} else {
			return nil
		}
	}

	format = fmt.Sprintf("%s:%s", stat, format)
	_, err := fmt.Fprintf(c.rw, format, args...)
	if err != nil {
		return err
	}

	err = c.rw.Flush()
	return err
}
