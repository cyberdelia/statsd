// Client library for statsd.
package statsd

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

// A statsd client representing a connection to a statsd server.
type Client struct {
	conn *net.Conn
	buf  *bufio.ReadWriter
	sync.Mutex
}

func millisecond(d time.Duration) int {
	return int(d.Seconds() * 1000)
}

// Dial connects to the given address on the given network using net.Dial and then returns a new Client for the connection.
func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return newClient(&conn), nil
}

func DialTimeout(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("udp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return newClient(&conn), nil
}

func newClient(conn *net.Conn) *Client {
	return &Client{
		conn: conn,
		buf:  bufio.NewReadWriter(bufio.NewReader(*conn), bufio.NewWriter(*conn)),
	}
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

// Record unique occurences of events
func (c *Client) Unique(stat string, value int, rate float64) error {
	return c.send(stat, rate, "%d|s", value)
}

func (c *Client) Close() error {
	err := c.buf.Flush()
	if err != nil {
		return err
	}
	c.buf = nil
	return (*c.conn).Close()
}

func (c *Client) send(stat string, rate float64, format string, args ...interface{}) error {
	if rate < 1 {
		if rand.Float64() < rate {
			format = fmt.Sprintf("%s|@%g", format, rate)
		} else {
			return nil
		}
	}

	format = fmt.Sprintf("%s:%s", stat, format)

	c.Lock()
	defer c.Unlock()

	_, err := fmt.Fprintf(c.buf, format, args...)
	if err != nil {
		return err
	}

	return c.buf.Flush()
}
