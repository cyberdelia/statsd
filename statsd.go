/*
Statsd client

Supports counting, sampling, timing, gauges, sets and multi-metrics packet.

Using the client to increment a counter:

	client, err := statsdclient.Dial("127.0.0.1:8125")
	if err != nil {
		// handle error
	}
	defer client.Close()
	err = client.Increment("buckets", 1, 1)

*/
package statsdclient

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	defaultBufSize = 512
)

// A statsd client representing a connection to a statsd server.
type Client struct {
	conn net.Conn
	buf  *bufio.Writer
	m    sync.Mutex

	// The prefix to be added to every key. Should include the "." at the end if desired
	prefix string
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
	return newClient(conn, 0), nil
}

// DialTimeout acts like Dial but takes a timeout. The timeout includes name resolution, if required.
func DialTimeout(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("udp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return newClient(conn, 0), nil
}

// DialSize acts like Dial but takes a packet size.
// By default, the packet size is 512, see https://github.com/etsy/statsd/blob/master/docs/metric_types.md#multi-metric-packets for guidelines.
func DialSize(addr string, size int) (*Client, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return newClient(conn, size), nil
}

func newClient(conn net.Conn, size int) *Client {
	if size <= 0 {
		size = defaultBufSize
	}
	return &Client{
		conn: conn,
		buf:  bufio.NewWriterSize(conn, size),
	}
}

// Set the key prefix for the client. All future stats will be sent with the
// prefix value prepended to the bucket
func (c *Client) SetPrefix(prefix string) {
	c.prefix = prefix
}

// Increment the counter for the given bucket.
func (c *Client) Increment(stat string, count int, rate float64) error {
	return c.send(stat, rate, "%d|c", count)
}

// Decrement the counter for the given bucket.
func (c *Client) Decrement(stat string, count int, rate float64) error {
	return c.Increment(stat, -count, rate)
}

// Record time spent for the given bucket with time.Duration.
func (c *Client) Duration(stat string, duration time.Duration, rate float64) error {
	return c.send(stat, rate, "%f|ms", duration.Seconds()*1000)
}

// Record time spent for the given bucket in milliseconds.
func (c *Client) Timing(stat string, delta int, rate float64) error {
	return c.send(stat, rate, "%d|ms", delta)
}

// Calculate time spent in given function and send it.
func (c *Client) Time(stat string, rate float64, f func()) error {
	ts := time.Now()
	f()
	return c.Duration(stat, time.Since(ts), rate)
}

// Record arbitrary values for the given bucket.
func (c *Client) Gauge(stat string, value int, rate float64) error {
	return c.send(stat, rate, "%d|g", value)
}

// Increment the value of the gauge.
func (c *Client) IncrementGauge(stat string, value int, rate float64) error {
	return c.send(stat, rate, "+%d|g", value)
}

// Decrement the value of the gauge.
func (c *Client) DecrementGauge(stat string, value int, rate float64) error {
	return c.send(stat, rate, "-%d|g", value)
}

// Record unique occurences of events.
func (c *Client) Unique(stat string, value int, rate float64) error {
	return c.send(stat, rate, "%d|s", value)
}

// Flush writes any buffered data to the network.
func (c *Client) Flush() error {
	return c.buf.Flush()
}

// Closes the connection.
func (c *Client) Close() error {
	if err := c.Flush(); err != nil {
		return err
	}
	c.buf = nil
	return c.conn.Close()
}

func (c *Client) send(stat string, rate float64, format string, args ...interface{}) error {
	if rate < 1 {
		if rand.Float64() < rate {
			format = fmt.Sprintf("%s|@%g", format, rate)
		} else {
			return nil
		}
	}

	format = fmt.Sprintf("%s%s:%s", c.prefix, stat, format)

	c.m.Lock()
	defer c.m.Unlock()

	// Flush data if we have reach the buffer limit
	if c.buf.Available() < len(format) {
		if err := c.Flush(); err != nil {
			return nil
		}
	}

	// Buffer is not empty, start filling it
	if c.buf.Buffered() > 0 {
		format = fmt.Sprintf("\n%s", format)
	}

	_, err := fmt.Fprintf(c.buf, format, args...)
	return err
}
