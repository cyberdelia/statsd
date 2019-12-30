/*
Statsd client

Supports counting, sampling, timing, gauges, sets and multi-metrics packet.

Using the client to increment a counter:

	client, err := statsd.Dial("127.0.0.1:8125")
	if err != nil {
		// handle error
	}
	defer client.Close()
	err = client.Increment("buckets", 1, 1)

*/
package statsd

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

var errMetricTooLarge = fmt.Errorf("metric too large for transport")

// Client is statsd client representing a connection to a statsd server.
type Client struct {
	conn net.Conn
	size int
	buf  *bufio.Writer
	m    sync.Mutex
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

// DialSize acts like Dial but takes a packet size.  Packet size limits the
// size of metric batches (as well as the size of individual metrics).  If size
// is 0, the default packet size of 512 bytes is used. Guidelines for packet
// size can be found in the statsd documentation
// (https://github.com/etsy/statsd/blob/master/docs/metric_types.md#multi-metric-packets).
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
		size: size,
		buf:  bufio.NewWriterSize(conn, size),
	}
}

// Increment increments the counter for the given bucket.
func (c *Client) Increment(stat string, count int, rate float64) error {
	return c.send(stat, rate, "%d|c", count)
}

// Decrement decrements the counter for the given bucket.
func (c *Client) Decrement(stat string, count int, rate float64) error {
	return c.Increment(stat, -count, rate)
}

// Duration records time spent for the given bucket with time.Duration.
func (c *Client) Duration(stat string, duration time.Duration, rate float64) error {
	return c.send(stat, rate, "%d|ms", millisecond(duration))
}

// Timing records time spent for the given bucket in milliseconds.
func (c *Client) Timing(stat string, delta int, rate float64) error {
	return c.send(stat, rate, "%d|ms", delta)
}

// Time calculates time spent in given function and send it.
func (c *Client) Time(stat string, rate float64, f func()) error {
	ts := time.Now()
	f()
	return c.Duration(stat, time.Since(ts), rate)
}

// Gauge records arbitrary values for the given bucket.
func (c *Client) Gauge(stat string, value int, rate float64) error {
	return c.send(stat, rate, "%d|g", value)
}

// IncrementGauge increments the value of the gauge.
func (c *Client) IncrementGauge(stat string, value int, rate float64) error {
	return c.send(stat, rate, "+%d|g", value)
}

// DecrementGauge decrements the value of the gauge.
func (c *Client) DecrementGauge(stat string, value int, rate float64) error {
	return c.send(stat, rate, "-%d|g", value)
}

// Unique records unique occurences of events.
func (c *Client) Unique(stat string, value int, rate float64) error {
	return c.send(stat, rate, "%d|s", value)
}

// Flush flushes writes any buffered data to the network.
func (c *Client) Flush() error {
	return c.buf.Flush()
}

// Close closes the connection.
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

	format = fmt.Sprintf("%s:%s", stat, format)

	c.m.Lock()
	defer c.m.Unlock()

	// Check the length of the metric string. Flush the buffer if necessary.
	if len(format) > c.size {
		return errMetricTooLarge
	}
	nbuf, navail := c.buf.Buffered(), c.buf.Available()
	nrequire := len(format)
	iscat := nbuf > 0 // Concatenate to the buffer w/ '\n'
	if iscat {
		nrequire++
	}
	if navail < nrequire {
		if err := c.Flush(); err != nil {
			return err
		}
		iscat = false
	}

	// Write the metric to the buffer. No flush occurs.
	if iscat {
		format = fmt.Sprintf("\n%s", format)
	}
	_, err := fmt.Fprintf(c.buf, format, args...)

	return err
}
