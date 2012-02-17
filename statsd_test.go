package statsd

import (
        "github.com/bmizerany/assert"
        "io"
        "testing"
        "bufio"
        "bytes"
)

type ReadWriter struct {
        io.Reader
        io.Writer
}

func fake() (io.ReadWriter) {
        buffer := bytes.NewBufferString("")
        return &ReadWriter{buffer, buffer}
}

func readData(rw *bufio.ReadWriter) string {
        rw.Flush()
        data, _ , _ := rw.ReadLine()
        return string(data)
}

func TestIncrement(t *testing.T) {
        c := newClient("<fake>", fake())
        err := c.Increment("incr", 1, 1)
        assert.Equal(t, err, nil)
        data := readData(c.rw)
        assert.Equal(t, data, "incr:1|c")
}

func TestDecrement(t *testing.T) {
        c := newClient("<fake>", fake())
        err := c.Decrement("decr", 1, 1)
        assert.Equal(t, err, nil)
        data := readData(c.rw)
        assert.Equal(t, data, "decr:-1|c")
}

func TestTiming(t *testing.T) {
        c := newClient("<fake>", fake())
        err := c.Timing("time", 350, 1)
        assert.Equal(t, err, nil)
        data := readData(c.rw)
        assert.Equal(t, data, "time:350|ms")
}

func TestIncrementRate(t *testing.T) {
        c := newClient("<fake>", fake())
        err := c.Increment("incr", 1, 0.99)
        assert.Equal(t, err, nil)
        data := readData(c.rw)
        assert.Equal(t, data, "incr:1|c|@0.99")
}