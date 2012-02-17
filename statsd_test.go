package statsd

import (
        "github.com/bmizerany/assert"
        "io"
        "testing"
        "bytes"
)

type ReadWriter struct {
        io.Reader
        io.Writer
}

func fake() (io.ReadWriter, *bytes.Buffer) {
        buffer := bytes.NewBufferString("")
        rd := new(bytes.Buffer)
        return &ReadWriter{rd, buffer}, buffer
}

func TestIncrement(t *testing.T) {
        rw, _ := fake()
        c := newClient("<fake>", rw)
        err := c.Increment("incr", 1, 1)
        assert.Equal(t, err, nil)
}

func TestDecrement(t *testing.T) {
        rw, _ := fake()
        c := newClient("<fake>", rw)
        err := c.Decrement("decr", 1, 1)
        assert.Equal(t, err, nil)
}

func TestTiming(t *testing.T) {
        rw, _ := fake()
        c := newClient("<fake>", rw)
        err := c.Timing("time", 350, 1)
        assert.Equal(t, err, nil)
}

func TestIncrementRate(t *testing.T) {
        rw, _ := fake()
        c := newClient("<fake>", rw)
        err := c.Increment("incr", 1, 0.99)
        assert.Equal(t, err, nil)    
}