package statsd

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
	"time"
)

func fakeClient(buffer *bytes.Buffer) *Client {
	return &Client{
		size: defaultBufSize,
		buf:  bufio.NewWriterSize(buffer, defaultBufSize),
	}
}

func assert(t *testing.T, value, control string) {
	if value != control {
		t.Errorf("incorrect command, want '%s', got '%s'", control, value)
	}
}

func TestMetricTooLarge(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Increment(strings.Repeat("a.man.a.plan.a.canal.panama", 300), 10, 1.0)
	if err == nil {
		t.Fatal("unexpected success incrementing long metric name")
	}
	if err != errMetricTooLarge {
		t.Fatalf("unexpected %T error: %v", err, err)
	}
	c.Flush()
	assert(t, buf.String(), "")
}

func TestIncrement(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Increment("incr", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "incr:1|c")
}

func TestIncrement64(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Increment64("incr64", 1099511627776, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "incr64:1099511627776|c")
}

func TestDecrement(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Decrement("decr", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "decr:-1|c")
}

func TestDecrement64(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Decrement("decr64", 1099511627778, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "decr64:-1099511627778|c")
}

func TestDuration(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Duration("timing", time.Duration(123456789), 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "timing:123|ms")
}

func TestIncrementRate(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Increment("incr", 1, 0.99)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "incr:1|c|@0.99")
}

func TestPreciseRate(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	// The real use case here is rates like 0.0001.
	err := c.Increment("incr", 1, 0.99901)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "incr:1|c|@0.99901")
}

func TestRate(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Increment("incr", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "")
}

func TestGauge(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Gauge("gauge", 300, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "gauge:300|g")
}

func TestIncrementGauge(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.IncrementGauge("gauge", 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "gauge:+10|g")
}

func TestDecrementGauge(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.DecrementGauge("gauge", 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "gauge:-4|g")
}

func TestUnique(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Unique("unique", 765, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "unique:765|s")
}

var millisecondTests = []struct {
	duration time.Duration
	control  int
}{
	{
		duration: 350 * time.Millisecond,
		control:  350,
	},
	{
		duration: 5 * time.Second,
		control:  5000,
	},
	{
		duration: 50 * time.Nanosecond,
		control:  0,
	},
}

func TestMilliseconds(t *testing.T) {
	for i, mt := range millisecondTests {
		value := millisecond(mt.duration)
		if value != mt.control {
			t.Errorf("%d: incorrect value, want %d, got %d", i, mt.control, value)
		}
	}
}

func TestTiming(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Timing("timing", 350, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "timing:350|ms")
}

func TestTime(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Time("time", 1, func() { time.Sleep(50e6) })
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultiPacket(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	err := c.Unique("unique", 765, 1)
	if err != nil {
		t.Fatal(err)
	}
	err = c.Unique("unique", 765, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Flush()
	assert(t, buf.String(), "unique:765|s\nunique:765|s")
}

func TestMultiPacketOverflow(t *testing.T) {
	buf := new(bytes.Buffer)
	c := fakeClient(buf)
	for i := 0; i < 40; i++ {
		err := c.Unique("unique", 765, 1)
		if err != nil {
			t.Fatal(err)
		}
	}
	assert(t, buf.String(), "unique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s\nunique:765|s")
	buf.Reset()
	c.Flush()
	assert(t, buf.String(), "unique:765|s")
}
