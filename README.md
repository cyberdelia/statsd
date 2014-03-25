# go-statsdclient

go-statsdclient is a client library for statsd written in Go.

## Installation

Download and install :

```
$ go get github.com/sendgrid/go-statsclient
```

Add it to your code :

```go
import "github.com/sendgrid/go-statsdclient"
```

## Use

```go
c := statsdclient.Dial("localhost:8125")

hostname, _ := os.Hostname()
c.SetPrefix(statsdclient.MakePrefix("production", "exampleapp", hostname))

c.Increment("incr", 1, 1)
c.Decrement("decr", 1, 0.1)
c.Timing("timer", 320, 0.1)
c.Time("timer", 0.1, func() {
        // do something  
})
c.Gauge("gauge", 30, 1)
c.Unique("unique", 765, 1)
```
