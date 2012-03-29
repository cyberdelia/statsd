# statsd

statsd is a client library for statsd written in Go.

## Installation

Download and install :

```
$ go get github.com/cyberdelia/statsd
```

Add it to your code :

```go
import "github.com/cyberdelia/statsd"
```

## Use

```go
c := statsd.Dial("localhost:8125")
c.Increment("incr", 1, 1)
c.Decrement("decr", 1, 0.1)
c.Timing("timer", 320, 0.1)
```
