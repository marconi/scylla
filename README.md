# Scylla

Stats logger for [Rivers](https://github.com/marconi/rivers).

## Usage

```go
queue := NewQueue("styx", "urgent")
logger := NewStatsLogger(queue)
logger.Bind() // binds to all hooks offered by a queue
```

## Running Tests

Tests can be run with [Docker](http://www.docker.com) using [Fig](http://www.fig.sh):

```sh
$ fig run --rm test
```

## Status
- Not used in any real project yet
- Coverage is at 86.3%

## License

[http://marconi.mit-license.org](http://marconi.mit-license.org)
