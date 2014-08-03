package scylla

import (
	"log"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/marconi/rivers"
)

var (
	PIPE_BUF     = 1024
	STATS_EXPIRY = 7200 // 2 hours
)

type statsLog struct {
	name  string
	value int64
}

type StatsLogger struct {
	conn      redis.Conn
	queue     rivers.Queue
	pipe      chan *statsLog
	cache     map[int64]map[string]int64
	lastFlush int64
}

func NewStatsLogger(queue rivers.Queue) *StatsLogger {
	l := &StatsLogger{
		conn:  rivers.NewNonPool(),
		queue: queue,
		pipe:  make(chan *statsLog, PIPE_BUF),
		cache: make(map[int64]map[string]int64),
	}
	l.startStatsLogger()
	return l
}

// Automatically bind to all of the queue's hooks
func (l *StatsLogger) Bind() {
	l.queue.Register("push", func(j rivers.Job) {
		l.Log(PushStatsKey(l.queue.GetName()), 1)
	})

	l.queue.Register("multipush", func(jobs []rivers.Job) {
		l.Log(PushStatsKey(l.queue.GetName()), int64(len(jobs)))
	})

	l.queue.Register("pop", func(j rivers.Job) {
		l.Log(PopStatsKey(l.queue.GetName()), 1)
	})

	l.queue.Register("multipop", func(jobs []rivers.Job) {
		l.Log(PopStatsKey(l.queue.GetName()), int64(len(jobs)))
	})

	l.queue.Register("ack", func(j rivers.Job) {
		l.Log(AckStatsKey(l.queue.GetName()), 1)
	})
}

// Flushes cached stats and closes resources
func (l *StatsLogger) Destroy() {
	// wait for 2 seconds to make sure caches are flushed
	time.Sleep(2 * time.Second)

	// close resources
	close(l.pipe)
	l.conn.Close()
}

// Logs a stat
func (l *StatsLogger) Log(name string, n int64) {
	s := &statsLog{name: name, value: n}
	l.pipe <- s
}

func (l *StatsLogger) startStatsLogger() {
	writing := false
	go func() {
		for s := range l.pipe {
			now := time.Now().UTC().Unix()

			// if now doesn't have a cache yet, allocate one
			if _, ok := l.cache[now]; !ok {
				l.cache[now] = make(map[string]int64)
			}

			// increment logged stats' value
			l.cache[now][s.name] += s.value

			// if we have accumulated some stats and
			// its safe to write, flush the stats cache.
			if now > l.lastFlush && !writing {
				writing = true
				l.flush(now)
				writing = false
			}
		}
	}()
}

func (l *StatsLogger) flush(now int64) {
	for sec, secCache := range l.cache {
		// only flush caches older than 2 seconds
		if sec >= now-1 {
			continue
		}

		for name, value := range secCache {
			secKey := SecStatsKey(name, sec)
			l.conn.Send("MULTI")
			l.conn.Send("INCRBY", secKey, value)
			l.conn.Send("EXPIRE", secKey, STATS_EXPIRY)
			if _, err := l.conn.Do("EXEC"); err != nil {
				log.Println("unable to increment flushed stats for %s:", name, err)
				continue
			}
		}

		// also log queue lengths to expire in two hours
		sizeKey := SecQueueSizeKey(l.queue.GetName(), sec)
		size, err := l.queue.GetSize()
		if _, err = l.conn.Do("SETEX", sizeKey, STATS_EXPIRY, size); err != nil {
			log.Println("unable to log queue size:", err)
		}

		// delete this second's cache
		delete(l.cache, sec)
	}
	l.lastFlush = now
}
