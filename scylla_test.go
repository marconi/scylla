package scylla_test

import (
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	. "github.com/marconi/rivers"
	. "github.com/marconi/scylla"
	. "github.com/smartystreets/goconvey/convey"
)

func TestStatsLogging(t *testing.T) {
	Convey("should be able to flush stats", t, func() {
		conn := Pool.Get()

		Convey("push without delay", func() {
			min := time.Now().UTC().Unix()

			uq1 := NewQueue("uq1", "urgent")
			logger1 := NewStatsLogger(uq1)
			logger1.Bind()

			uq1.Push(NewJob())
			uq1.Push(NewJob())

			now := time.Now().UTC().Unix()
			diff := now - min

			// check that before min up to max,
			// there shouldn't be any stats logged
			for i := int64(0); i <= diff; i++ {
				sec := min + i
				secKey := SecStatsKey(PushStatsKey(uq1.GetName()), sec)
				exists, _ := redis.Bool(conn.Do("EXISTS", secKey))
				So(exists, ShouldEqual, false)

				// there should be no queue size logged as well
				sizeKey := SecQueueSizeKey(uq1.GetName(), sec)
				exists, _ = redis.Bool(conn.Do("EXISTS", sizeKey))
				So(exists, ShouldEqual, false)
			}

			Reset(func() {
				uq1.Destroy()
				logger1.Destroy()
			})
		})

		Convey("push with 2 seconds delay", func() {
			min := time.Now().UTC().Unix()

			uq2 := NewQueue("uq2", "urgent")
			logger2 := NewStatsLogger(uq2)
			logger2.Bind()

			uq2.Push(NewJob())

			// sleep two seconds to trigger a flush on next push
			time.Sleep(2 * time.Second)

			uq2.Push(NewJob())

			now := time.Now().UTC().Unix()
			diff := now - min

			// sleep one more to make sure flush is done
			// before we actually start checking
			time.Sleep(1 * time.Second)

			// check the before min up to max,
			// there should be stats logged
			foundSecKey := false
			foundSizeKey := false
			for i := int64(0); i <= diff; i++ {
				sec := min + i
				secKey := SecStatsKey(PushStatsKey(uq2.GetName()), sec)
				secExists, _ := redis.Bool(conn.Do("EXISTS", secKey))
				if secExists {
					foundSecKey = true

					pushStats, _ := redis.Int64(conn.Do("GET", secKey))
					So(pushStats, ShouldEqual, 1)
				}

				// there should be queue size logged as well
				sizeKey := SecQueueSizeKey(uq2.GetName(), sec)
				sizeExists, _ := redis.Bool(conn.Do("EXISTS", sizeKey))
				if sizeExists {
					foundSizeKey = true

					// due to the sleep we can be sure that both items
					// has been flushed so we check for size of two here
					sizeStats, _ := redis.Int64(conn.Do("GET", sizeKey))
					So(sizeStats, ShouldEqual, 2)
				}
			}

			So(foundSecKey, ShouldEqual, true)
			So(foundSizeKey, ShouldEqual, true)

			Reset(func() {
				uq2.Destroy()
				logger2.Destroy()
			})
		})

		Convey("with pop stats", func() {
			min := time.Now().UTC().Unix()

			uqdq1 := NewQueue("uqdq1", "delayed")
			uq3 := NewQueue("uq3", "urgent")

			logger3 := NewStatsLogger(uqdq1)
			logger3.Bind()

			uqdq1.Push(NewJob())
			uqdq1.Push(NewJob())

			j1, err := uqdq1.Pop()
			So(j1, ShouldNotEqual, nil)
			So(err, ShouldEqual, nil)

			_, err = uq3.Push(j1)
			So(err, ShouldEqual, nil)

			j2, err := uqdq1.Pop()
			So(j2, ShouldNotEqual, nil)
			So(err, ShouldEqual, nil)

			_, err = uq3.Push(j2)
			So(err, ShouldEqual, nil)

			// sleep two seconds to trigger a flush on next push
			time.Sleep(2 * time.Second)

			// that would be 3 push and 2 pop jobs flushed
			uqdq1.Push(NewJob())

			now := time.Now().UTC().Unix()
			diff := now - min

			// sleep one more to make sure flush is done
			// before we actually start checking
			time.Sleep(1 * time.Second)

			// check the before min up to max,
			// there should be stats logged for pop
			foundSecKey := false
			foundSizeKey := false
			for i := int64(0); i <= diff; i++ {
				sec := min + i
				secKey := SecStatsKey(PopStatsKey(uqdq1.GetName()), sec)
				secExists, _ := redis.Bool(conn.Do("EXISTS", secKey))
				if secExists {
					foundSecKey = true

					// we sleep after popping so we can
					// be sure both pop were flushed
					popStats, _ := redis.Int64(conn.Do("GET", secKey))
					So(popStats, ShouldEqual, 2)
				}

				// there should be queue size logged for uqdq1
				sizeKey := SecQueueSizeKey(uqdq1.GetName(), sec)
				sizeExists, _ := redis.Bool(conn.Do("EXISTS", sizeKey))
				if sizeExists {
					foundSizeKey = true

					// we pushed trice and popped twice so
					// uqdq1 should have one size
					sizeStats, _ := redis.Int64(conn.Do("GET", sizeKey))
					So(sizeStats, ShouldEqual, 1)
				}
			}

			So(foundSecKey, ShouldEqual, true)
			So(foundSizeKey, ShouldEqual, true)

			Reset(func() {
				uqdq1.Destroy()
				uq3.Destroy()
				logger3.Destroy()
			})
		})

		Reset(func() {
			conn.Close()
		})
	})
}
