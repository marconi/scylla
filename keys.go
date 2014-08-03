package scylla

import (
	"fmt"
)

func PushStatsKey(qname string) string {
	return fmt.Sprintf("%s:push", qname)
}

func PopStatsKey(qname string) string {
	return fmt.Sprintf("%s:pop", qname)
}

func AckStatsKey(qname string) string {
	return fmt.Sprintf("%s:ack", qname)
}

func SecStatsKey(sname string, sec int64) string {
	return fmt.Sprintf("%s:%d", sname, sec)
}

func SecQueueSizeKey(qname string, sec int64) string {
	return fmt.Sprintf("%s:size:%d", qname, sec)
}
