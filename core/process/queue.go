package process

import (
	"github.com/deckarep/golang-set"
	sq "github.com/yireyun/go-queue"
	"log"
	"time"
)

var Queue *sq.EsQueue

func init() {
	Queue = sq.NewQueue(1000)
}

func Put(set mapset.Set) {
	slice := set.ToSlice()
	for _, v := range slice {
		Queue.Put(v)
	}
}

func LoopProcess() {
	for {
		val, ok, _ := Queue.Get()
		if ok {
			log.Print(val)
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
}
