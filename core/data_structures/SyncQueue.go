package data

import (
	"log"
	"sync"
)

type SyncQueue struct {
	M *sync.Mutex
	Q *Queue
}

func NewSyncQueue() *SyncQueue {
	return &SyncQueue{
		M: &sync.Mutex{},
		Q: NewQueue(),
	}
}

func (sq *SyncQueue) Queue(v any) {
	sq.M.Lock()
	sq.Q.Queue(v)
	sq.M.Unlock()
}

func (sq *SyncQueue) Unqueue() any {
	sq.M.Lock()
	defer sq.M.Unlock()
	return sq.Q.Unqueue()
}

func (sq *SyncQueue) At(n int) (any, bool) {
	sq.M.Lock()
	defer sq.M.Unlock()

	cur := sq.Q.List

	if cur == nil {
		return nil, false
	}

	for i := 0; i != n; i++ {
		if cur.Next == nil {
			return nil, false
		}
		cur = cur.Next
	}

	return cur.Val, true
}

func (sq *SyncQueue) RmAt(n int) {
	sq.M.Lock()
	defer sq.M.Unlock()
	log.Println("remove from list at >>>", n)
	cur := sq.Q.List
	if n == 0 {
		sq.Q.List = nil
	}

	for i := 0; i != n-1; i++ {
		if cur.Next == nil {
			return
		}
		cur = cur.Next
	}

	if cur.Next != nil {
		cur.Next = cur.Next.Next
	}
}
