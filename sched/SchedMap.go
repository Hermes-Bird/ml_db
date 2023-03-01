package sched

import (
	"github.com/Hermes-Bird/ml_db/core/tx"
	"log"
	"sync"
)

type SchedMap struct {
	M          sync.Mutex
	State      map[string]tx.TxType
	ExTxAmount map[string]int
}

func NewSchedMap() *SchedMap {
	return &SchedMap{
		M:          sync.Mutex{},
		State:      map[string]tx.TxType{},
		ExTxAmount: map[string]int{},
	}
}

func (s *SchedMap) SetToExecIfReady(t tx.Transaction) bool {
	s.M.Lock()
	defer s.M.Unlock()

	cn := t.Op.Collection

	cs := s.State[cn]
	switch cs {
	case tx.NoTxType:
		s.State[cn] = t.TxT
		s.ExTxAmount[cn] += 1
		return true
	case tx.RTxType:
		if t.TxT == tx.RTxType {
			s.ExTxAmount[cn] += 1
			return true
		}
		return false
	case tx.WTxType:
		return false
	}

	log.Printf("Invalid state of a transaction %#v", t)

	return false
}

func (s *SchedMap) RmTx(cn string) {
	s.M.Lock()
	defer s.M.Unlock()

	n := s.ExTxAmount[cn]
	if n == 0 {
		log.Printf("Trying to decrease tx amount while it 0, collection -> %s", cn)
	}

	n = n - 1
	if n == 0 {
		s.State[cn] = tx.NoTxType
	}
}
