package sched

import (
	"fmt"
	data "github.com/Hermes-Bird/ml_db/core/data_structures"
	"github.com/Hermes-Bird/ml_db/core/tx"
	"log"
)

type Scheduler struct {
	Q        *data.SyncQueue
	M        SchedMap
	ShedTxCh <-chan tx.Transaction
	ExecTxCh chan<- tx.Transaction
	DoneTxCh <-chan string
}

func NewScheduler(schedTxChan chan tx.Transaction, execTxChan chan tx.Transaction, doneTxChan chan string) *Scheduler {
	s := &Scheduler{
		Q:        data.NewSyncQueue(),
		M:        *NewSchedMap(),
		ShedTxCh: schedTxChan,
		DoneTxCh: doneTxChan,
		ExecTxCh: execTxChan,
	}

	return s
}

func (s *Scheduler) SetupScheduling() {
	go s.AcceptTransactions()
	go s.ExecTransactions()
	go s.AcceptDoneTxInfo()
}

func (s *Scheduler) AcceptTransactions() {
	for {
		t := <-s.ShedTxCh
		log.Println(fmt.Sprintf("Accepted tx >>> %#v", t))
		s.Q.Queue(t)
	}
}

func (s *Scheduler) AcceptDoneTxInfo() {
	for {
		cn := <-s.DoneTxCh
		s.M.RmTx(cn)
	}
}

func (s *Scheduler) ExecTransactions() {
	for {
		i := 0
		for {
			v, ok := s.Q.At(i)
			if !ok {
				break
			}

			tr, ok := v.(tx.Transaction)
			if !ok {
				log.Println("Failed to upcast tx to tx.Transaction from the Queue", tr)
				continue
			}

			if s.M.SetToExecIfReady(tr) {
				s.Q.RmAt(i)
				log.Println("send tx to exec >>>", tr)
				s.ExecTxCh <- tr
			}

			i++
		}

	}
}
