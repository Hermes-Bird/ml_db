package exec

import (
	"github.com/Hermes-Bird/ml_db/core"
	"github.com/Hermes-Bird/ml_db/core/tx"
	"github.com/Hermes-Bird/ml_db/files"
	"log"
	"time"
)

type TxExecutor struct {
	Cmd        *core.CommandExecutor
	FileKeeper files.FileKeeper
	ExecTxChan <-chan tx.Transaction
	ResTxChan  chan<- tx.TxResultData
	DoneTxChan chan<- string
}

func NewTxExecutor(cmd *core.CommandExecutor, execTxChan chan tx.Transaction, resChan chan tx.TxResultData, doneTxChan chan string) *TxExecutor {
	return &TxExecutor{
		Cmd:        cmd,
		ExecTxChan: execTxChan,
		ResTxChan:  resChan,
		DoneTxChan: doneTxChan,
		FileKeeper: files.NewFileKeeperImpl(),
	}
}

func (e *TxExecutor) AcceptTransactions() {
	for {
		t := <-e.ExecTxChan
		go e.ExecTx(t)
	}
}

func (e *TxExecutor) ExecTx(t tx.Transaction) {
	log.Println("Get tx to execute", t)
	res := tx.TxResultData{
		ConId: t.ConId,
		TxT:   t.TxT,
		Op:    t.Op.Command,
	}

	start := time.Now()

	var n int
	var err error
	var data [][]byte

	f, err := e.FileKeeper.GetFileByCollection(t.Op.Collection)
	if err != nil {
		log.Println(err, "error opening file")
		res.Err = err
		res.DoneIn = time.Now().UnixMicro() - start.UnixMicro()
		e.ResTxChan <- res
		e.DoneTxChan <- t.Op.Collection
		return
	}

	t.Op.File = f

	switch t.Op.Command {
	case "UPDATE":
		n, err = e.Cmd.Update(t.Op)
	case "PATCH":
		n, err = e.Cmd.Update(t.Op)
	case "DELETE":
		n, err = e.Cmd.Delete(t.Op)
	case "SEARCH":
		data, err = e.Cmd.Search(t.Op)
	case "INSERT":
		n, err = e.Cmd.Insert(t.Op)
	default:
	}

	e.FileKeeper.DoneCollectionTask(t.Op.Collection)

	res.RowsAffected = n
	res.Err = err
	res.Res = data
	res.DoneIn = time.Now().UnixMicro() - start.UnixMicro()

	e.ResTxChan <- res
	e.DoneTxChan <- t.Op.Collection
}
