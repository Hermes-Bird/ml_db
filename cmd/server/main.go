package main

import (
	"github.com/Hermes-Bird/ml_db/core"
	"github.com/Hermes-Bird/ml_db/core/tx"
	"github.com/Hermes-Bird/ml_db/exec"
	"github.com/Hermes-Bird/ml_db/sched"
	"github.com/Hermes-Bird/ml_db/server"
)

func main() {
	cmdExecutor := core.NewCommandExecutor()

	schedTxChan := make(chan tx.Transaction, 50)
	execTxChan := make(chan tx.Transaction, 50)
	executedTxResChan := make(chan tx.TxResultData, 50)
	doneTxChan := make(chan string, 50)

	txScheduler := sched.NewScheduler(schedTxChan, execTxChan, doneTxChan)
	txExecutor := exec.NewTxExecutor(cmdExecutor, execTxChan, executedTxResChan, doneTxChan)

	s := server.NewDbServer(schedTxChan, executedTxResChan)

	go txExecutor.AcceptTransactions()
	go txScheduler.SetupScheduling()
	go s.HandleTxResults()

	s.SetupController()
}
