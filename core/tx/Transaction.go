package tx

import "os"

type TxType = uint8

const (
	NoTxType = iota
	RTxType
	WTxType
)

type Operation struct {
	File       *os.File
	Collection string
	Command    string
	Data       []byte
	Condition  []byte
}

type TxResultData struct {
	ConId        int
	RowsAffected int
	DoneIn       int64
	TxT          TxType
	Res          [][]byte
	Op           string
	Err          error
}

type Transaction struct {
	TxId  any
	ConId int
	TxT   TxType
	Op    Operation
}

func GetTxType(cmd string) TxType {
	switch cmd {
	case "UPDATE":
	case "INSERT":
	case "PATCH":
	case "DELETE":
		return WTxType
	default:
		return RTxType
	}

	return NoTxType
}
