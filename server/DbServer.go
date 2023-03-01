package server

import (
	"fmt"
	"github.com/Hermes-Bird/ml_db/core/tx"
	"github.com/Hermes-Bird/ml_db/json_handler"
	"github.com/Hermes-Bird/ml_db/parser"
	"github.com/Hermes-Bird/ml_db/proto"
	"log"
	"math/rand"
	"net"
	"sync"
)

type DbServer struct {
	ConLocker   sync.Mutex
	Connections map[int]proto.Proto
	// Channel of txScheduler
	SchedTxChan chan<- tx.Transaction
	// Channel of executed transactions for TxExecutor
	ExecutedTxChan <-chan tx.TxResultData
	Parser         *parser.Parser
}

func NewDbServer(schedChan chan tx.Transaction, execChan chan tx.TxResultData) *DbServer {
	return &DbServer{
		ConLocker:      sync.Mutex{},
		Connections:    map[int]proto.Proto{},
		SchedTxChan:    schedChan,
		ExecutedTxChan: execChan,
		Parser:         &parser.Parser{},
	}
}

func (s *DbServer) SetupController() {
	socket, err := net.Listen("tcp", ":9414")
	if err != nil {
		log.Fatal("Error while establishing connection")
	}

	log.Println("Start listening on port 9414")

	for {
		con, err := socket.Accept()
		if err != nil {
			log.Println(err)
		} else {
			go s.HandleConnection(con)
		}
	}
}

func (s *DbServer) HandleConnection(con net.Conn) {
	id := rand.Int()
	log.Println("Got a new connection", con.RemoteAddr(), "id >>> ", id)
	tcpProto := proto.NewTcpProto(con)

	s.ConLocker.Lock()
	// TODO add connection id there
	s.Connections[id] = tcpProto
	s.ConLocker.Unlock()

	for {
		msg, err := tcpProto.GetMessage()
		if err != nil {
			log.Println("Error while listening connection >>>", err)
			break
		}
		fmt.Printf("Decode msg from connection %d -> ` %s ` \n", id, msg)
		go s.AcceptMessage(msg, id)
	}

	s.ConLocker.Lock()

	delete(s.Connections, id)
	tcpProto.Close()

	s.ConLocker.Unlock()
}

func (s *DbServer) AcceptMessage(msg string, conId int) {
	log.Println("Value accepted <<<<", msg, conId)
	op, err := s.Parser.ParseExpression(msg)
	if err != nil {
		fmt.Printf("Error parsing expression \"%s\" from %d: %s", msg, conId, err.Error())
		return
	}

	tr := tx.Transaction{
		TxId:  1,
		TxT:   tx.GetTxType(op.Command),
		ConId: conId,
		Op:    *op,
	}

	s.SchedTxChan <- tr
}

func (s *DbServer) HandleTxResults() {
	for res := range s.ExecutedTxChan {
		go func(res tx.TxResultData) {
			s.ConLocker.Lock()
			con := s.Connections[res.ConId]
			s.ConLocker.Unlock()

			b, err := json_handler.ToJsonBytes(res)
			if err != nil {
				log.Println("Error while parsing to json ", err)
			}
			err = con.SendMessage(b)
			if err != nil {
				log.Printf("Erorr while sending message to %d connection \n", res.ConId)
			}
		}(res)
	}
}
