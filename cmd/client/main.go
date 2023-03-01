package main

import (
	"github.com/Hermes-Bird/ml_db/proto"
	"log"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", ":9414")
	if err != nil {
		log.Fatalln("Errr")
	}

	myproto := proto.NewTcpProto(conn)
	err = myproto.SendMessage([]byte(`
	COLLECTION name
	INSERT
	DATA {"data":"my new super data"}
	`))
	if err != nil {
		log.Println("send")
		return
	}
	message, err := myproto.GetMessage()
	if err != nil {
		log.Println("get")
	}
	log.Println(message)
}
