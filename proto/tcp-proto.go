package proto

import (
	"bytes"
	"io"
	"log"
	"net"
)

var TcpProtoMsgStart = []byte("s||")
var TcpProtoMsgEnd = []byte("||e")

type TcpProto struct {
	Con net.Conn
}

func NewTcpProto(con net.Conn) *TcpProto {
	return &TcpProto{Con: con}
}

// TODO fix logic
func (p *TcpProto) GetMessage() (string, error) {
	buf := make([]byte, 1024)
	for {
		log.Println(string(buf))
		n, err := p.Con.Read(buf)
		if err == io.EOF {
			return "", err
		}
		b := buf[:n]

		startInd := bytes.Index(buf, TcpProtoMsgStart)
		endInd := bytes.Index(buf, TcpProtoMsgEnd)
		if startInd != -1 && endInd != -1 {
			return string(b[startInd+len(TcpProtoMsgStart) : endInd]), nil
		}
	}
}

func (p *TcpProto) SendMessage(msg []byte) error {
	buf := make([]byte, 0, len(TcpProtoMsgStart)+len(msg)+len(TcpProtoMsgEnd))

	buf = append(buf, TcpProtoMsgStart...)
	buf = append(buf, msg...)
	buf = append(buf, TcpProtoMsgEnd...)

	_, err := p.Con.Write(buf)
	return err
}

func (p *TcpProto) Close() error {
	return p.Con.Close()
}
