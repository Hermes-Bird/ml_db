package proto

import "io"

type Proto interface {
	GetMessage() (string, error)
	SendMessage(b []byte) error
	io.Closer
}
