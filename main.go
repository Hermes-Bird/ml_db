package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

func main() {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go setupServer(&wg)

	wg.Wait()
	dialServer()
	<-time.NewTimer(time.Second * 10).C
}

func setupServer(g *sync.WaitGroup) {
	l, err := net.Listen("tcp", ":9414")
	if err != nil {
		log.Fatal(err)
	}
	g.Done()
	con, err := l.Accept()
	defer con.Close()
	if err != nil {
		log.Fatal(err)
	}

	buf := make([]byte, 1024)
	o := sync.Once{}
	for {
		n, err := con.Read(buf)
		if err != nil {
			o.Do(func() {
				log.Println(err)
			})
		} else {
			fmt.Printf("Str -> %s\n", buf[:n])
			o = sync.Once{}
		}
	}
}

func dialServer() {
	con, err := net.Dial("tcp", ":9414")
	defer con.Close()
	if err != nil {
		log.Fatal(err)
	}

	n, err := con.Write([]byte("Hello world"))
	if err != nil {
		log.Println(n, err)
	}
	<-time.NewTimer(5 * time.Second).C
	con.Write([]byte("Nice ass"))
}
