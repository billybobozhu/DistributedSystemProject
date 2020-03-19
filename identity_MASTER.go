package main

import (
	"container/list"
	"fmt"
	"net"
	"os"
	"time"
)

type master struct {
	neighbours *list.List
}

func (self *master) HeartBeat() error {

	for {
		for friend := self.neighbours.Front(); friend != nil; friend = friend.Next() {
			conn, err1 := net.Dial("tcp", fmt.Sprintf("%s", friend.Value))
			if err1 != nil {
				return err1
			}
			msg, err := conn.Write([]byte(HEARTBEAT))
			if err != nil {
				debugPrintf("master: %s Fatal error: %s\n", conn.RemoteAddr().String(), err)
				os.Exit(1)
			}
			debugPrintf("master: the other side received: %s\n", msg)
		}
		time.Sleep(2 * time.Second)
	}
}
