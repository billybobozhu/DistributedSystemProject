package main

import (
	"errors"
	"fmt"
)

type node struct {
	identity identityType
	master
	slave
	replica
	replica1
	
}

//NewNode to new a node
func NewNode(capacity int, identity identityType) *node {
	suchNode := new(node)
	// suchNode.capacity = capacity
	suchNode.identity = identity
	// New node should raise election here upon it is created
	return suchNode
}

func (self *node) Run() error {
	// OwO the node is too lazy to do anything for now
	switch self.identity {

	case MASTER:

		go self.master.Listen("localhost:8989")
		fmt.Println("This is a master node")

	case SLAVE:
		fmt.Println("This is a slave node")
		//go self.slave.Listen("localhost:7879")
		go self.slave.SlaveMain()

	case REPLICA:
		fmt.Println("This is a replica node 1")
		go self.replica.Listen1("localhost:7878",1)

	case REPLICA1:
		fmt.Println("This is a replica node 2")
		go self.replica.Listen1("localhost:7880",2)

	default:
		return errors.New("Node has illegal identity type")
	}
	return nil
}
