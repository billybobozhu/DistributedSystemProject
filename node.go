package main

import (
	"container/list"
	"errors"
	"fmt"
)

type node struct {
	identity identityType

	master
	slave
}

//NewNode to new a node
func NewNode(capacity int, identity identityType) *node {
	suchNode := new(node)
	suchNode.capacity = capacity
	suchNode.identity = identity
	suchNode.master.neighbours = list.New()
	suchNode.slave.neighbours = list.New()

	// New node should raise election here upon it is created
	return suchNode
}

func (self *node) Run() error {
	// OwO the node is too lazy to do anything for now
	switch self.identity {

	case MASTER:
		fmt.Println("This is a MASTER node")
		go self.master.HeartBeat()

	case SLAVE:
		fmt.Println("This is a SLAVE node")
		go self.slave.Listen("localhost:8900")

	default:
		return errors.New("Node has illegal identity type")
	}
	return nil
}
