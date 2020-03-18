package main

import "fmt"

type node struct {
	identity identityType

	master
	slave
}

//NewNode to new a node
func NewNode(capacity int) *node {
	suchNode := new(node)
	// New node should raise election here upon it is created
	return suchNode
}

func (self *node) Run() {
	// OwO the node is too lazy to do anything for now
	switch self.identity {
	case MASTER:
		fmt.Println("This is a MASTER node")
	case SLAVE:
		fmt.Println("This is a SLAVE node")
	}
}
