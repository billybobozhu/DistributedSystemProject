package main

type identityType int

const (
	//MASTER identifies this node is a master
	MASTER identityType = 0

	//SLAVE identifies this node is a slave
	SLAVE identityType = 1

	//REPLICA identifies this node is a replica
	REPLICA identityType = 2
)
