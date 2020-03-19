package main

import (
	"fmt"
	"os"
)

//DEBUG falg only used by the node struct.
//To switch debug mode on or off.
const DEBUG = true

//debugPrintf only prints message if DEBUG flag in nodeUtils.go is set to true.
//Otherwise it doesn't want to do anything OwO
func debugPrintf(format string, a ...interface{}) (n int, err error) {
	if DEBUG {
		return fmt.Fprintf(os.Stdout, format, a...)
	}
	return
}

type identityType string

const (
	//MASTER identifies this node is a master
	MASTER identityType = "MASTER"

	//SLAVE identifies this node is a slave
	SLAVE identityType = "SLAVE"
)

type messageType string

const (
	//HEARTBEAT indicates this is a heartbeat message
	HEARTBEAT messageType = "HEARTBEAT"

	//FILE indicates a file is going to be sent
	FILE messageType = "FILE"
)
