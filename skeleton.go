package main

import (
	"fmt"
	"sync"
)

func main() {

	slave := NewNode(233, SLAVE)
	master1 := NewNode(4096, MASTER)
	replica := NewNode(4096, REPLICA)

	// fmt.Printf("client1 capacity: %d\n", client1.capacity)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		master1.Run()
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		slave.Run()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		replica.Run()
		wg.Done()
	}()
	var destination string
	destination = slave.requestAddr("hello.txt", "localhost:8989")
	fmt.Println("target dest is:", destination)
	// b := []byte(destination)
	// slave.CreateFile("anc.txt", b)
	slave.SendFile("hello.txt", destination)

	// var content = []byte("OwO Hello I am some random file")
	// client.CreateFile("hello.txt", content)
	// client.DeleteFile("hello.txt")

	// var pause string
	wg.Wait()

	fmt.Scanln()
}
