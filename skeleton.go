package main

import (
	"fmt"
	"sync"
)

func main() {

	client1 := NewNode(233, MASTER)
	client2 := NewNode(4096, SLAVE)

	fmt.Printf("client1 capacity: %d\n", client1.capacity)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		client1.Run()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		client2.Run()
		wg.Done()
	}()

	client1.SendFile("hello.txt", "localhost:8900")

	// var content = []byte("OwO Hello I am some random file")
	// client.CreateFile("hello.txt", content)
	// client.DeleteFile("hello.txt")

	// var pause string
	wg.Wait()

	fmt.Scanln()
}
