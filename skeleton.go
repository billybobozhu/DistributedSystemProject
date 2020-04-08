package main

import (
	"fmt"
	"sync"
)

func main() {
	var method string
	var name string
	fmt.Printf("What do you want? PLEASE KEY IN SEND OR RECEIVE \n")
	fmt.Scanln(&method) //Scanln 扫描来自标准输入的文本，将空格分隔的值依次存放到后续的参数内，直到碰到换行。
	// fmt.Scanf("%s %s", &firstName, &lastName)    //Scanf与其类似，除了 Scanf 的第一个参数用作格式字符串，用来决定如何读取。
	fmt.Printf("Enter File Name \n")
	fmt.Scanln(&name)

	slave := NewNode(233, SLAVE)
	master1 := NewNode(4096, MASTER)
	// replica := NewNode(4096, REPLICA)
	replica1 := NewNode(4096, REPLICA1)

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

	// wg.Add(1)
	// go func() {
	// 	replica.Run()
	// 	wg.Done()
	// }()

	wg.Add(1)
	go func() {
		replica1.Run()
		wg.Done()
	}()

	if method == "SEND" || method == "send" {

		var destination string
		destination = slave.requestAddr(name, "localhost:8989")
		slave.SendFile(name, destination)
	} else if method == "RECEIVE" || method == "receive" {

		var destination string
		destination = slave.requestReplica(name, "localhost:8989")
		slave.requestFile(name, destination)
	}

	// var content = []byte("OwO Hello I am some random file")
	// client.CreateFile("hello.txt", content)
	// client.DeleteFile("hello.txt")

	// var pause string
	wg.Wait()

	fmt.Scanln()
}
