package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
)

func split(buf []byte, lim int) [][]byte {
	var chunk []byte
	chunks := make([][]byte, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:len(buf)])
	}
	//fmt.Println(len(chunks))
	return chunks
}

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
	replica := NewNode(4096, REPLICA)
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

	wg.Add(1)
	go func() {
		replica.Run()
		wg.Done()
	}()

	wg.Add(1)
	go func() {

		replica1.Run()
		wg.Done()
	}()

	if method == "SEND" || method == "send" {
		f, _ := os.Open(name)
		var libname string
		var subfile string
		libname = fmt.Sprintf("%s%s", "list_", name)
		fileObj, _ := os.OpenFile(libname, os.O_CREATE, 0644)
		fileObj.Close()
		bytes, _ := ioutil.ReadAll(f)

		result := split(bytes, 50000)
		for i := 0; i < len(result); i++ {
			d1 := []byte(result[i])
			subfile = fmt.Sprintf("%s%s%s", strconv.Itoa(i+1), ",", name)

			err := ioutil.WriteFile(subfile, d1, 0644)
			if err != nil {
				fmt.Println(err)
			}
			fileObj1, _ := os.OpenFile(libname, os.O_APPEND, 0644)
			fileObj1.Write([]byte(subfile))
			fileObj1.Write([]byte("\n"))

		}

		var s string
		s = fmt.Sprintf("%s%s", "list_", name)
		// slave.sendContentPage(s, "localhost:8989")
		file, err := os.Open(s)
		if err != nil {
			fmt.Println(err)
		}

		defer file.Close()

		var lines []string
		linecount := 0
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
			linecount++
		}

		for i := 0; i < len(lines); i++ {
			var destination string
			destination = slave.requestAddr(lines[i], "localhost:8989")
			slave.SendFile(lines[i], destination)
		}
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
