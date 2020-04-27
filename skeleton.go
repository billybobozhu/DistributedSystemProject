package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"strconv"
	"strings"

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
	
	fmt.Printf("What do you want? PLEASE KEY IN SEND OR RECEIVE OR DELETE OR MODIFY OR APPEND\n")
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
		//f, _ := os.Open(name)
		slave.CutFile(name)
	} else if method == "RECEIVE" || method == "receive" {
		slave.request(name)
		// var destination string
		// destination = slave.requestReplica(name, "localhost:8989")
		// fmt.Println(destination)
		// dest := strings.Split(destination, ",")
		// var length string
		// length = dest[0]
		// fmt.Println(length)
		// limit, _ := strconv.Atoi(length)
		// fmt.Println(dest[1])
		// var subfiles []string
		// for i := 0; i < limit; i++ {
		// 	var subfilename string
		// 	subfilename = fmt.Sprintf("%s%s%s", strconv.Itoa(i+1), ",", name)
		// 	fmt.Println(subfilename)
		// 	//subfiles = append(subfiles, subfilename)
		// 	subfiles = append(subfiles,subfilename)
		// 	//fmt.Println(path.Join(filePath,subfilename))
		// 	slave.requestFile(subfilename, dest[i+1])
		// }

		// //Join(subfiles, fmt.Sprintf("%s%s", "received_from_replica_", name))
		// Join(subfiles, fmt.Sprintf("%s", name))

		// // slave.requestFile("1,a.txt", "localhost:7880")
		// // slave.requestFile("2,a.txt", "localhost:7878")
		// // slave.requestFile("3,a.txt", "localhost:7880")
		// // slave.requestFile("4,a.txt", "localhost:7880")

	} else if method == "DELETE" || method == "delete" {
		var destination string
		destination = slave.deleteFileCotentPage(name, "localhost:8989")
		fmt.Println(destination)
		dest := strings.Split(destination, ",")
		var length string
		length = dest[0]
		fmt.Println(length)
		limit, _ := strconv.Atoi(length)
		fmt.Println(dest[1])
		var subfiles []string
		for i := 0; i < limit; i++ {
			var subfilename string
			subfilename = fmt.Sprintf("%s%s%s", strconv.Itoa(i+1), ",", name)
			fmt.Println(subfilename)
			subfiles = append(subfiles, subfilename)
			slave.delete(subfilename, dest[i+1])
		}

	}else if method == "MODIFY" || method == "modify"{
		slave.modify(name)
	}else if method == "APPEND" || method == "append"{
		slave.append(name)
	}

	// var content = []byte("OwO Hello I am some random file")
	// client.CreateFile("hello.txt", content)
	// client.DeleteFile("hello.txt")

	// var pause string
	wg.Wait()

	fmt.Scanln()
}

// func SendFile(filePath string,destination string) {
// 	conn, err1 := net.Dial("tcp", "localhost:8989")
// 	defer conn.Close()
// 	if err1 != nil {
// 		fmt.Println(err1)
// 	}
// 	file, err := os.Open(path.Join(Path,filePath))
// 	defer file.Close()
// 	if err != nil {
// 		// fmt.Println("os.Open err = ", err)
// 		fmt.Println(err)
// 	}
// 	buf := make([]byte, 1024*8)
// 	for {
// 		//  打开之后读取文件
// 		n, err := file.Read(buf)
// 		if err != nil {
// 			// fmt.Println("fs.Open err = ", err)
// 			fmt.Println(err)
// 		}
// 		//  发送文件
// 		conn.Write(buf[:n])
// 	}
// }

// func send(filePath string, conn net.Conn) error {
// 	defer conn.Close()
// 	var Path string
// 	file, err := os.Open(path.Join(Path,filePath))
// 	defer file.Close()
// 	if err != nil {
// 		// fmt.Println("os.Open err = ", err)
// 		return err
// 	}
// 	buf := make([]byte, 1024*8)
// 	for {
// 		//  打开之后读取文件
// 		n, err := file.Read(buf)
// 		if err != nil {
// 			// fmt.Println("fs.Open err = ", err)
// 			return err
// 		}
// 		//  发送文件
// 		conn.Write(buf[:n])
// 	}
// }
// func SendFile(filePath string, destination string) error {
// 	info, err := os.Stat(path.Join(Path,filePath))
// 	if err != nil {
// 		// fmt.Println("os.Stat err = ", err)
// 		return err
// 	}
// 	conn, err1 := net.Dial("tcp", destination)
// 	defer conn.Close()
// 	if err1 != nil {
// 		// fmt.Println("net.Dial err = ", err1)
// 		return err1
// 	}
// 	// conn.Write([]byte("SEND"))
// 	conn.Write([]byte(info.Name()))
// 	// 接受到是不是ok
// 	buf := make([]byte, 1024)
// 	n, err2 := conn.Read(buf)

// 	if err2 != nil {
// 		// fmt.Println("conn.Read err = ", err2)
// 		return err2
// 	}
// 	if string(buf[:n]) == "ok" {
// 		fmt.Println("Filename sent")
// 		err = send(filePath, conn)
// 	}
// 	return err
// }


// func requestAddr(filePath string, masterdestination string) string {
// 	var destination string

// 	//info, err := os.Stat(filePath)
// 	info, err := os.Stat(path.Join(Path,filePath))
// 	if err != nil {
// 		fmt.Println(err)
// 	}

// 	conn, err1 := net.Dial("tcp", "localhost:8989")
// 	defer conn.Close()
// 	if err1 != nil {
// 		fmt.Println(err1)
// 	}
// 	conn.Write([]byte("SEND"))

// 	// var s string
// 	// s = fmt.Sprintf("%s%s", "list_", info.Name())
// 	// conn.Write([]byte(s))
// 	// self.sendContentPage(s, "localhost:8989")
// 	buf := make([]byte, 1024)
// 	n, err2 := conn.Read(buf)
// 	if err2 != nil {
// 		// fmt.Println("conn.Read err = ", err2)
// 		fmt.Println(err2)
// 	}
// 	fmt.Println("receive status: ", string(buf[:n]))
// 	if string(buf[:n]) == "ok" {
// 		conn.Write([]byte(info.Name()))

// 		n, err2 := conn.Read(buf)
// 		if err2 != nil {
// 			// fmt.Println("conn.Read err = ", err2)
// 			fmt.Println(err2)
// 		}
// 		fmt.Printf("The address of replica server is %s \n", buf[:n])
// 		destination = string(buf[:n])

// 	}
// 	return destination

// }

func Join(filenames []string, targetfile string) {

	f, err := os.Create(targetfile)
	if err != nil {
		fmt.Println(err)
	}
	for i := 0; i < len(filenames); i++ {
		m, err := os.Open(filenames[i])
		if err != nil {
			fmt.Println(err)
		}
		bytes, err := ioutil.ReadAll(m)
		if err != nil {
			fmt.Println(err)
		}
		f.Write(bytes)
	}
	f.Close()
	fmt.Println("finish combined")
}
