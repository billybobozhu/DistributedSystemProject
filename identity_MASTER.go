package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

type master struct {
	capacity int
}

var addressMap map[string]string

// func (self *master) requestAddr(filePath string, masterdestination string) string {
// 	var destination string

// 	info, err := os.Stat(filePath)
// 	fmt.Println(err)

// 	conn, err1 := net.Dial("tcp", "localhost:8989")
// 	defer conn.Close()
// 	fmt.Println(err1)

// 	conn.Write([]byte(info.Name()))
// 	buf := make([]byte, 1024*8)
// 	n, err2 := conn.Read(buf)
// 	fmt.Println(err2)

// 	fmt.Println("get address", n)
// 	destination = "aaa"

// 	return destination
// }

func (self *master) CreateFile(fileName string, content []byte) error {
	file, err := os.Create(fileName)
	if err != nil {
		// fmt.Println(err)
		return err
	}
	// err := ioutil.WriteFile(fileName, content, 0777)
	// if err != nil{
	// 	fmt.Println(err)
	// }
	n, err := file.Write(content)
	if err == nil && n < len(content) {
		err = io.ErrShortWrite
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (self *master) DeleteFile(fileName string) error {
	err := os.Remove(fileName)
	return err
}

// func (self *master) SendFile(filePath string, destination string) error {
// 	info, err := os.Stat(filePath)
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
// 		err = self.send(filePath, conn)
// 	}
// 	return err
// }

// func (self *master) send(filePath string, conn net.Conn) error {
// 	defer conn.Close()
// 	file, err := os.Open(filePath)
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

func (self *master) Listen(port string) error {
	var addr string
	addr = "localhost:7878"
	Server, err := net.Listen("tcp", port)
	if err != nil {
		// fmt.Println("net.Listen err =", err)
		return err
	}
	defer Server.Close()
	// 接受文件名
	for {
		conn, err := Server.Accept()
		defer conn.Close()
		if err != nil {
			// fmt.Println("Server.Accept err =", err)
			return err
		}
		buf := make([]byte, 1024*10)
		n, err1 := conn.Read(buf)
		if err1 != nil {
			// fmt.Println("conn.Read err =", err1)
			return err1
		}
		// 拿到了文件的名字
		fileName := string(buf[:n])
		fmt.Println(fileName)
		// 返回ok
		// addressMap[fileName] = clientAddress[0]

		conn.Write([]byte(addr))
		// 接收文件,
		// err = self.recv(fmt.Sprintf("received_%s", fileName), conn)
	}
}

// func (self *master) recv(fileName string, conn net.Conn) error {
// 	defer conn.Close()
// 	file, err := os.Create(fileName)
// 	defer file.Close()
// 	if err != nil {
// 		// fmt.Println("os.Create err =", err)
// 		return err
// 	}

// 	// 拿到数据
// 	buf := make([]byte, 1024*10)
// 	for {
// 		n, err := conn.Read(buf)
// 		if n == 0 || err == io.EOF {
// 			// fmt.Println("Transfer Finish", err)
// 			return io.EOF
// 		}
// 		if err != nil {
// 			// fmt.Println("conn.Read err =", err)
// 			return err
// 		}
// 		fmt.Printf("%s: %s", fileName, buf[:n])
// 		file.Write(buf[:n])
// 	}
// }
