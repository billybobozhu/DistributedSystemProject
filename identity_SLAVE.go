package main

import (
	"container/list"
	"fmt"
	"io"
	"net"
	"os"
)

type slave struct {
	capacity   int
	neighbours *list.List
}

func (self *slave) CreateFile(fileName string, content []byte) error {
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

func (self *slave) DeleteFile(fileName string) error {
	err := os.Remove(fileName)
	return err
}

func (self *slave) SendFile(filePath string, destination string) error {
	info, err := os.Stat(filePath)
	if err != nil {
		// fmt.Println("os.Stat err = ", err)
		return err
	}

	conn, err1 := net.Dial("tcp", destination)
	defer conn.Close()
	if err1 != nil {
		// fmt.Println("net.Dial err = ", err1)
		return err1
	}
	//Tell the other side this is going to send a file
	conn.Write([]byte(FILE))
	// 接受到是不是ok
	buf := make([]byte, 1024)
	n, err2 := conn.Read(buf)
	if err2 != nil {
		debugPrintf("slave: SendFile conn.Read err = %s", err2)
		return err2
	}
	if string(buf[:n]) == "ok" {
		conn.Write([]byte(info.Name()))
		fmt.Println("Filename sent")
		err = self.send(filePath, conn)
	}
	return err
}

func (self *slave) send(filePath string, conn net.Conn) error {
	defer conn.Close()
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		// fmt.Println("os.Open err = ", err)
		return err
	}
	buf := make([]byte, 1024*8)
	for {
		//  打开之后读取文件
		n, err := file.Read(buf)
		if err != nil {
			// fmt.Println("fs.Open err = ", err)
			return err
		}

		//  发送文件
		conn.Write(buf[:n])
	}
}

func (self *slave) RecvFile(conn net.Conn) error {
	debugPrintf("slave: RecvFile from %s", conn.RemoteAddr().String())
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	// Received file name
	fileName := string(buf[:n])
	debugPrintf("slave received filename: %s\n", fileName)

	// receive file
	debugPrintf("slave: going to receive file %s from %s\n", fileName, conn.RemoteAddr().String())
	if DEBUG {
		fileName = fmt.Sprintf("receive_%s", fileName)
	}
	err = self.recv(fileName, conn)
	return err
}

func (self *slave) recv(fileName string, conn net.Conn) error {
	defer conn.Close()
	file, err := os.Create(fileName)
	defer file.Close()
	if err != nil {
		// fmt.Println("os.Create err =", err)
		return err
	}

	// 拿到数据
	buf := make([]byte, 1024*10)
	for {
		n, err := conn.Read(buf)
		if n == 0 || err == io.EOF {
			// fmt.Println("Transfer Finish", err)
			return io.EOF
		}
		if err != nil {
			// fmt.Println("conn.Read err =", err)
			return err
		}
		fmt.Printf("%s: %s", fileName, buf[:n])
		file.Write(buf[:n])
	}
}

func (self *slave) Listen(port string) error {
	slaveListener, err := net.Listen("tcp", port)
	debugPrintf("slave: listening %s\n", port)
	if err != nil {
		// fmt.Println("net.Listen err =", err)
		return err
	}
	defer slaveListener.Close()

	// Begin listening
	for {
		conn, err := slaveListener.Accept()
		defer conn.Close()
		if err != nil {
			// fmt.Println("Server.Accept err =", err)
			return err
		}
		buf := make([]byte, 1024)
		n, err1 := conn.Read(buf)
		if err1 != nil {
			// fmt.Println("conn.Read err =", err1)
			return err1
		}
		// Received message Type
		recvMsgType := messageType(buf[:n])
		debugPrintf("slave listener received message type: %s\n", recvMsgType)

		// Reply ok
		// conn.Write([]byte("ok"))

		switch recvMsgType {

		case HEARTBEAT:
			go func() {
				debugPrintf("slave: received HEARTBEAT from %s \n", conn.RemoteAddr().String())
			}()

		case FILE:
			debugPrintf("slave: go RecvFile\n")
			// receive file
			go func(conn net.Conn) error {
				err = self.RecvFile(conn)
				return err
			}(conn)
		}

	}
}
