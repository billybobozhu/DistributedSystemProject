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

//TODO:  BUILD AN ADDRESS LIST TO STORE THE ADDR OF REPLICA SERVER
var addressMap map[string]string //TODO:  BUILD AN ADDRESS MAP TO STORE THE METADATA OF FILES
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

func (self *master) Listen(port string) error {
	var addr string
	addr = "localhost:7878" //default replica server address
	//TODO : SELECT REPLICA SERVER WITH LOWEST DELAY
	Server, err := net.Listen("tcp", port)
	if err != nil {
		// fmt.Println("net.Listen err =", err)
		return err
	}
	defer Server.Close()
	// accept
	for {
		conn, err := Server.Accept()
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
		if string(buf[:n]) == "SEND" {

			fmt.Println("Master Method : SEND")
			conn.Write([]byte("ok"))
			buf := make([]byte, 1024)
			n, err1 := conn.Read(buf)
			if err1 != nil {
				// fmt.Println("conn.Read err =", err1)
				return err1
			}
			fileName := string(buf[:n])
			fmt.Printf("Requesting file name is: %s \n", fileName) // TODO: store the filename MD5 and replica server in a map
			conn.Write([]byte(addr))

		} else if string(buf[:n]) == "RECEIVE" {

			fmt.Println("Master Method : RECEIVE")
			conn.Write([]byte("ok"))
			buf := make([]byte, 1024)
			n, err1 := conn.Read(buf)
			if err1 != nil {
				// fmt.Println("conn.Read err =", err1)
				return err1
			}
			fileName := string(buf[:n])
			fmt.Printf("Requesting file name is: %s \n", fileName) // TODO: store the filename MD5 and replica server in a map
			conn.Write([]byte(addr))

		}
		// get file name

		// 接收文件,
		// err = self.recv(fmt.Sprintf("received_%s", fileName), conn)
	}
}
