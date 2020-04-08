package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
)

type master struct {
	capacity int
}

//TODO:  BUILD AN ADDRESS LIST TO STORE THE ADDR OF REPLICA SERVER
// var addressMap map[string]string //TODO:  BUILD AN ADDRESS MAP TO STORE THE METADATA OF FILES
// func (self *master) initializeAddr() map[string]string {
// 	addressMap := make(map[string]string, 100)

// 	return addressMap

// }
// func (self *master) AddToMap(fileName string, ipaddress string) map[string]string {
// 	self.addressMap[fileName] = ipaddress

// 	return self.addressMap

// }
// func (self *master) FindAddr(fileName string, addressMap map[string]string) string {
// 	var ipaddress string
// 	// x, _ := addressMap[fileName] // found == true

// 	if x, found := addressMap[fileName]; found {
// 		ipaddress = x
// 	}
// 	return ipaddress

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

func (self *master) Listen(port string) error {

	var chunknum int
	addressMap := make(map[string]string, 100)
	//chunkMap := make(map[string]string, 100)
	var addr string

	balance1, _ := ioutil.ReadFile("storage1.txt")
	balance2, _ := ioutil.ReadFile("storage2.txt")

	load1, _ := strconv.Atoi(string(balance1))
	load2, _ := strconv.Atoi(string(balance2))

	//addr = "localhost:7878" //default replica server address
	//addr = "localhost:7880" //default replica1 server address
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
			// buf1 := make([]byte, 1024)
			// n, err1 := conn.Read(buf1)
			// if err1 != nil {
			// 	// fmt.Println("conn.Read err =", err1)
			// 	return err1
			// }
			// // 拿到了文件的名字
			// fileName := string(buf[:n])
			// self.recv(fileName, c)
			if load1 >= load2 {
				addr = "localhost:7878"
				load1 = load1 - 1
				file, _ := os.Create("storage1.txt")
				file.Write([]byte(strconv.Itoa(load1)))
				chunknum = chunknum + 1

			} else {
				addr = "localhost:7880"
				load2 = load2 - 1
				file, _ := os.Create("storage2.txt")
				file.Write([]byte(strconv.Itoa(load2)))
				chunknum = chunknum + 1
			}

			fmt.Println("Master Method : SEND , Chunk Number: ", chunknum)

			conn.Write([]byte("ok"))
			buf := make([]byte, 1024)
			n, err1 := conn.Read(buf)
			if err1 != nil {
				// fmt.Println("conn.Read err =", err1)
				return err1
			}
			fileName := string(buf[:n])
			// var d string
			// d = fmt.Sprintf("%s%s", "contentpage_", fileName)
			ss := strings.Split(fileName, ",")
			realname := ss[1]
			var f string
			f = fmt.Sprintf("%s%s", realname, "_content.txt")
			if chunknum == 1 {

				fileObj, _ := os.OpenFile(f, os.O_CREATE, 0644)
				var d string
				d = fmt.Sprintf("%s%s", realname, "\n")

				fileObj.Write([]byte(d))
			}
			fileObj1, _ := os.OpenFile(f, os.O_APPEND, 0644)

			fileObj1.Write([]byte(fileName))
			fileObj1.Write([]byte("\n"))
			fmt.Printf("Requesting file name is: %s \n", fileName) // TODO: store the filename MD5 and replica server in a map
			conn.Write([]byte(addr))

			addressMap[fileName] = addr
			var s string
			s = fmt.Sprintf("%s %s", fileName, ".json")
			out, _ := json.Marshal(addressMap)
			_ = ioutil.WriteFile(s, out, 0755)
			//self.AddToMap(fileName, addr)

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
			var s string
			s = fmt.Sprintf("%s %s", fileName, ".json")
			data, _ := ioutil.ReadFile(s)

			err := json.Unmarshal(data, &addressMap)
			if err != nil {

				fmt.Println("Umarshal failed:", err)

			}
			fmt.Println("current addr mapping: ", addressMap)
			var ipaddress string
			if x, found := addressMap[fileName]; found {
				ipaddress = x
			}

			conn.Write([]byte(ipaddress))

		}
		// get file name

		// 接收文件,
		// err = self.recv(fmt.Sprintf("received_%s", fileName), conn)
	}
}

func (self *master) recv(fileName string, conn net.Conn) error {
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
