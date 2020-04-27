package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
	"path"
)

type replica struct {
	capacity int
}

type replica1 struct {
	capacity int
}


func (self *replica) DeleteFile1(fileName string,Path string) error {
	//err := os.Remove(fileName)
	err := os.Remove(path.Join(Path,fileName))
	return err
}

func (self *replica) SendFile1(filePath string,Path string, destination string) error {
	info, err := os.Stat(path.Join(Path,filePath))
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
	// conn.Write([]byte("SEND"))
	conn.Write([]byte(info.Name()))
	// 接受到是不是ok
	buf := make([]byte, 1024)
	n, err2 := conn.Read(buf)

	if err2 != nil {
		// fmt.Println("conn.Read err = ", err2)
		return err2
	}
	if string(buf[:n]) == "ok" {
		fmt.Println("Filename sent")
		err = self.send1(filePath, Path,conn)
	}
	return err
}

func (self *replica) send1(filePath string,Path string, conn net.Conn) error {
	defer conn.Close()
	file, err := os.Open(path.Join(Path,filePath))
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

func (self *replica) Listen1(port string,replicaid int) error {
	var PathStorage1 ="/Users/liuzh/Downloads/DistributedSystemProject-bobozhu/storage1/"
	var PathStorage2 ="/Users/liuzh/Downloads/DistributedSystemProject-bobozhu/storage2/"
	var Path string
	if replicaid==1{
		Path=PathStorage1
	}else{
		Path=PathStorage2
	}
	//fmt.Println("paththththt",Path)
	Server, err := net.Listen("tcp", port)
	if err != nil {
		// fmt.Println("net.Listen err =", err)
		return err
	}
	defer Server.Close()
	// 接受文件名
	for {
		conn, err := Server.Accept()
		defer func() {
			conn.Close()
			fmt.Println("connnction replica closed")
		}()
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
		if string(buf[:n]) == "RECEIVE" {
			fmt.Println("Relica receive command: RECEIVE, FINDING TARGET FILE")
			conn.Write([]byte("ok"))
			n, err1 := conn.Read(buf)
			if err1 != nil {
				// fmt.Println("conn.Read err =", err1)
				return err1
			}
			fileName := string(buf[:n])
			var storedName string
			storedName = fileName
			fmt.Println("received message is:", fileName)
			err = self.send1(storedName,Path, conn)
		} else if string(buf[:n]) == "DELETE" {
			fmt.Println("Relica receive command: DELETE, FINDING TARGET FILE")
			conn.Write([]byte("ok"))
			n, err1 := conn.Read(buf)
			if err1 != nil {
				// fmt.Println("conn.Read err =", err1)
				return err1
			}
			fileName := string(buf[:n])
			//var storedName string
			//storedName = fmt.Sprintf("received_from_client%s", fileName)
			fmt.Println("Deleting: ", fileName)
			//err = self.DeleteFile1(storedName)
			err = self.DeleteFile1(fileName,Path)
			if err != nil {
				fmt.Println(err)
			}

		} else {
			fileName := string(buf[:n])
			conn.Write([]byte("ok"))
			//err = self.recv1(fmt.Sprintf("received_from_client%s", fileName), conn)
			err = self.recv1(fileName,Path,conn)
		}

	}
}

func (self *replica) recv1(fileName string,Path string, conn net.Conn) error {
	defer conn.Close()
	//file, err := os.Create(fileName)
	//fmt.Println(path.Join(PathSlave,fileName))
	// fmt.Println("Rrrrrrrrrrrrr",Path)
	file, err := os.Create(path.Join(Path,fileName))
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
		fmt.Printf("%s", fileName)
		file.Write(buf[:n])
		//err = self.recv(fmt.Sprintf("received_from_client%s", fileName), conn)
	}
}
// func (self *replica) requestAddr(filePath string, masterdestination string) string {

// 	var destination string

// 	//info, err := os.Stat(filePath)
// 	info, err := os.Stat(path.Join(PathSlave,filePath))
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
// func (self *replica) requestReplica(filePath string, masterdestination string) string {
// 	var destination string
// 	file, err0 := os.Create(path.Join(PathSlave,"nullfile"))
// 	if err0 != nil {
// 		fmt.Println(err0)

// 	}
// 	log.Println(file)
// 	file.Close()

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
// 	conn.Write([]byte("RECEIVE"))
// 	buf := make([]byte, 1024)
// 	n, err2 := conn.Read(buf)
// 	if err2 != nil {
// 		// fmt.Println("conn.Read err = ", err2)
// 		fmt.Println(err2)
// 	}
// 	fmt.Println("receive status: ", string(buf[:n]))
// 	if string(buf[:n]) == "ok" {
// 		conn.Write([]byte(info.Name()))
// 		time.Sleep(2 * time.Second)
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
func (self *replica) requestFile1(fileName string,Path string, destination string) bool {
	//var destination string

	//info, err := os.Stat(fileName)
	info, err := os.Stat(path.Join(Path,fileName))
	if err != nil {
		fmt.Println(err)
	}

	conn, err1 := net.Dial("tcp", destination)
	if err1 != nil {
		fmt.Println(err1)
	}
	defer conn.Close()

	conn.Write([]byte("RECEIVE"))
	buf := make([]byte, 1024)
	n, err2 := conn.Read(buf)
	if err2 != nil {
		// fmt.Println("conn.Read err = ", err2)
		fmt.Println(err2)
	}
	fmt.Println("receive status: ", string(buf[:n]))
	if string(buf[:n]) == "ok" {
		fmt.Printf("received ok and file requesting is %s \n", info.Name())
		conn.Write([]byte(info.Name()))
		time.Sleep(2 * time.Second)
		err = self.recv1(info.Name(), Path,conn)
		
		// n, err2 := conn.Read(buf)
		// if err2 != nil {
		// 	// fmt.Println("conn.Read err = ", err2)
		// 	fmt.Println(err2)
		// }
		// self.recv(fmt.Sprintf("received_%s", info.Name()), conn)
	}

	return false

}

func (self *replica) sendContentPage1(fileName string, Path string,destination string) bool {
	self.SendFile1(fileName, Path,destination)
	return false

}

func (self *replica) deleteFileCotentPage1(filePath string, Path string,masterdestination string) string {
	var destination string
	file, err0 := os.Create(path.Join(Path,"nullfile"))
	if err0 != nil {
		fmt.Println(err0)

	}
	log.Println(file)
	file.Close()

	info, err := os.Stat(path.Join(Path,filePath))
	if err != nil {
		fmt.Println(err)
	}

	conn, err1 := net.Dial("tcp", "localhost:8989")
	defer conn.Close()
	if err1 != nil {
		fmt.Println(err1)
	}
	conn.Write([]byte("DELETE"))
	buf := make([]byte, 1024)
	n, err2 := conn.Read(buf)
	if err2 != nil {
		// fmt.Println("conn.Read err = ", err2)
		fmt.Println(err2)
	}
	fmt.Println("receive status: ", string(buf[:n]))
	if string(buf[:n]) == "ok" {
		conn.Write([]byte(info.Name()))
		time.Sleep(2 * time.Second)
		n, err2 := conn.Read(buf)
		if err2 != nil {
			// fmt.Println("conn.Read err = ", err2)
			fmt.Println(err2)
		}
		fmt.Printf("The address of replica server is %s \n", buf[:n])
		destination = string(buf[:n])

	}
	return destination

}
func (self *replica) delete1(fileName string, Path string,destination string) bool {
	//var destination string

	info, err := os.Stat(path.Join(Path,fileName))
	if err != nil {
		fmt.Println(err)
	}

	conn, err1 := net.Dial("tcp", destination)
	if err1 != nil {
		fmt.Println(err1)
	}
	defer conn.Close()

	conn.Write([]byte("DELETE"))
	buf := make([]byte, 1024)
	n, err2 := conn.Read(buf)
	if err2 != nil {
		// fmt.Println("conn.Read err = ", err2)
		fmt.Println(err2)
	}
	fmt.Println("receive status: ", string(buf[:n]))
	if string(buf[:n]) == "ok" {
		fmt.Printf("received ok and file requesting is %s \n", info.Name())
		conn.Write([]byte(info.Name()))
		// n, err2 := conn.Read(buf)
		// if err2 != nil {
		// 	// fmt.Println("conn.Read err = ", err2)
		// 	fmt.Println(err2)
		// }
		// self.recv(fmt.Sprintf("received_%s", info.Name()), conn)
	}

	return false

}
