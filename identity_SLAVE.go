package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
	"path"
	"io/ioutil"
	"bufio"
	"strconv"
	"strings"
)

type slave struct {
	capacity int
}
var seqC map[string]string

var PathSlave = "/Users/liuzh/Desktop/Git/DistributedSystemProject-bobozhu/slave/"
func (self *slave) CreateFile(fileName string, content []byte) error {
	file, err := os.Create(path.Join(PathSlave,fileName))
	if err != nil {
		return err
	}
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
	//err := os.Remove(fileName)
	err := os.Remove(path.Join(PathSlave,fileName))
	return err
}

func (self *slave) SendFile(filePath string, destination string) error {
	info, err := os.Stat(path.Join(PathSlave,filePath))
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
		err = self.send(filePath, conn)
	}
	return err
}

func (self *slave) CutFile(name string) {
	f, err := os.Open(path.Join(PathSlave,name))
	defer f.Close()
	if err != nil {
		fmt.Println("os.Open err = ", err)
	}
	var libname string
	var subfile string
	libname = fmt.Sprintf("%s%s", "list_", name)
	fileObj, _ := os.OpenFile(path.Join(PathSlave,libname), os.O_CREATE, 0644)
	fileObj.Close()
	bytes, _ := ioutil.ReadAll(f)

	result := self.split(bytes, 50000)
	for i := 0; i < len(result); i++ {
		d1 := []byte(result[i])
		subfile = fmt.Sprintf("%s%s%s", strconv.Itoa(i+1), ",", name)
		err := ioutil.WriteFile(path.Join(PathSlave,subfile), d1, 0644)
		if err != nil {
			fmt.Println(err)
		}
		//fileObj1, err := os.OpenFile(libname, os.O_APPEND, 0644)
		fileObj1, _:= os.OpenFile(path.Join(PathSlave,libname), os.O_WRONLY|os.O_APPEND, 0644)
		defer fileObj1.Close()
			//fmt.Println(libname)
			//err1 := ioutil.WriteFile(path.Join(filePath,libname), []byte(subfile+"\n"), 0644)
		_,err1:=fileObj1.Write([]byte(subfile+"\n"))
		//fmt.Println(subfile)
		if err1 != nil {
			fmt.Println(err)
		}
		// fileObj1.Write([]byte(subfile))
		// fileObj1.Write([]byte("\n"))
	}
	var s string
	s = fmt.Sprintf("%s%s", "list_", name)
	// slave.sendContentPage(s, "localhost:8989")
	//file, err := os.Open(s)
	fmt.Println(s)
	file1, err := os.Open(path.Join(PathSlave,s))
	if err != nil {
		fmt.Println(err)
	}
	defer file1.Close()

	var lines []string
	linecount := 0
	scanner := bufio.NewScanner(file1)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		linecount++
	}
	fmt.Println(linecount)

	for i := 0; i < len(lines); i++ {
		var destination string
		//destination = slave.requestAddr(lines[i], "localhost:8989")
		destination = self.requestAddr(lines[i], "localhost:8989")
		//slave.SendFile(lines[i], destination)
		fmt.Println("test",lines[i],destination)
		self.SendFile(lines[i],destination)
		err = os.Remove(path.Join(PathSlave,lines[i]))
	}
	time.Sleep(2000)
	err = os.Remove(path.Join(PathSlave,s))
}
func (self *slave) send(filePath string, conn net.Conn) error {
	defer conn.Close()
	file, err := os.Open(path.Join(PathSlave,filePath))
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
	err=os.Remove(path.Join(PathSlave,filePath))
	return err
}

func (self *slave) Listen(port string) error {
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
		buf := make([]byte, 1024)
		n, err1 := conn.Read(buf)
		if err1 != nil {
			// fmt.Println("conn.Read err =", err1)
			return err1
		}
		// 拿到了文件的名字
		fileName := string(buf[:n])
		// 返回ok
		conn.Write([]byte("ok"))
		// 接收文件,
		err = self.recv(path.Join("received_", fileName), conn)
		
	}
}

func (self *slave) recv(fileName string, conn net.Conn) error {
	defer conn.Close()
	file, err := os.Create(path.Join(PathSlave,fileName))
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

func (self *slave) requestAddr(filePath string, masterdestination string) string {
	var destination string

	//info, err := os.Stat(filePath)
	info, err := os.Stat(path.Join(PathSlave,filePath))
	if err != nil {
		fmt.Println(err)
	}

	conn, err1 := net.Dial("tcp", "localhost:8989")
	defer conn.Close()
	if err1 != nil {
		fmt.Println(err1)
	}
	conn.Write([]byte("SEND"))

	buf := make([]byte, 1024)
	n, err2 := conn.Read(buf)
	if err2 != nil {
		fmt.Println(err2)
	}
	fmt.Println("receive status: ", string(buf[:n]))
	if string(buf[:n]) == "ok" {
		realName:=strings.Split(info.Name(),",")
		//fmt.Println("?????????????",info.Name(),seqC)
		seqnum, ok := seqC[realName[1]]
	    if (ok) {
	        conn.Write([]byte(seqnum+info.Name()))
	    } else {
	        conn.Write([]byte("999999"+info.Name()))
	    }
		//conn.Write([]byte(info.Name()))
		//Check validation
		time.Sleep(2 * time.Second)
		buf := make([]byte, 1024)
		n, err2 := conn.Read(buf)
		if err2 != nil {
			fmt.Println(err2)
		}
		fmt.Println(string(buf[:n]))
		destination = string(buf[:n])
		// if !string(buf[:n]) || string(buf[:n])[:6] != "000000"{
		// 	if string(buf[:n])!=""{
		// 		fmt.Println(string(buf[:n])[:6])
		// 		seq[filePath]=string(buf[:n])[:6]
		// 		destination = string(buf[:n])[6:]
		// 	}else{
				
		// 	}
		// 	// buf := make([]byte, 1024)
		// 	// n, err2 = conn.Read(buf)
		// 	// if err2 != nil {
		// 	// 	fmt.Println(err2)
		// 	// }
			
		// 	fmt.Printf("The address of replica server is %s \n", destination)
		// }else if string(buf[:n]) == "000000"{
		// 	fmt.Printf("The request is denied from master, try again later")
		// }

	}
	return destination

}
func (self *slave) request(name string){
	var destination string
	destination = self.requestReplica(name, "localhost:8989")
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
		//subfiles = append(subfiles, subfilename)
		subfiles = append(subfiles,subfilename)
		//fmt.Println(path.Join(filePath,subfilename))
		self.requestFile(subfilename, dest[i+1])
	}
	file, _ := os.Create(path.Join(PathSlave,name))

	for i:= 0;i<limit;i++{
		subname:=subfiles[i]
		file, _= os.OpenFile(path.Join(PathSlave,name), os.O_WRONLY|os.O_APPEND, 0644)
		defer file.Close()
		f, _ := os.Open(path.Join(PathSlave,subname))
		bytes, _ := ioutil.ReadAll(f)
		_,err1:=file.Write(bytes)
		if err1!=nil{
			fmt.Println(err1)
		}
	}
}

func (self *slave) requestReplica(filePath string, masterdestination string) string {
	var destination string
	file, err0 := os.Create(path.Join(PathSlave,"nullfile"))
	if err0 != nil {
		fmt.Println(err0)

	}
	log.Println(file)
	file.Close()

	conn, err1 := net.Dial("tcp", masterdestination)
	defer conn.Close()
	if err1 != nil {
		fmt.Println(err1)
	}
	conn.Write([]byte("RECEIVE"))
	buf := make([]byte, 1024)
	n, err2 := conn.Read(buf)
	if err2 != nil {
		// fmt.Println("conn.Read err = ", err2)
		fmt.Println(err2)
	}
	fmt.Println("receive status: ", string(buf[:n]))
	if string(buf[:n]) == "ok" {
		//conn.Write([]byte(info.Name()))
		conn.Write([]byte(filePath))
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
func (self *slave) requestFile(fileName string, destination string) bool {
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
		fmt.Printf("received ok and file requesting is %s \n", fileName)
		//conn.Write([]byte(info.Name()))
		conn.Write([]byte(fileName))
		time.Sleep(2 * time.Second)
		err := self.recv(fileName, conn)
		if err != nil {
			fmt.Println(err)
		}
	}
	return false

}

func (self *slave) sendContentPage(fileName string, destination string) bool {
	self.SendFile(fileName, destination)
	return false

}

func (self *slave) deleteFileCotentPage(filePath string, masterdestination string) string {
	var destination string
	file, err0 := os.Create(path.Join(PathSlave,"nullfile"))
	if err0 != nil {
		fmt.Println(err0)
	}
	log.Println(file)
	file.Close()
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
		//conn.Write([]byte(info.Name()))
		conn.Write([]byte(filePath))
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
func (self *slave) delete(fileName string, destination string) bool {
	fmt.Println("here delete")
	conn, err1 := net.Dial("tcp", destination)
	if err1 != nil {
		fmt.Println(err1)
	}
	defer conn.Close()

	conn.Write([]byte("DELETE"))
	buf := make([]byte, 1024)
	n, err2 := conn.Read(buf)
	if err2 != nil {
		fmt.Println(err2)
	}
	fmt.Println("DELETE receive status: ", string(buf[:n]))
	if string(buf[:n]) == "ok" {
		conn.Write([]byte(fileName))
	}

	return false

}

func (self *slave) Invalid_request(filePath string, masterdestination string) string {
	var destination string
	file, err0 := os.Create(path.Join(PathSlave,"nullfile"))
	if err0 != nil {
		fmt.Println(err0)
	}
	log.Println(file)
	file.Close()
	conn, err1 := net.Dial("tcp", "localhost:8989")
	defer conn.Close()
	if err1 != nil {
		fmt.Println(err1)
	}
	conn.Write([]byte("INVALID"))
	buf := make([]byte, 1024)
	n, err2 := conn.Read(buf)
	if err2 != nil {
		fmt.Println(err2)
	}
	fmt.Println("receive status: ", string(buf[:n]))
	if string(buf[:n]) == "ok" {
		//conn.Write([]byte(info.Name()))
		conn.Write([]byte(filePath))
		time.Sleep(2 * time.Second)
		buf := make([]byte, 1024)
		n, err2 := conn.Read(buf)
		if err2 != nil {
			fmt.Println(err2)
		}
		msg:=strings.Split(string(buf[:n]), " ")
		fmt.Println("msgsss",msg)
		if msg[0] != string(000000){
			seqC[filePath]=msg[0]
			fmt.Println("sequences numbbber ",msg[0])
			// buf := make([]byte, 1024)
			// n, err2 = conn.Read(buf)
			// if err2 != nil {
			// 	fmt.Println(err2)
			// }
			destination = msg[1]
			fmt.Printf("The address of replica server is %s \n", destination)
		}else if string(buf[:n]) == string(000000){
			fmt.Printf("The request is denied from master, try again later")
		}
		
	}
	return destination

}


func (self *slave)modify(name string){
	var destination string
	destination = self.Invalid_request(name, "localhost:8989")
	if destination != ""{
		self.request(name)
		dest := strings.Split(destination, ",")
		var length string
		length = dest[0]
		fmt.Println(length)
		limit, _ := strconv.Atoi(length)
		fmt.Println("llllmit",limit)

		var subfiles []string
		for i := 0; i < limit; i++ {
			var subfilename string
			subfilename = fmt.Sprintf("%s%s%s", strconv.Itoa(i+1), ",", name)
			subfiles = append(subfiles, subfilename)
			fmt.Println("here here",subfilename, dest[i],dest[i+1])
			self.delete(subfilename, dest[i+1])
		}
		time.Sleep(1000)
		fmt.Printf("Please type 'SUBMIT' after finish editing\n")
		input := bufio.NewScanner(os.Stdin)
		input.Scan()
		if input.Text()=="SUBMIT"{
			self.CutFile(name)
		}
	}
	

}
func (self *slave)append(name string){
	var destination string
	destination = self.Invalid_request(name, "localhost:8989")
	if destination != ""{
		self.request(name)
		dest := strings.Split(destination, ",")
		var length string
		length = dest[0]
		fmt.Println(length)
		limit, _ := strconv.Atoi(length)
		var subfiles []string
		for i := 0; i < limit; i++ {
			var subfilename string
			subfilename = fmt.Sprintf("%s%s%s", strconv.Itoa(i+1), ",", name)
			fmt.Println("SubfileName:",subfilename)
			subfiles = append(subfiles, subfilename)
			self.delete(subfilename, dest[i+1])
		}
		fmt.Printf("Please type in file name you want append\n")
		var addName string
		fmt.Scanln(&addName)
		file, _:= os.OpenFile(path.Join(PathSlave,name), os.O_WRONLY|os.O_APPEND, 0644)
		defer file.Close()
		f, _ := os.Open(path.Join(PathSlave,addName))
		bytes, _ := ioutil.ReadAll(f)
		_,err1:=file.Write(bytes)
		if err1!=nil{
			fmt.Println(err1)
		}
		self.CutFile(name)
	}
}


func (self *slave)split(buf []byte, lim int) [][]byte {
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

func (self *slave) SlaveMain(){
	seqC = make(map[string]string)
	go self.Listen("localhost:7879")
}
