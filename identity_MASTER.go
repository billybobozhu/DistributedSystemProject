package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"path"
	"math/rand"
	"time"
)

type master struct {
	capacity int
}

var PathMaster ="/Users/liuzh/Downloads/DistributedSystemProject-bobozhu/master/"
func (self *master) CreateFile(fileName string, content []byte) error {
	//file, err := os.Create(fileName)
	file, err := os.Create(path.Join(PathMaster,fileName))
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
	err := os.Remove(path.Join(PathMaster,fileName))
	return err
}

func (self *master) Listen(port string) error {

	var chunknum int
	Content := make(map[string]string,100)
	addressMap := make(map[string]string, 100)
	//chunkMap := make(map[string]string, 100)
	var addr string
	f1, _ := os.Open(path.Join(PathMaster,"storage1.txt"))
	balance1, _ := ioutil.ReadAll(f1)
	f2, _ := os.Open(path.Join(PathMaster,"storage2.txt"))
	balance2, _ := ioutil.ReadAll(f2)

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
			if load1 >= load2 {
				addr = "localhost:7878"
				load1 = load1 - 1
				ioutil.WriteFile(path.Join(PathMaster,"storage1.txt"), []byte(strconv.Itoa(load1)), 0644)
				chunknum = chunknum + 1

			} else {
				addr = "localhost:7880"
				load2 = load2 - 1
				ioutil.WriteFile(path.Join(PathMaster,"storage2.txt"), []byte(strconv.Itoa(load1)), 0644)
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
			fmt.Println("thisthis???",fileName)
			ss := strings.Split(fileName, ",")
			realname := ss[1]
			fmt.Println(realname)
			var f string
			f = fmt.Sprintf("%s%s", realname, "_content.txt")
			// err3 := os.Remove(path.Join(PathMaster,f))
			// if err3 != nil {
			// 	fmt.Println(err3)
			// }
			seqN := fileName[:6]
			fileName = fileName[6:]
			fmt.Println("thisthis",fileName)
			fmt.Println("seq",seqN,realname)
			jsonFile, err := os.Open(path.Join(PathMaster,"Content.json"))
			byteValue, _ := ioutil.ReadAll(jsonFile)
			json.Unmarshal(byteValue, &Content)
			seqnum, ok := Content[realname]
			if (ok&&seqnum==seqN) || ok==false || Content[realname]=="free"{
				if (ok&&seqnum==seqN){
					chunknum = 1
					err3 := os.Remove(path.Join(PathMaster,f))
					if err3 != nil {
						fmt.Println(err3)
					}
				}
				Content[realname] = "free"
				fmt.Println(Content)
				out, _ := json.Marshal(Content)
				_ = ioutil.WriteFile(path.Join(PathMaster,"Content.json"), out, 0755)
				
				if chunknum == 1 {

					//fileObj, _ := os.OpenFile(f, os.O_CREATE, 0644)
					fileObj, _ := os.OpenFile(path.Join(PathMaster,f), os.O_CREATE, 0644)
					var d string
					d = fmt.Sprintf("%s%s", fileName, "\n")

					fileObj.Write([]byte(d))
				}
				//fileObj1, _ := os.OpenFile(f, os.O_APPEND, 0644)
				fileObj1, _ := os.OpenFile(path.Join(PathMaster,f), os.O_WRONLY|os.O_APPEND, 0644)
				defer fileObj1.Close()
				_,err2:=fileObj1.Write([]byte(fileName+"\n"))
				if err2 != nil {
					fmt.Println(err)
				}
				//fileObj1.Write([]byte("\n"))
				fmt.Printf("Requesting file name is: %s \n", fileName) // TODO: store the filename MD5 and replica server in a map
				conn.Write([]byte(addr))

				addressMap[fileName] = addr
				var s string
				s = fmt.Sprintf("%s%s", fileName, ".json")
				out, _ = json.Marshal(addressMap)
				_ = ioutil.WriteFile(path.Join(PathMaster,s), out, 0755)
				Content[realname] = "free"
				out, _ = json.Marshal(Content)
				_ = ioutil.WriteFile(path.Join(PathMaster,"Content.json"), out, 0755)
			}else{
				fmt.Println("request denied, pls request later")
			}

			
			// buf := make([]byte, 1024)
			// n, err1 := conn.Read(buf)
			// if err1 != nil {
			// 	return err1
			// }
			// if string(buf[:n]) == "DONE"{

			// }
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
			fmt.Printf("Requesting file name is: %s \n", fileName) 
			var contentPageName string
			contentPageName = fmt.Sprintf("%s%s", fileName, "_content.txt")
			fmt.Println(contentPageName)
			file, err1 := os.Open(path.Join(PathMaster,contentPageName))
			if err1 != nil {
				fmt.Println(err)
			}

			defer file.Close()

			var lines []string
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				lines = append(lines, scanner.Text())
			}
			fmt.Println(lines)
			var addressBook string

			// for i := 0; i < len(lines)-1; i++ {
			var s string
			s = fmt.Sprintf("%s%s", lines[len(lines)-1], ".json")
			data, err1 := ioutil.ReadFile(path.Join(PathMaster,s))
			if err1 != nil {
				fmt.Print(err1)
			}
			err := json.Unmarshal(data, &addressMap)
			if err != nil {
				fmt.Println("Umarshal failed:", err)
			}
			// fmt.Println("current addr mapping: ", addressMap)
			// }
			var ipaddress string
			var length int
			var finalTransmission string
			for i := 0; i < len(lines); i++ {

				if x, found := addressMap[lines[i]]; found {
					ipaddress = x
					var temp string
					temp = fmt.Sprintf("%s%s", ipaddress, ",")
					addressBook = fmt.Sprintf("%s%s", addressBook, temp)
					length = length + 1

				}
				fmt.Println("aaaaaaaaaaa:", addressBook)

				// fmt.Println(finalTransmission)

			}
			finalTransmission = fmt.Sprintf("%s%s%s", strconv.Itoa(length), ",", addressBook)
			fmt.Println("final trans message ：", finalTransmission)
			conn.Write([]byte(finalTransmission))

		} else if string(buf[:n]) == "DELETE" {
			fmt.Println("Master Method : DELETE")
			conn.Write([]byte("ok"))
			buf := make([]byte, 1024)
			n, err1 := conn.Read(buf)
			if err1 != nil {
				// fmt.Println("conn.Read err =", err1)
				return err1
			}
			fileName := string(buf[:n])
			fmt.Printf("Requesting file name is: %s \n", fileName) // TODO: store the filename MD5 and replica server in a map
			//here we need to find the correct content page for the file

			jsonFile, err := os.Open(path.Join(PathMaster,"Content.json"))
			byteValue, _ := ioutil.ReadAll(jsonFile)
			json.Unmarshal(byteValue, &Content)

			delete(Content,fileName)
			out, _ := json.Marshal(Content)
			_ = ioutil.WriteFile(path.Join(PathMaster,"Content.json"), out, 0755)
			

			var contentPageName string
			contentPageName = fmt.Sprintf("%s%s", fileName, "_content.txt")
			fmt.Println(contentPageName)
			file, err1 := os.Open(path.Join(PathMaster,contentPageName))
			if err1 != nil {
				fmt.Println(err)
			}

			defer file.Close()

			var lines []string
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				lines = append(lines, scanner.Text())
			}
			fmt.Println(lines)
			var addressBook string

			// for i := 0; i < len(lines)-1; i++ {
			var s string
			s = fmt.Sprintf("%s%s", lines[len(lines)-1], ".json")
			data, err1 := ioutil.ReadFile(path.Join(PathMaster,s))
			if err1 != nil {
				fmt.Print(err1)
			}
			for i := 0; i < len(lines); i++ {
				var s string
				s = fmt.Sprintf("%s%s", lines[i], ".json")
				os.Remove(path.Join(PathMaster,s))
			}
			file.Close()
			err = json.Unmarshal(data, &addressMap)
			if err != nil {

				fmt.Println("Umarshal failed:", err)

			}
			fmt.Println("current addr mapping: ", addressMap)
			// }
			var ipaddress string
			var length int
			var finalTransmission string
			for i := 0; i < len(lines); i++ {

				if x, found := addressMap[lines[i]]; found {
					ipaddress = x
					var temp string
					temp = fmt.Sprintf("%s%s", ipaddress, ",")
					addressBook = fmt.Sprintf("%s%s", addressBook, temp)
					length = length + 1

				}
			}

			finalTransmission = fmt.Sprintf("%s%s%s", strconv.Itoa(length), ",", addressBook)
			fmt.Println("final trans message ：", finalTransmission)
			conn.Write([]byte(finalTransmission))
			err3 := os.Remove(path.Join(PathMaster,contentPageName))
			if err3 != nil {
				fmt.Println(err3)
			} else {
				fmt.Println("The content page of the file is removed from MASTER")
			}

		}else if string(buf[:n]) == "INVALID" {
			fmt.Println("Master Method : INVALID")
			conn.Write([]byte("ok"))
			buf := make([]byte, 1024)
			n, err1 := conn.Read(buf)
			if err1 != nil {
				// fmt.Println("conn.Read err =", err1)
				return err1
			}
			fileName := string(buf[:n])
			fmt.Println("Requesting file name is: ", fileName)
			jsonFile, err := os.Open(path.Join(PathMaster,"Content.json"))
			byteValue, _ := ioutil.ReadAll(jsonFile)
			json.Unmarshal(byteValue, &Content)
			fmt.Println("file status",Content[fileName])
			if Content[fileName] == "free"{
				seqN := fmt.Sprintf("%06v",rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(1000000))
				conn.Write([]byte(seqN))
				var contentPageName string
				contentPageName = fmt.Sprintf("%s%s", fileName, "_content.txt")
				fmt.Println(contentPageName)
				file, err1 := os.Open(path.Join(PathMaster,contentPageName))
				if err1 != nil {
					fmt.Println(err)
				}
				defer file.Close()
				var lines []string
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					lines = append(lines, scanner.Text())
				}
				fmt.Println(lines)
				var addressBook string
				var s string
				s = fmt.Sprintf("%s%s", lines[len(lines)-1], ".json")
				data, err1 := ioutil.ReadFile(path.Join(PathMaster,s))
				if err1 != nil {
					fmt.Print(err1)
				}
				file.Close()
				err := json.Unmarshal(data, &addressMap)
				if err != nil {

					fmt.Println("Umarshal failed:", err)

				}
				fmt.Println("current addr mapping: ", addressMap)
				// }
				var ipaddress string
				var length int
				var finalTransmission string
				for i := 0; i < len(lines); i++ {

					if x, found := addressMap[lines[i]]; found {
						ipaddress = x
						var temp string
						temp = fmt.Sprintf("%s%s", ipaddress, ",")
						addressBook = fmt.Sprintf("%s%s", addressBook, temp)
						length = length + 1
					}
					// fmt.Println("addr:", addressBook)
				}
				finalTransmission = fmt.Sprintf("%s%s%s", strconv.Itoa(length), ",", addressBook)
				fmt.Println("final trans message ：", finalTransmission)
				conn.Write([]byte(finalTransmission))
				Content[fileName] = seqN
				out, _ := json.Marshal(Content)
				_ = ioutil.WriteFile(path.Join(PathMaster,"Content.json"), out, 0755)
				for i := 0; i < len(lines); i++ {
					var s string
					s = fmt.Sprintf("%s%s", lines[i], ".json")
					os.Remove(path.Join(PathMaster,s))
				}
				fmt.Println("The content page of the file is INVALID from MASTER")

			}else if Content[fileName] != "free"{
				fmt.Println("request denied")
				conn.Write([]byte("000000"))
			}
		}
	}
}

func (self *master) recv(fileName string, conn net.Conn) error {
	defer conn.Close()
	file, err := os.Create(path.Join(PathMaster,fileName))
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
func Delete(filepath string) error {
	err := os.Remove(path.Join(PathMaster,filepath))

	if err != nil {
		fmt.Println("file content page deleted from master")

	} else {
		return err

	}
	return err
}
