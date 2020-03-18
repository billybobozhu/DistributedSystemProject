package main

import "fmt"

// type identityType int

// const (
// 	//MASTER identifies this node is a master
// 	MASTER identityType = 0

// 	//SLAVE identifies this node is a slave
// 	SLAVE identityType = 1
// )

// type master struct {
// }

// type slave struct {
// 	capacity int
// }

// func (self *slave) CreateFile(fileName string, content []byte) error {
// 	file, err := os.Create(fileName)
// 	if err != nil {
// 		// fmt.Println(err)
// 		return err
// 	}
// 	// err := ioutil.WriteFile(fileName, content, 0777)
// 	// if err != nil{
// 	// 	fmt.Println(err)
// 	// }
// 	n, err := file.Write(content)
// 	if err == nil && n < len(content) {
// 		err = io.ErrShortWrite
// 	}
// 	if closeErr := file.Close(); err == nil {
// 		err = closeErr
// 	}
// 	return err
// }

// func (self *slave) DeleteFile(fileName string) error {
// 	err := os.Remove(fileName)
// 	return err
// }

// type node struct {
// 	identity identityType

// 	master
// 	slave
// }

// //NewNode to new a node
// func NewNode(capacity int) *node {
// 	suchNode := new(node)
// 	// New node should raise election here upon it is created
// 	return suchNode
// }

// func (self *node) Run() {
// 	// OwO the node is too lazy to do anything for now
// 	switch self.identity {
// 	case MASTER:
// 		fmt.Println("This is a MASTER node")
// 	case SLAVE:
// 		fmt.Println("This is a SLAVE node")
// 	}
// }

func main() {
	client := NewNode(233)
	go client.Run()
	var content = []byte("OwO Hello I am some random file")
	client.CreateFile("hello.txt", content)
	// client.DeleteFile("hello.txt")

	// var pause string
	fmt.Scanln()
}
