package main

import (
	"io"
	"os"
)

type slave struct {
	capacity int
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
