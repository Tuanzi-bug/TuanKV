package main

import (
	"fmt"
	bitcask "github.com/Tuanzi-bug/TuanKV"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = "../../tmp/bit2"
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("123"), []byte("123"))
	if err != nil {
		fmt.Println(err)
	}
	v, err := db.Get([]byte("123"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(v))
	err = db.Delete([]byte("123"))
	if err != nil {
		panic(err)
	}
}
