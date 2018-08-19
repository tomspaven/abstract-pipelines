package main

import (
	dapper "abstract-pipelines/pkg"
	"fmt"
)

func main() {
	dapper := dapper.NewDapper()
	fmt.Printf("I am a go-dapper %v", dapper)
}
