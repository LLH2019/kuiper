package main

import (
	"fmt"
)

func main() {
	//server := new(server.Server)
	//server.CreateETLTopo()
	ch := make(chan int)

	//go func() {
	//	for range time.Tick(2*time.Second){
	//		ch <- 0
	//	}
	//}()

	for {
		select {
		case <-ch:
			fmt.Println("case1")
			//default:
			//	fmt.Println("default")
		}
	}

}
