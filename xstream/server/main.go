package main

import (
	"fmt"
	"github.com/emqx/kuiper/xstream/server/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

var (
	Version      = "unknown"
	LoadFileType = "relative"
)

//type User struct {
//	name string
//	age int
//}

func main() {
		server.StartUp(Version, LoadFileType)
		fmt.Println("http Listen.......")
		fmt.Println("http Listen.......")
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":10108", nil)

	//user := &User{
	//	name: "str",
	//	age : 12,
	//}
	//
	//fmt.Println("type", user , &user, *user, &(user.name),  &(user.age))
	//user.getId()
	//getOuter(user)
}

//func (user *User) getId()  {
//	fmt.Println("getId", user, &user, *user, &(user.name),  &(user.age))
//
//}
//
//func getOuter(user *User)  {
//	fmt.Println("getOuter", user, &user, *user,&(user.name),  &(user.age))
//}