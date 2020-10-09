package xstream

import "github.com/emqx/kuiper/xstream/server/server"

func main() {
	server := new(server.Server)
	server.CreateETLTopo()
}
