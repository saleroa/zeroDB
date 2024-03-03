package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"zeroDB/cmd"
	"zeroDB/global/config"
)

var dirPath string = "/home/go/src/zeroDB/global/config/config.yaml"

func main() {

	config := config.InitConfig(dirPath)

	// Listen the server.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	server, err := cmd.NewServer(config)
	if err != nil {
		log.Printf("create zerokv server err: %+v\n", err)
		return
	}
	go server.Listen(config.Addr)

	<-sig
	server.Stop()
	log.Println("zerokvdb is ready to exit, bye...")
}
