package main

import (
	"log"

	"github.com/yeongseoksong/proglog/internal/server"
)

func main() {
	srv := server.NewHttpServer(":8088")
	log.Fatal(srv.ListenAndServe())
}
