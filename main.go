package main

import (
	"context"
	"github.com/meromen/rm-receiver/db"
	"github.com/meromen/rm-receiver/rmq"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var WORKER_COUNT = 5
func main() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	ctxWriter, cancelWriter := context.WithCancel(context.Background())

	dbConn, err := db.Connect(nil)
	if err != nil {
		panic(err)
	}

	photoStorage, err := db.NewPhotoStorage(dbConn)
	if err != nil {
		log.Fatalf("Failed to create photo storage: %s", err)
	}

	rmqService, err := rmq.NewRmqService("amqp://guest:guest@localhost:5672/", "photos")
	if err != nil {
		log.Fatalf("Failed to start rmq service: %s", err)
	}

	rmqService.ReceiverStart(WORKER_COUNT, &ctxWriter, photoStorage)

	<-ch

	rmqService.ReceiverStop(&cancelWriter)
}
