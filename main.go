package main

import (
	"context"
	"github.com/meromen/rm-receiver/db"
	"log"
)

var WORKER_COUNT = 5
func main() {
	ctxDone, serviceStop := context.WithCancel(context.Background())

	dbConn, err := db.Connect(nil)
	if err != nil {
		panic(err)
	}

	defer dbConn.Close()

	photoStorage, err := db.NewPhotoStorage(dbConn)
	if err != nil {
		log.Fatalf("Failed to create photo storage: %s", err)
	}

	rmqService, err := InitializeRmqService( "photos")
	if err != nil {
		log.Fatalf("Failed to start rmq service: %s", err)
	}

	rmqService.ReceiverStart(WORKER_COUNT, &serviceStop, &photoStorage)

	<-ctxDone.Done()
}
