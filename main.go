package main

import (
	"github.com/meromen/rm-receiver/db"
	"github.com/meromen/rm-receiver/service"
	"log"
)

var WORKER_COUNT = 5

func main() {
	dbConn, err := db.Connect(nil)
	if err != nil {
		panic(err)
	}

	defer dbConn.Close()

	photoStorage, err := db.NewPhotoStorage(dbConn)
	if err != nil {
		log.Fatalf("Failed to create photo storage: %s", err)
	}

	rmqService, err := service.InitializeRmqService("amqp://guest:guest@localhost:5672/", "photos")
	if err != nil {
		log.Fatalf("Failed to start service service: %s", err)
	}

	rmqService.ReceiverStart(WORKER_COUNT, &photoStorage)
}
