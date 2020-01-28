package main

import (
	"context"
	"github.com/meromen/rm-receiver/db"
	"github.com/meromen/rm-receiver/rmq"
	"github.com/meromen/rm-receiver/util"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var WORKER_COUNT = 5



func main() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	wg := sync.WaitGroup{}
	ctxWriter, cancelWriter := context.WithCancel(context.Background())

	err := util.CreateDirIfNotExists("photos")
	if err != nil {
		panic(err)
	}

	dbConn, err := db.Connect(nil)
	if err != nil {
		panic(err)
	}

	photoStorage := db.NewPhotoStorage(dbConn)
	err = photoStorage.CreateTable()
	if err != nil {
		panic(err)
	}

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}
	defer conn.Close()

	mqch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
	}
	defer mqch.Close()

	q, err := mqch.QueueDeclare(
		"photos",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %s", err)
	}

	msgs, err := mqch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	wg.Add(WORKER_COUNT)
	for i := 0; i < WORKER_COUNT; i++ {
		go rmq.PhotoWorker(&ctxWriter, &msgs, &wg, photoStorage)
	}

	<-ch

	cancelWriter()

	wg.Wait()
	log.Println("All workers stopped")
}
