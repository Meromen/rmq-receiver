package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/meromen/rm-receiver/db"
	"github.com/streadway/amqp"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var WORKER_COUNT = 5

func createDirIfNotExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return err
		}
	}

	return nil
}

func downloadFile(filepath string, url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

func main() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	wg := sync.WaitGroup{}
	ctxWriter, cancelWriter := context.WithCancel(context.Background())

	err := createDirIfNotExists("photos")
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
		"photo",
		false,
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
		true,
		false,
		false,
		false,
		nil,
	)

	photosChan := make(chan db.Photo)

	go func() {
		for msg := range msgs {
			photo := db.Photo{}
			err := json.Unmarshal(msg.Body, &photo)
			if err != nil {
				log.Fatalf("Failed to unmarshal message: %s", err)
			}
			photosChan <- photo
		}
	}()

	wg.Add(WORKER_COUNT)
	for i := 0; i < 5; i++ {
		go func() {
			for {
				select {
				case photo := <-photosChan:
					exist := photoStorage.CheckExisting(photo.Id, photo.IdempotencyKey)
					if !exist {
						err := photoStorage.InsertPhoto(&photo)
						if err != nil {
							log.Fatalf("Failed to insert: %s", err)
						}

						err = downloadFile(fmt.Sprintf("photos/%s_%s.jpg", photo.Id, photo.IdempotencyKey), photo.Url)
						if err != nil {
							log.Fatalf("Failed to download: %s", err)
						}

						if err == nil {
							log.Println("Photo downloaded")
						}
					} else {
						log.Println("Photo Exists")
					}
				case <-ctxWriter.Done():
					{
						log.Println("Worker stopped")
						wg.Done()
						return
					}
				}
			}
		}()
	}

	<-ch

	err = conn.Close()
	if err != nil {
		log.Fatalf("Failed to close connectin: %s", err)
	}

	cancelWriter()

	wg.Wait()
	log.Println("All workers stopped")
}
