package rmq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/meromen/rm-receiver/db"
	"github.com/meromen/rm-receiver/util"
	"github.com/streadway/amqp"
	"log"
	"sync"
)

func PhotoWorker(ctx *context.Context, msgChan *<-chan amqp.Delivery, wg *sync.WaitGroup, photoStorage db.PhotosStorage) {
	for {
		select {
		case <-(*ctx).Done():
			{
				log.Println("Worker stopped")
				wg.Done()
				return
			}
		case msg := <-*msgChan:
			func() {
				if msg.Body == nil {
					log.Println("Body is empty")
					msg.Ack(false)
					return
				} else {
					photo := db.Photo{}
					err := json.Unmarshal(msg.Body, &photo)
					if err != nil {
						log.Printf("Failed to unmarshal message: %s", err)
						return
					}
					photo.FilePath = fmt.Sprintf("photos/%s_%s.jpg", photo.Id, photo.IdempotencyKey)

					exist := photoStorage.CheckExisting(photo.Id, photo.IdempotencyKey)
					if exist {
						log.Println("Photo Exists")
						msg.Ack(false)
					} else {
						err = util.DownloadFile(fmt.Sprintf("photos/%s_%s.jpg", photo.Id, photo.IdempotencyKey), photo.Url)
						if err != nil {
							log.Printf("Failed to download: %s", err)
							return
						}

						err := photoStorage.InsertPhoto(&photo)
						if err != nil {
							log.Printf("Failed to insert: %s", err)
							return
						}

						if err == nil {
							log.Printf("Photo downloaded")
						}

						msg.Ack(false)
					}
				}
			}()
		}
	}
}
