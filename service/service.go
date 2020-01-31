package service

import (
	"context"
	"fmt"
	"github.com/meromen/rm-receiver/db"
	"github.com/meromen/rm-receiver/util"
	"log"
	"sync"
)

type Message interface {
	GetBody() db.DataRow
	Ack(bool)
}

type Service interface {
	ReceiverStart(int, db.Storage)
	ReceiverStop(*context.CancelFunc)
}

func PhotoWorker(ctx *context.Context, msgChan *chan Message, wg *sync.WaitGroup, storage db.Storage) {
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
				body := msg.GetBody()
				photo := (body).(*db.Photo)
				exist := storage.CheckExisting(photo)
				if exist {
					log.Println("Photo Exists")
					msg.Ack(false)
					return
				} else {
					err := util.DownloadFile(fmt.Sprintf("photos/%s_%s.jpg", photo.Id, photo.IdempotencyKey), photo.Url)
					if err != nil {
						log.Printf("Failed to download: %s", err)
						return
					}

					err = storage.Insert(photo)
					if err != nil {
						log.Printf("Failed to insert: %s", err)
						return
					}

					log.Printf("Photo downloaded")

					msg.Ack(false)
				}
			}()
		}
	}
}
