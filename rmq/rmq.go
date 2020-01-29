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

type RmqService struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
	msgChan <-chan amqp.Delivery
	wg      sync.WaitGroup
}

func NewRmqConnection(url string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return &amqp.Connection{}, err
	}

	return conn, nil
}

func NewRmqChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	mqch, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, err
	}

	return mqch, nil
}

func NewRmqQueue(mqch *amqp.Channel, name string) (amqp.Queue, error) {
	q, err := mqch.QueueDeclare(
		name,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return amqp.Queue{}, err
	}

	return q, nil
}

func NewRqmMsgChan(mqch *amqp.Channel, q amqp.Queue) (<-chan amqp.Delivery, error) {
	msgs, err := mqch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return msgs, nil
}

func NewRmqService(url string, queueName string) (*RmqService, error) {
	err := util.CreateDirIfNotExists("photos")
	if err != nil {
		return nil, err
	}

	conn, err := NewRmqConnection(url)
	if err != nil {
		return nil, err
	}

	mqch, err := NewRmqChannel(conn)
	if err != nil {
		return nil, err
	}

	q, err := NewRmqQueue(mqch, queueName)
	if err != nil {
		return nil, err
	}

	msgs, err := NewRqmMsgChan(mqch, q)
	if err != nil {
		return nil, err
	}

	return &RmqService{
		conn:    conn,
		channel: mqch,
		queue:   q,
		msgChan: msgs,
		wg:      sync.WaitGroup{},
	}, nil
}

func (rs *RmqService) ReceiverStart(workerCount int, ctx *context.Context, storage db.PhotosStorage) {
	rs.wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go PhotoWorker(ctx, &rs.msgChan, &rs.wg, storage)
	}
}

func (rs *RmqService) ReceiverStop(ctxCancel *context.CancelFunc) {

	(*ctxCancel)()

	rs.wg.Wait()

	log.Println("All workers stopped")
}

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
						return
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

						log.Printf("Photo downloaded")

						msg.Ack(false)
					}
				}
			}()
		}
	}
}
