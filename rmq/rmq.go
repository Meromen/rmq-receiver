package rmq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/meromen/rm-receiver/db"
	"github.com/meromen/rm-receiver/util"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type RmqConfig struct {
	url       string
	queueName string
}

type RmqService struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
	msgChan <-chan amqp.Delivery
	wg      sync.WaitGroup
}

func NewRmqConfig(queueName string) RmqConfig {
	return RmqConfig{
		url:       "amqp://guest:guest@localhost:5672/",
		queueName: queueName,
	}
}

func NewRmqConnection(config RmqConfig) (*amqp.Connection, error) {
	conn, err := amqp.Dial(config.url)
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

func NewRmqQueue(mqch *amqp.Channel, config RmqConfig) (amqp.Queue, error) {
	q, err := mqch.QueueDeclare(
		config.queueName,
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

func NewRmqService(conn *amqp.Connection, mqch *amqp.Channel, q amqp.Queue, msgs <-chan amqp.Delivery) (*RmqService, error) {
	err := util.CreateDirIfNotExists("photos")
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

func (rs *RmqService) ReceiverStart(workerCount int, serviceStop *context.CancelFunc, storage db.Storage) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	ctxWriter, cancelWriter := context.WithCancel(context.Background())

	go func() {
		<-ch

		rs.ReceiverStop(&cancelWriter, serviceStop)
	}()

	rs.wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go PhotoWorker(&ctxWriter, &rs.msgChan, &rs.wg, storage)
	}
}

func (rs *RmqService) ReceiverStop(ctxCancel *context.CancelFunc, serviceStop *context.CancelFunc) {

	(*ctxCancel)()

	rs.wg.Wait()

	rs.conn.Close()

	(*serviceStop)()
	log.Println("All workers stopped")
}

func PhotoWorker(ctx *context.Context, msgChan *<-chan amqp.Delivery, wg *sync.WaitGroup, storage db.Storage) {
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

					exist := storage.CheckExisting(photo)
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

						err := storage.Insert(photo)
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
