package service

import (
	"context"
	"encoding/json"
	"github.com/meromen/rm-receiver/db"
	"github.com/meromen/rm-receiver/util"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type RmqMessage struct {
	body db.DataRow
	ack  *amqp.Delivery
}

func (rm RmqMessage) GetBody() db.DataRow {
	return rm.body
}

func (rm RmqMessage) Ack(flag bool) {
	rm.ack.Ack(flag)
}

type RmqConfig struct {
	url       string
	queueName string
}

type RmqService struct {
	conn    *amqp.Connection
	msgChan *chan Message
	wg      sync.WaitGroup
}

func NewRmqConfig(url string, queueName string) RmqConfig {
	return RmqConfig{
		url:       url,
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

func NewRmqService(conn *amqp.Connection, msgChan *chan Message) (*RmqService, error) {
	err := util.CreateDirIfNotExists("photos")
	if err != nil {
		return nil, err
	}

	return &RmqService{
		conn:    conn,
		msgChan: msgChan,
		wg:      sync.WaitGroup{},
	}, nil
}

func InitializeRmqService(url string, queueName string) (*RmqService, error) {
	rmqConfig := NewRmqConfig(url, queueName)
	connection, err := NewRmqConnection(rmqConfig)
	if err != nil {
		return nil, err
	}
	channel, err := NewRmqChannel(connection)
	if err != nil {
		return nil, err
	}
	queue, err := NewRmqQueue(channel, rmqConfig)
	if err != nil {
		return nil, err
	}
	v, err := NewRqmMsgChan(channel, queue)
	if err != nil {
		return nil, err
	}

	msgChan := make(chan Message)
	go func() {
		for delevery := range v {
			func() {
				body := db.Photo{}
				err := json.Unmarshal(delevery.Body, &body)
				if err != nil {
					log.Printf("Faited to unmarshal msg: %s", err)
					return
				}
				msg := RmqMessage{
					body: &body,
					ack:  &delevery,
				}
				msgChan <- msg
			}()
		}
	}()

	rmqService, err := NewRmqService(connection, &msgChan)
	if err != nil {
		return nil, err
	}
	return rmqService, nil
}

func (rs *RmqService) ReceiverStart(workerCount int, storage db.Storage) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	ctxWriter, cancelWriter := context.WithCancel(context.Background())

	rs.wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go PhotoWorker(&ctxWriter, rs.msgChan, &rs.wg, storage)
	}

	<-ch

	rs.ReceiverStop(&cancelWriter)
}

func (rs *RmqService) ReceiverStop(ctxCancel *context.CancelFunc) {
	(*ctxCancel)()

	rs.wg.Wait()

	rs.conn.Close()

	log.Println("All workers stopped")
}

