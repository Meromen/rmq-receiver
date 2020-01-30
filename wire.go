//+build wireinject

package main

import (
	"github.com/google/wire"
	"github.com/meromen/rm-receiver/rmq"
)

func InitializeRmqService(queueName string) (*rmq.RmqService, error) {
	wire.Build(rmq.NewRmqService, rmq.NewRqmMsgChan, rmq.NewRmqQueue, rmq.NewRmqChannel, rmq.NewRmqConnection, rmq.NewRmqConfig)
	return &rmq.RmqService{}, nil
}
