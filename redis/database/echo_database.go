package database

import (
	"github.com/Tuanzi-bug/TuanKV/redis/interface/redis"
	"github.com/Tuanzi-bug/TuanKV/redis/protocol"
)

type EchoDatabase struct {
}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}

func (e *EchoDatabase) Exec(client redis.Connection, cmdLine [][]byte) redis.Reply {
	return &protocol.PongReply{}
}

func (e *EchoDatabase) AfterClientClose(c redis.Connection) {
	//TODO implement me
	panic("implement me")
}

func (e *EchoDatabase) Close() error {
	return nil
}
