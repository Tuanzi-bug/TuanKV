package database

import (
	"github.com/Tuanzi-bug/TuanKV/redis/interface/redis"
	"github.com/Tuanzi-bug/TuanKV/redis/protocol"
)

func Ping(c redis.Connection, args [][]byte) redis.Reply {
	return &protocol.PongReply{}
}
