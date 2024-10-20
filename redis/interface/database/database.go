package database

import (
	"github.com/Tuanzi-bug/TuanKV/redis/interface/redis"
)

// CmdLine is alias for [][]byte, represents a database line  CmdLine是[][]byte的别名，表示一行命令
type CmdLine = [][]byte

// DB is the interface for redis style storage engine  DB是redis风格存储引擎的接口
type DB interface {
	Exec(client redis.Connection, cmdLine [][]byte) redis.Reply
	AfterClientClose(c redis.Connection)
	Close() error
	//LoadRDB(dec *core.Decoder) error
}

// DataEntity stores data bound to a key, including a string, list, hash, set and so on 存储与键绑定的数据，包括字符串、列表、哈希、集合等
type DataEntity struct {
	Data interface{}
}
