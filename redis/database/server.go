package database

import (
	"github.com/Tuanzi-bug/TuanKV/redis/config"
	"github.com/Tuanzi-bug/TuanKV/redis/interface/redis"
	"sync/atomic"
)

type Server struct {
	dbSet []*atomic.Value
}

func (s *Server) Exec(client redis.Connection, cmdLine [][]byte) redis.Reply {
	//TODO implement me
	panic("implement me")
}

func (s *Server) AfterClientClose(c redis.Connection) {
	//TODO implement me
	panic("implement me")
}

func (s *Server) Close() error {
	//TODO implement me
	panic("implement me")
}

// NewStandaloneServer creates a standalone redis server, with multi database and all other funtions
func NewStandaloneServer() *Server {
	server := &Server{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	server.dbSet = make([]*atomic.Value, config.Properties.Databases)
	return server
}
