package server

import (
	"context"
	"errors"
	"github.com/Tuanzi-bug/TuanKV/redis/connection"
	database2 "github.com/Tuanzi-bug/TuanKV/redis/database"
	"github.com/Tuanzi-bug/TuanKV/redis/interface/database"
	"github.com/Tuanzi-bug/TuanKV/redis/parser"
	"github.com/Tuanzi-bug/TuanKV/redis/protocol"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"io"
	"net"
	"strings"
	"sync"
)

type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         database.DB
	closing    atomic.Boolean // refusing new client and new request
}

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()

}

func MakeHandler() *Handler {
	//var db *bitcask.DB
	//db, err := bitcask.Open(bitcask.DefaultOptions)
	//if err != nil {
	//	return nil
	//}
	db := database2.NewStandaloneServer()
	return &Handler{db: db}
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}
	// 创建一个新的连接
	client := connection.NewConn(conn)
	h.activeConn.Store(client, struct{}{}) // remember alive connection
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			// 读取到EOF或者连接关闭
			if payload.Err == io.EOF || errors.Is(payload.Err, io.ErrUnexpectedEOF) || strings.Contains(payload.Err.Error(), "use of closed network connection") {
				_ = client.Close()
				_ = h.db.Close()
				h.activeConn.Delete(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// 协议解析错误
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			// 回写失败，说明已经无法继续通信
			if err != nil {
				_ = client.Close()
				_ = h.db.Close()
				h.activeConn.Delete(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_, _ = client.Write(result.ToBytes())
		} else {
			_, _ = client.Write(unknownErrReplyBytes)
		}
	}
}

func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	h.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	_ = h.db.Close()
	return nil
}
