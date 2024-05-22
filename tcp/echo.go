package tcp

import (
	"bufio"
	"context"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"github.com/hdt3213/godis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}

type EchoClient struct {
	// tcp 连接
	Conn net.Conn
	// 当服务端开始发送数据时进入waiting, 阻止其它goroutine关闭连接
	Waiting wait.Wait
}

// MakeEchoHandler creates EchoHandler
func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

func (c *EchoClient) Close() error {
	// 等待服务端发送数据完成
	c.Waiting.WaitWithTimeout(10 * time.Second)
	_ = c.Conn.Close()
	return nil
}

func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	// 处于关闭状态 handler 不处理新的连接
	if h.closing.Get() {
		_ = conn.Close()
		return
	}
	client := &EchoClient{Conn: conn}
	h.activeConn.Store(client, struct{}{}) // 记住仍然存活的连接

	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("client closed")
				h.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		// 发送数据前先置为waiting状态，阻止连接被关闭
		client.Waiting.Add(1)

		b := []byte(msg)
		_, _ = conn.Write(b)
		// 发送完毕, 结束waiting
		client.Waiting.Done()
	}

}
func (h *EchoHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	// 逐个关闭连接
	h.activeConn.Range(func(key, value any) bool {
		client := key.(*EchoClient)
		_ = client.Close()
		return true
	})
	return nil
}
