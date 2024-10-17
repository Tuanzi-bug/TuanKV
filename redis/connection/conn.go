package connection

import (
	"github.com/Tuanzi-bug/TuanKV/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// 用位运算标记连接状态
const (
	// flagMulti means this connection is within a transaction
	flagMulti = uint64(1 << iota)
)

// Connection represents a connection with a redis-cli
type Connection struct {
	conn net.Conn
	// 等待数据发送完成，用于正常关机
	sendingData wait.Wait
	// 标记连接状态
	flags    uint64
	password string
	//lock while server sending response
	mu sync.Mutex

	// 订阅的频道
	subs  map[string]bool
	queue [][][]byte

	watching map[string]uint32
	// 事务错误
	txErrors []error

	// 当前选择的数据库
	selectedDB int
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

// Write  sends response to client over tcp connection
func (c *Connection) Write(bytes []byte) (int, error) {
	if len(bytes) == 0 {
		return 0, nil
	}
	// 添加一个链接
	c.sendingData.Add(1)
	defer func() {
		c.sendingData.Done()
	}()
	return c.conn.Write(bytes)
}

// Close closes the connection
func (c *Connection) Close() error {
	// 超时等待数据发送完成
	c.sendingData.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	c.subs = nil
	return nil
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

// SetPassword stores password for authentication
func (c *Connection) SetPassword(s string) {
	c.password = s
}

// GetPassword get password for authentication
func (c *Connection) GetPassword() string {
	return c.password
}

// Subscribe add current connection into subscribers of the given channel 添加当前连接到给定频道的订阅者
func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

// UnSubscribe remove current connection from subscribers of the given channel 从给定频道的订阅者中删除当前连接
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

func (c *Connection) SubsCount() int {
	return len(c.subs)
}

// GetChannels returns all channels that the connection is subscribed to 返回连接订阅的所有频道
func (c *Connection) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

// InMultiState tells is connection in an uncommitted transaction  告诉连接是否处于未提交的事务中
func (c *Connection) InMultiState() bool {
	return c.flags&flagMulti > 0
}

func (c *Connection) SetMultiState(b bool) {
	if b {
		c.flags |= flagMulti
	} else {
		c.flags &= ^flagMulti
		c.queue = nil
		c.watching = nil
	}
}

// GetQueuedCmdLine returns queued commands of current transaction 返回当前事务的排队命令
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

func (c *Connection) EnqueueCmd(i [][]byte) {
	c.queue = append(c.queue, i)
}

func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}

func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

func (c *Connection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

func (c *Connection) GetTxErrors() []error {
	return c.txErrors
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(i int) {
	c.selectedDB = i
}

//func (c *Connection) SetSlave() {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (c *Connection) IsSlave() bool {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (c *Connection) SetMaster() {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (c *Connection) IsMaster() bool {
//	//TODO implement me
//	panic("implement me")
//}

func (c *Connection) Name() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}
