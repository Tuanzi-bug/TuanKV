package protocol

import (
	"bytes"
	"github.com/Tuanzi-bug/TuanKV/redis/interface/redis"
	"strconv"
)

var (
	// CRLF is the line separator of redis serialization protocol
	CRLF = "\r\n"
)

type BulkReply struct {
	Arg []byte
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{Arg: arg}
}

// ToBytes marshal redis.Reply 例子：
// 简单字符串：以"+" 开始， 如："+OK\r\n"
// 错误：以"-" 开始，如："-ERR Invalid Synatx\r\n"
// 整数：以":"开始，如：":1\r\n"
func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return nullBulkBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

type MultiBulkReply struct {
	Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{Args: args}
}

func (r *MultiBulkReply) ToBytes() []byte {
	var buf bytes.Buffer

	argLen := len(r.Args)
	bufLen := 1 + len(strconv.Itoa(argLen)) + 2 // 类型+长度+crlf
	for _, arg := range r.Args {
		if arg == nil {
			bufLen += 3 + 2 // $-1 + crlf
		} else {
			bufLen += 1 + len(strconv.Itoa(len(arg))) + 2 + len(arg) + 2 // $+正文长度+crlf+实际内容+crlf。
		}
	}

	buf.Grow(bufLen)
	buf.WriteString("*")
	buf.WriteString(strconv.Itoa(argLen))
	buf.WriteString(CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1")
			buf.WriteString(CRLF)
		} else {
			buf.WriteString("$")
			buf.WriteString(strconv.Itoa(len(arg)))
			buf.WriteString(CRLF)
			buf.Write(arg)
			buf.WriteString(CRLF)
		}
	}
	return buf.Bytes()
}

// StatusReply 回复状态：以"+" 开始， 如："+OK\r\n"
type StatusReply struct {
	Status string
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{Status: status}
}

func (r StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

// IntReply 整数：以":"开始，如：":1\r\n"
type IntReply struct {
	Code int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{Code: code}
}

func (r IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

type ErrorReply interface {
	Error() string
	ToBytes() string
}

// StandardErrReply 标准的错误：以"-" 开始，如："-ERR Invalid Synatx\r\n"
type StandardErrReply struct {
	Status string
}

func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

// IsErrorReply returns true if the given protocol is error
func IsErrorReply(reply redis.Reply) bool {
	return reply.ToBytes()[0] == '-'
}
