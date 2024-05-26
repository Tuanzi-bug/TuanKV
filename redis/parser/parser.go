package parser

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/Tuanzi-bug/TuanKV/interface/redis"
	"github.com/Tuanzi-bug/TuanKV/redis/protocol"
	"github.com/hdt3213/godis/lib/logger"

	"io"
	"runtime/debug"
	"strconv"
)

// Payload stores redis.Reply or error
type Payload struct {
	Data redis.Reply
	Err  error
}

// ParseStream reads data from io.Reader and send payloads through channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// ParseBytes reads data from []byte and return all replies
func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	var results []redis.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

// ParseOne reads data from []byte and return the first payload
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err
}

func parse0(rawReader io.Reader, ch chan<- *Payload) {
	// 保证程序不会因为 panic 而退出
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()
	reader := bufio.NewReader(rawReader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{
				Err: err,
			}
			close(ch)
			return
		}
		length := len(line)
		// 忽略空行
		if length <= 2 || line[length-2] != '\r' {
			continue
		}
		// 处理尾部的 \r\n
		// RESP 以行作为单位，客户端和服务器发送的命令或数据一律以 \r\n （CRLF）作为换行符。
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		/*
			RESP 通过第一个字符来表示格式.
				简单字符串：以"+" 开始， 如："+OK\r\n"
				错误：以"-" 开始，如："-ERR Invalid Synatx\r\n"
				整数：以":"开始，如：":1\r\n"
				字符串：以 $ 开始
				数组：以 * 开始
		*/
		switch line[0] {
		case '+':
			content := string(line[1:])
			ch <- &Payload{
				Data: protocol.MakeStatusReply(content),
			}
		case '-':
			ch <- &Payload{Data: protocol.MakeErrReply(string(line[1:]))}
		case ':':
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				protocolError(ch, "illegal number "+string(line[1:]))
				continue
			}
			ch <- &Payload{Data: protocol.MakeIntReply(value)}
		case '$':
			err = parseBulkString(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		case '*':
			err = parseArray(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
			//默认情况下，返回多组
		default:
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{Data: protocol.MakeMultiBulkReply(args)}
		}

	}
}

// 解析 bulk string 格式，例子： $3\r\nSET\r\n
func parseBulkString(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 第一行为 $+正文长度，第二行为实际内容。
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	// 处理非法情况：长度小于0，或者解析失败
	if err != nil || strLen < -1 {
		protocolError(ch, "illegal bulk string header: "+string(header))
		return nil
	} else if strLen == -1 { // $-1 表示 nil 查询不存在key，返回 $-1
		ch <- &Payload{Data: protocol.MakeNullBulkReply()}
		return nil
	}
	// 读取实际内容 +2 是因为 末尾\r\n
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{Data: protocol.MakeBulkReply(body[:len(body)-2])}
	return nil

}

// 解析数组形式 例子：*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// Array 格式第一行为 "*"+数组长度，其后是相应数量的 Bulk String
	arrayLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || arrayLen < 0 {
		protocolError(ch, "illegal array header: "+string(header))
		return nil
	} else if arrayLen == 0 {
		ch <- &Payload{Data: protocol.MakeNullBulkReply()}
		return nil
	}
	lines := make([][]byte, 0, arrayLen)
	for i := int64(0); i < arrayLen; i++ {
		var line []byte
		// 读取每一个 Bulk String
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		length := len(line)
		// 处理不满足要求的情况：长度小于4，不是以\r\n结尾，不是以$开头
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(ch, "illegal bulk string header: "+string(line))
			break
		}
		// 解析 Bulk String
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			protocolError(ch, "illegal bulk string length: "+string(line))
			break
		} else if strLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err = io.ReadFull(reader, body)
			if err != nil {
				return err
			}
			lines = append(lines, body[:len(body)-2])
		}
		ch <- &Payload{Data: protocol.MakeMultiBulkReply(lines)}
	}
	return nil
}

// 自定义错误信息
func protocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{Err: err}
}
