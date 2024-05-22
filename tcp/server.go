package tcp

import (
	"context"
	"errors"
	"fmt"
	"github.com/hdt3213/godis/interface/tcp"
	"github.com/hdt3213/godis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Config stores tcp server properties
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// ClientCounter Record the number of clients in the current redis server
var ClientCounter int32

func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	// 监听系统指定信号 SIGHUP：终端挂起或者控制进程终止，SIGQUIT：终端退出，SIGTERM：终止信号，SIGINT：中断信号
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{} // 发送关闭信号
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	// 开启监听
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe binds port and handle requests, blocking until close
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// 监听错误信号
	errCh := make(chan error, 1)
	defer close(errCh)
	go func() {
		select {
		case <-closeChan:
			logger.Info("get exit signal")
		case er := <-errCh:
			logger.Info(fmt.Sprintf("accept error: %s", er.Error()))
		}
		logger.Info("shutting down...")
		_ = listener.Close()
		_ = handler.Close() //close connections
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		//  Accept 会一直阻塞直到有新的连接建立或者listen中断才会返回
		conn, err := listener.Accept()
		// 判断当前错误是否是一个临时错误，如果是则等待5ms后重试
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				logger.Info(fmt.Sprintf("accept occurs temporary error: %v, retry in 5ms", err))
				time.Sleep(5 * time.Millisecond)
				continue
			}
			errCh <- err
			break
		}
		logger.Info("accept link")
		ClientCounter++
		// 计数器+1
		waitDone.Add(1)
		// 开启新的 goroutine 处理该连接
		go func() {
			defer func() {
				waitDone.Done()
				atomic.AddInt32(&ClientCounter, -1)
			}()
			handler.Handle(ctx, conn)
		}()
	}
	waitDone.Wait()
}
