package main

import (
	"fmt"
	"github.com/Tuanzi-bug/TuanKV/redis/config"
	"github.com/Tuanzi-bug/TuanKV/redis/server"
	"github.com/Tuanzi-bug/TuanKV/tcp"
	"github.com/hdt3213/godis/lib/logger"
	"os"
)

var defaultProperties = &config.ServerProperties{
	Bind: "0.0.0.0",
	Port: 6399,
	//AppendOnly:     false,
	//AppendFilename: "",
	//MaxClients:     1000,
	//RunID:          utils.RandString(40),
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func main() {
	// 从环境变量中获取配置文件路径
	configFilename := os.Getenv("CONFIG")
	if configFilename == "" {
		if fileExists("config") {
			config.SetupConfig("redis.conf")
		} else {
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(configFilename)
	}
	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, server.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}
