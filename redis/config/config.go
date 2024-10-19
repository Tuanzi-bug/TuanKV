package config

import (
	"bufio"
	"github.com/hdt3213/godis/lib/logger"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

// ServerProperties defines global config properties
type ServerProperties struct {
	Bind      string `cfg:"bind"`
	Port      int    `cfg:"port"`
	Databases int    `cfg:"databases"`
	Dir       string `cfg:"dir,omitempty"`
	// config file path
	CfPath string `cfg:"cf,omitempty"`
}

// Properties holds global config properties
var Properties *ServerProperties

func init() {
	Properties = &ServerProperties{
		Bind: "127.0.0.1",
		Port: 6379,
	}
}

func parse(src io.Reader) *ServerProperties {
	config := &ServerProperties{}
	rawMap := make(map[string]string)
	// read config file
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		line := scanner.Text()
		// 判断是否当前行被注释
		if len(line) > 0 && strings.TrimLeft(line, " ")[0] == '#' {
			continue
		}
		parts := strings.Split(line, " ")
		if len(parts) == 2 {
			key := parts[0]
			value := strings.Trim(parts[1], " ")
			rawMap[strings.ToLower(key)] = value
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}
	// store properties into Properties
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldValue := v.Elem().Field(i)
		// get key from tag if not exists, use field name
		key, ok := field.Tag.Lookup("cfg")
		if !ok || strings.TrimLeft(key, " ") == "" {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			switch field.Type.Kind() {
			case reflect.String:
				fieldValue.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldValue.SetInt(intValue)
				}
			case reflect.Bool:
				boolValue := value == "yes"
				fieldValue.SetBool(boolValue)
			}
		}

	}
	return config
}

// SetupConfig read config file and store properties into Properties
func SetupConfig(configFilename string) {
	// read config file
	file, err := os.Open(configFilename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// store properties into Properties
	Properties = parse(file)
	configFilePath, err := filepath.Abs(configFilename)
	if err != nil {
		return
	}
	Properties.CfPath = configFilePath
	if Properties.Dir == "" {
		Properties.Dir = "."
	}

}
