package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"gopkg.in/yaml.v2"
)

//全局变量
var sourceHost string  // 源地址
var scanCount int64    // 单页扫描数 默认20000
var logger *log.Logger // 日志记录器

//配置结构体
type Migrate struct {
	Source    Redis  `yaml:"source"`    // 源地址
	ScanCount int64  `yaml:"scanCount"` // 单页扫描数
	LogFile   string `yaml:"logFile"`   // 日志文件目录
}

type Redis struct {
	Host string `yaml:"host"`
}

func init() {
	//读取配置
	yamlFile, err := ioutil.ReadFile("/home/bestv/code/redis-migrate/keys.yml")
	config := Migrate{}
	err = yaml.Unmarshal([]byte(yamlFile), &config)
	if err != nil {
		panic(err)
	}
	//赋值参数
	if config != (Migrate{}) {
		sourceHost = config.Source.Host
		scanCount = config.ScanCount
	}
	// 定义日志工具
	file, err := os.Create(config.LogFile)
	if err != nil {
		log.Fatalln("fail to create " + config.LogFile + " file!")
	}
	logger = log.New(file, "", log.Llongfile)
	logger.SetFlags(log.LstdFlags)
}

func main() {
	timeBegin := time.Now().UnixNano() //获取微妙时间戳
	cSource, err := redis.Dial("tcp", sourceHost)
	if err != nil {
		fmt.Println("Connect to redis error", err)
		return
	}
	defer func() {
		if cSource != nil {
			cSource.Close()
		} else {
			panic("redis连接回收失败!!!")
		}
	}()

	//迭代器
	iter := 0
	//存储keys
	var keys []string
	var length int = 0
	//全量迁移
	for {
		//扫描redis
		if arr, err := redis.Values(cSource.Do("SCAN", iter, "COUNT", scanCount)); err != nil {
			panic(err)
		} else {
			//获取迭代器和keys
			iter, _ = redis.Int(arr[0], nil)
			keys, _ = redis.Strings(arr[1], nil)
			keyJoin := strings.Join(keys, "\r\n")
			logger.Println(keyJoin)
		}
		length += len(keys)
		//记录日志
		fmt.Println("当前scan序号: ", iter, " ;本次读取的总数据: ", length)
		if iter == 0 {
			break
		}
	}
	logger.Println("总的长度=", length)
	//获取结束时间
	timeEnd := time.Now().UnixNano()
	duration := (timeEnd - timeBegin) / 1e9
	s := strconv.FormatInt(duration, 10)
	fmt.Println(s + "s")
}
