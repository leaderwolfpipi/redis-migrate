package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/chasex/redis-go-cluster"
	redigo "github.com/garyburd/redigo/redis"
	"gopkg.in/yaml.v2"
)

//全局变量
var sourceHost string = ""     // 源地址
var destHost string = ""       // 目标地址
var destType int = 0           // 目标类型
var sourceType int = 0         // 目标类型
var logFile string = ""        // 日志文件
var scanCount int64 = 20000    // 单页扫描数 默认10000
var logger *log.Logger         // 日志记录器
var config Migrate = Migrate{} // 配置
var wGroup = sync.WaitGroup{}  // 用于控制goroutine的结束

// 配置结构体
type Migrate struct {
	Source     Redis  `yaml:"source"`     // 源地址
	DestRedis  Redis  `yaml:"destRedis"`  // 目标redis地址
	DestType   int    `yaml:"destType"`   // 目标类型
	SourceType int    `yaml:"sourceType"` // 源类型
	ScanCount  int64  `yaml:"scanCount"`  // 单页扫描数
	LogFile    string `yaml:"logFile"`    // 日志文件目录
}

type Redis struct {
	Host     string `yaml:"host"`
	Password string `yaml:"password"`
}

func init() {
	// 读取配置
	yamlFile, err := ioutil.ReadFile("/home/louhao/kibana-log/redis-migration.yml")
	err = yaml.Unmarshal([]byte(yamlFile), &config)
	if err != nil {
		panic(err)
	}
	// 赋值参数
	if config != (Migrate{}) {
		sourceHost = config.Source.Host
		destHost = config.DestRedis.Host
		destType = config.DestType
		sourceType = config.SourceType
		scanCount = config.ScanCount
		logFile = config.LogFile
	}
	// 定义日志工具
	file, err := os.Create(logFile)
	if err != nil {
		log.Fatalln("fail to create " + logFile + " file!")
	}
	logger = log.New(file, "", log.Llongfile)
	logger.SetFlags(log.LstdFlags)
}

// 异常处理
func panicHandler() {
	if err := recover(); err != nil {
		// 错误返回
		logger.Println("捕获异常:", err)
	}
}

// 主函数
func main() {
	// 处理异常捕获
	defer panicHandler()
	// 循环拷贝
	logger.Println("主机:", sourceHost)
	for i := 0; i < 30; i++ {
		wGroup.Add(1)
		go build(sourceHost)
	}
	// 等待所有goroutine结束
	wGroup.Wait()

}

// 实际构建流程
func build(host string) {
	// 起始时间
	timeBegin := time.Now().UnixNano() // 获取微妙时间戳
	// 变量定义
	var keys []string           // keys
	var respArr []string        // 值
	var expArr []int            // 过期时间
	var err error = nil         // 源错误
	var errDest error = nil     // 目标错误
	var cSource interface{}     // 源连接
	var cDest interface{}       // 目标连接
	var cs *redis.Cluster = nil // 集群源连接
	var ss redigo.Conn = nil    // 单机源连接
	var cd *redis.Cluster = nil // 集群目标连接
	var sd redigo.Conn = nil    // 单机目标连接
	var length int = 0          // 长度
	iter := 0                   // 迭代器
	var ok bool = false         // 判断标志
	// 导出源
	if config.SourceType == 0 {
		// 集群
		cSource, err = redis.NewCluster(
			&redis.Options{
				StartNodes:   []string{sourceHost},
				ConnTimeout:  2000 * time.Millisecond,
				ReadTimeout:  2000 * time.Millisecond,
				WriteTimeout: 2000 * time.Millisecond,
				KeepAlive:    16,
				AliveTime:    60 * time.Second,
			})

	} else {
		// 单机（代理）
		cSource, err = redigo.Dial("tcp", sourceHost)
	}
	// 导出目标
	if config.DestType == 0 {
		// 集群
		cDest, errDest = redis.NewCluster(
			&redis.Options{
				StartNodes:   []string{destHost},
				ConnTimeout:  2000 * time.Millisecond,
				ReadTimeout:  2000 * time.Millisecond,
				WriteTimeout: 2000 * time.Millisecond,
				KeepAlive:    16,
				AliveTime:    60 * time.Second,
			})
	} else {
		// 单机（代理）
		cDest, errDest = redigo.Dial("tcp", destHost)
	}
	// 判断连接
	if err != nil || errDest != nil {
		fmt.Println("Connect to redis error", err, errDest)
		return
	}
	// 延迟关闭
	defer func() {
		if cs, ok = cSource.(*redis.Cluster); ok && cs != nil {
			cs.Close()
		} else if ss, ok = cSource.(redigo.Conn); ok && cs != nil {
			cs.Close()
		}
		if cd, ok = cDest.(*redis.Cluster); ok && cd != nil {
			cd.Close()
		} else if sd, ok = cDest.(redigo.Conn); ok && sd != nil {
			sd.Close()
		}
	}()
	var totalKeyNum int = 0
	// 全量迁移
	for {
		if config.SourceType == 0 { // 集群
			// 扫描redis
			if cs, ok = cSource.(*redis.Cluster); !ok {
				panic("源集群连接错误")
			}
			if arr, err := redis.Values(cs.Do("SCAN", iter, "COUNT", scanCount)); err != nil {
				panic(err)
			} else {
				// 获取迭代器和keys
				iter, _ = redis.Int(arr[0], nil)
				keys, _ = redis.Strings(arr[1], nil)
			}
			length = len(keys)
			respArr = make([]string, length)
			expArr = make([]int, length)
			// 获取键值对
			batch := cs.NewBatch()
			for _, itemKey := range keys {
				err = batch.Put("GET", itemKey)
				if err != nil {
					panic(err)
				}
			}
			// 批量执行
			reply, err0 := cs.RunBatch(batch)
			length = len(reply)
			if err0 != nil {
				panic(err0)
			}
			for i := 0; i < length; i++ {
				var resp interface{} = nil
				var r []byte
				reply, err = redis.Scan(reply, &resp)
				// 只取字符串类型
				if r, ok = resp.([]byte); ok {
					respArr[i] = string(r)
				} else {
					logger.Println("异常类型：nil")
				}
				if err != nil {
					// 只记录异常
					logger.Println("捕获异常:", err)
				}
			}
			batch = cs.NewBatch()
			// 获取过期时间
			for _, itemEXP := range keys {
				batch.Put("TTL", itemEXP)
			}
			// 批量执行
			reply, err = cs.RunBatch(batch)
			length = len(reply)
			for i := 0; i < length; i++ {
				var respInt int = 0
				reply, err = redis.Scan(reply, &respInt)
				if err != nil {
					panic(err)
				}
				expArr[i] = respInt
			}
		} else {
			// 单机（代理）
			if ss, ok = cSource.(redigo.Conn); !ok {
				panic("单机（代理）连接错误")
			}
			//扫描redis
			if arr, err := redis.Values(ss.Do("SCAN", iter, "COUNT", scanCount)); err != nil {
				panic(err)
			} else {
				//获取迭代器和keys
				iter, _ = redis.Int(arr[0], nil)
				keys, _ = redis.Strings(arr[1], nil)
			}

			length = len(keys)
			respArr = make([]string, length)
			expArr = make([]int, length)
			//获取键值对
			for _, itemKey := range keys {
				ss.Send("GET", itemKey)
			}
			//flush进redis
			ss.Flush()
			for i := 0; i < length; i++ {
				tmpV, errV := ss.Receive()
				if errV != nil {
					panic(errV)
				}
				respArr[i] = string(tmpV.([]byte))
			}
			//获取过期时间
			for _, itemEXP := range keys {
				ss.Send("TTL", itemEXP)
			}
			//flush进redis
			ss.Flush()
			for i := 0; i < length; i++ {
				tmpEXP, errEXP := ss.Receive()
				if errEXP != nil {
					panic(errEXP)
				}
				expArr[i] = tmpEXP.(int)
			}
		}
		// 写入目标
		if config.DestType == 0 {
			// 集群
			if cd, ok = cDest.(*redis.Cluster); !ok {
				panic("集群目标连接错误")
			}
			batchD := cd.NewBatch()
			// 存储键值+过期时间
			for index, kOrigin := range keys {
				batchD.Put("SET", kOrigin, respArr[index])
				if expArr[index] > 0 {
					batchD.Put("EXPIRE", kOrigin, expArr[index])
				}
			}
			// 批量操作
			_, errInsert := cd.RunBatch(batchD)
			if errInsert != nil {
				fmt.Println(errInsert)
			}
		} else {
			// 单机(代理)
			if sd, ok = cDest.(redigo.Conn); !ok {
				panic("单机目标连接错误")
			}
			//存储键值+过期时间
			for index, kOrigin := range keys {
				totalKeyNum++
				sd.Send("SET", kOrigin, respArr[index])
				if expArr[index] > 0 {
					sd.Send("EXPIRE", kOrigin, expArr[index])
				}
			}
			//flush进redis
			sd.Flush()
			if err != nil {
				fmt.Println(err)
				return
			}
		}
		//记录日志
		logger.Println("当前scan序号: ", iter, " ;本次读取的总数据: ", length)
		if iter == 0 {
			break
		}
	}
	//记录日志
	logger.Println("总的key的数目: ", totalKeyNum)
	//获取结束时间
	timeEnd := time.Now().UnixNano()
	duration := (timeEnd - timeBegin) / 1e9
	s := strconv.FormatInt(duration, 10)
	fmt.Println(s + "s")
	wGroup.Done()
}
