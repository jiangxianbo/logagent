package main

import (
	"fmt"
	"gopkg.in/ini.v1"
	"logagent/conf"
	"logagent/etcd"
	"logagent/kafka"
	"logagent/taillog"
	"logagent/utils"
	"sync"
	"time"
)

var (
	cfg = new(conf.AppConf)
)

// logAgent 入口程序
func main() {
	// 0.加载配置文件
	err := ini.MapTo(cfg, "./conf/config.ini")
	if err != nil {
		fmt.Printf("load ini failed, err:%v\n", err)
		return
	}

	// 1.初始化kafka连接
	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.ChanMaxSize)
	if err != nil {
		fmt.Printf("init kafka failed, err:%v\n", err)
		return
	}
	fmt.Println("init kafka sucess!")

	// 2. 初始化etccd
	err = etcd.Init(cfg.EtcdConf.Address, time.Duration(cfg.Timeout)*time.Second)
	if err != nil {
		fmt.Printf("init etcd failed, err:%v\n", err)
		return
	}

	// 为了实现每个loggent都拉取自己独有的配置，所以要以自己的ip地址作为区分
	ipStr, err := utils.GetOutBoundIP()
	if err != nil {
		panic(err)
	}
	etcdConfKey := fmt.Sprintf(cfg.EtcdConf.Key, ipStr)
	// 2.1 从etcd中获取日志收集项的配置信息
	logEntryConf, err := etcd.GetConf(etcdConfKey)
	if err != nil {
		fmt.Printf("etcd.GetConf failed, err:%v\n", err)
		return
	}
	fmt.Printf("get conf from etcd sucess, %v\n", logEntryConf)

	// 2.2 派一个哨兵去监控之日收集项的变化（有变化及时通知我的logAgent实现热加载配置）

	for index, value := range logEntryConf {
		fmt.Printf("index:%v, value:%v \n", index, value)
	}

	// 3. 收集日志发往kafka
	taillog.Init(logEntryConf)
	// 因为NewConfChan访问了tskMgr的newConfChan，这个channel是在taillog.init(logEntryConf) 执行初始化
	newConfChan := taillog.NewConfChan() // 从taillog包中获取对外暴漏的通道

	var wg sync.WaitGroup
	wg.Add(1)
	go etcd.WatchConf(etcdConfKey, newConfChan) // 哨兵发现最新的配置新会通知上面的通道
	wg.Wait()

	// 2.打开日志文件准备收集文件
	//	err = taillog.Init(cfg.TaillogConf.FileName)
	//	if err != nil {
	//		fmt.Printf("init taillog failed, err:%v\n", err)
	//		return
	//	}
	//	fmt.Println("init taillog sucess!")
	// run()
}
