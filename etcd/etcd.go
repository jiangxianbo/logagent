package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	cli *clientv3.Client
)

// LogEntry 需要收集的日志的配置信息
type LogEntry struct {
	Path  string `ini:"path"`  // 日志存放的路径
	Topic string `ini:"topic"` // 日志要发往kafka中的那个topic
}

// Init 初始化
func Init(addrs string, timeout time.Duration) (err error) {
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{addrs},
		DialTimeout: timeout,
	})
	if err != nil {
		// handle error!
		fmt.Printf("connect to etcd failed, err:%v\n", err)
		return
	}
	fmt.Println("connect to etcd success")
	return
}

// GetConf 从ETCD中根据key获取配置
func GetConf(key string) (logEntryConf []*LogEntry, err error) {
	// get
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Printf("get from etcd failed, err:%v\n", err)
		return
	}
	for _, ev := range resp.Kvs {
		// fmt.Printf("%s:%s\n", ev.Key, ev.Value)
		err = json.Unmarshal(ev.Value, &logEntryConf)
		if err != nil {
			fmt.Printf("Unmarshal etcd value failed, err:%v\n", err)
			return
		}
	}
	return
}

// WatchConf etch watch
func WatchConf(key string, newConfCh chan<- []*LogEntry) {
	rch := cli.Watch(context.Background(), key) // <-chan WatchResponse
	for wresp := range rch {
		for _, evt := range wresp.Events {
			fmt.Printf("Type: %s Key:%s Value:%s\n", evt.Type, evt.Kv.Key, evt.Kv.Value)
			// 通知taillog.tskMgr
			// 先判断操作的类型
			var newConf []*LogEntry
			if evt.Type != clientv3.EventTypeDelete {
				// 如果是删除操作，手动传递空
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					fmt.Printf("Unmarshal failed, err:%v\n", err)
				}
			}

			fmt.Printf(" get new conf:%v\n", newConf)
			newConfCh <- newConf
		}
	}
}
