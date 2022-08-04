package taillog

import (
	"fmt"
	"logagent/etcd"
	"time"
)

var (
	tskMgr *taillogMgr
)

// taillogMgr tailTask 管理者
type taillogMgr struct {
	logEntry    []*etcd.LogEntry
	tskMap      map[string]*TailTask
	newConfChan chan []*etcd.LogEntry
}

// Init 初始化
func Init(logEntryConf []*etcd.LogEntry) {
	tskMgr = &taillogMgr{
		logEntry:    logEntryConf, // 把当前的日志收集任配置信息存起来
		tskMap:      make(map[string]*TailTask, 16),
		newConfChan: make(chan []*etcd.LogEntry), // 无缓冲区的通道
	}
	for _, logEntry := range logEntryConf {
		// conf:etcd.logEntry
		// logEntry.Path：要手机的日志文件路径
		// 初始化齐了多少个tailtask 都要记下来，为了后续判断方便
		tailObj := NewTailTask(logEntry.Path, logEntry.Topic)
		mk := fmt.Sprintf("%s_%s", logEntry.Path, logEntry.Topic)
		tskMgr.tskMap[mk] = tailObj
	}

	go tskMgr.run()
}

// run 监听自己的newConfChan，有新的配置过来之后做对应的处理
func (t taillogMgr) run() {
	for {
		select {
		case newConf := <-t.newConfChan:
			for _, conf := range newConf {
				mk := fmt.Sprintf("%s_%s", conf.Path, conf.Topic)
				_, ok := t.tskMap[mk]
				if ok {
					// 原来就有，不需要操作
					continue
				} else {
					// 新增的
					tailObj := NewTailTask(conf.Path, conf.Topic)
					t.tskMap[mk] = tailObj
				}
			}
			// 原来有t.logEntry有，但是newConf中没有，要删掉
			for _, c1 := range t.logEntry { // 从原配置中一次拿出配置项
				isDelete := true
				for _, c2 := range newConf { // 去新的配置中逐一进行比较
					if c1.Path == c2.Path && c1.Topic == c2.Topic {
						isDelete = false
						continue
					}
				}
				if isDelete {
					// 把c1对应的这个tailObj停掉
					mk := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
					// t.tskMap[mk] ==> tailObj
					t.tskMap[mk].cancelFunc()
				}
			}

			// 1. 配置新增
			// 2. 配置删除
			// 3. 配置变更
			fmt.Println("新配置来了", newConf)
		default:
			time.Sleep(time.Second)
		}
	}
}

// 一个函数向外暴漏 TskMgr的newConfChan
func NewConfChan() chan<- []*etcd.LogEntry {
	return tskMgr.newConfChan
}
