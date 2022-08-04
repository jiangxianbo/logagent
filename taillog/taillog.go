package taillog

import (
	"context"
	"fmt"
	"logagent/kafka"

	"github.com/hpcloud/tail"
)

// 专门收集日志文件

var (
	tailObj *tail.Tail
)

// TailTask 一个任务收集的任务
type TailTask struct {
	path     string
	topic    string
	instance *tail.Tail
	// 为了能实现退出t.run()
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewTailTask(path, topic string) (tailObj *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailObj = &TailTask{
		path:       path,
		topic:      topic,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	tailObj.init() // 根据路径去打开对应的日志
	return
}

func (t *TailTask) init() {
	config := tail.Config{
		ReOpen:    true,                                 // 打开文件
		Follow:    true,                                 // 文件切割自动重新打开
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // Location读取文件的位置, Whence更加系统选择参数
		MustExist: false,                                // 允许日志文件不存在
		Poll:      true,                                 // 轮询
	}
	// 打开文件读取日志
	var err error
	t.instance, err = tail.TailFile(t.path, config)
	if err != nil {
		fmt.Println("tail file failed, err:", err)
	}

	// 当goroutine执行的函数退出的时候，grooutine就结束了
	go t.run()
}

func (t TailTask) run() {
	for {
		select {
		case <-t.ctx.Done():
			fmt.Printf("tail task:%s_%s 结束了...", t.path, t.topic)
			return
		case line := <-t.instance.Lines: // 从tailObj的通道中一行一行读日志数据
			// 3.2 发往kafka
			// kafka.SendToKafka(t.topic, line.Text) // 函数调佣函数

			// 先把日志数据发送到一个通道中
			kafka.SendToChan(t.topic, line.Text)
			// kafka包中有个单独的 goroutine 去取日志数据发送到 kafka

		}
	}
}
