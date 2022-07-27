package taillog

import (
	"fmt"
	"github.com/hpcloud/tail"
)

// 专门收集日志文件

var (
	tailObj *tail.Tail
)

func Init(fileName string) (err error) {
	config := tail.Config{
		ReOpen:    true,                                 // 打开文件
		Follow:    true,                                 // 文件切割自动重新打开
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // Location读取文件的位置, Whence更加系统选择参数
		MustExist: false,                                // 允许日志文件不存在
		Poll:      true,                                 // 轮询
	}
	// 打开文件读取日志
	tailObj, err = tail.TailFile(fileName, config)
	if err != nil {
		fmt.Println("tail file failed, err:", err)
		return
	}
	return
}

func ReadChan() <-chan *tail.Line {
	return tailObj.Lines
}
