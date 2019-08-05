package storage

import (
	"fmt"
	"time"
)

var ErrExist = fmt.Errorf("记录已存在")
var ErrNotExist = fmt.Errorf("记录不存在")

// 信息存储
type Bucket interface {
	Set(key string, value string) error       // Key重复时，直接覆盖
	Get(key string) (value string, err error) // 未找到时，返回ErrNotExist
	Delete(key string) error                  // 删除不存在记录时，返回ErrNotExist
	Keys() ([]string, error)
	Truncate() error
}

// 过滤器, 保存已完成的URL
type Filter interface {
	Insert(url string) error
	Delete(url string) error
	Lookup(url string) (bool, error)
	Count() (uint, error)
	Truncate() error
}

// URL优先级
type Priority int

const (
	Priority0 Priority = iota
	Priority1
	Priority2
	Priority3
	Priority4
)

// URL状态
type State int

const (
	StateNotExist   State = iota // URL未在队列中
	StateWaiting                 // URL正在队列中
	StateProcessing              // URL已被调度
)

// 队列中的URL
type QueueItem struct {
	ID       int64     `db:"id"`
	URL      string    `db:"url"`
	State    State     `db:"state"`
	Priority Priority  `db:"priority"`
	Created  time.Time `db:"created"`
	Updated  time.Time `db:"updated"`
	Count    int       `db:"count"` // 用于SQL查询统计数量
}

// 队列, 保存等待中,进行中的URL
type Queue interface {
	AddDirect(url string, priority Priority) (bool, error) // 添加URL到队列中
	Add(url string, priority Priority) (bool, error)       // 若URL未在Filter中，则添加URL到队列中
	Pop() (item QueueItem, err error)                      // 弹出最优先URL, 无URL时返回io.EOF
	Length(state State) (map[Priority]int, error)          // 获取各优先级的队列长度
	Truncate() error                                       // 清空队列
	Collect() ([]QueueItem, error)                         // 清理超时的URL
	Finish(url string) (bool, error)                       // 报告URL已完成
	Lookup(url string) (State, error)                      // URL是否存在
}
