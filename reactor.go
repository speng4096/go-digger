package digger

import (
	"github.com/pkg/errors"
	"github.com/spencer404/go-digger/storage"
	"github.com/spencer404/go-digger/transport"
	"io"
	"log"
	"time"
)

// 反应堆可选参数
type ReactorOpt struct {
	transport     transport.Maker
	interval      *time.Duration
	downloadRetry *int
	debug         *bool
}

func NewReactorOpt() *ReactorOpt {
	return &ReactorOpt{}
}

func (r *ReactorOpt) Transport(t transport.Maker) *ReactorOpt {
	r.transport = t
	return r
}

func (r *ReactorOpt) Interval(d time.Duration) *ReactorOpt {
	r.interval = &d
	return r
}

func (r *ReactorOpt) DownloadRetry(i int) *ReactorOpt {
	r.downloadRetry = &i
	return r
}

func (r *ReactorOpt) Debug(enable bool) *ReactorOpt {
	r.debug = &enable
	return r
}

// 反应堆
type Reactor struct {
	Queue         storage.Queue
	Bucket        storage.Bucket
	Transport     transport.Maker
	Interval      time.Duration
	DownloadRetry int
	popErrCount   int // 队列连续弹出失败计数器
	parallels     int // Goroutine数量
}

func (r *Reactor) MustRun(spider *Spider) {
	if err := r.Run(spider); err != nil {
		panic(err)
	}
}

// 启动爬虫
// TODO: 使用支持日志等级的日志模块
// TODO: DownloadRetry变为Retry，根据OnProcess返回值决定是否Retry
func (r *Reactor) Run(spider *Spider) error {
	// 初始化爬虫
	// Spider字段检查与设置默认值
	if spider.OnInit == nil {
		spider.OnInit = func() error {
			return nil
		}
	}
	if spider.OnProcess == nil {
		return errors.Errorf("未设置Spider.OnProcess")
	}
	// 设置初始化标识
	if _, err := r.Bucket.Get("_IsInit"); err == storage.ErrNotExist {
		log.Printf("正在执行OnInit")
		// 运行爬虫初始化函数
		if err := spider.OnInit(); err != nil {
			return errors.Wrap(err, "初始化爬虫失败")
		}
		if err := r.Bucket.Set("_IsInit", ""); err != nil {
			return errors.Wrap(err, "启动爬虫失败，设置'_IsInit'失败")
		}
		// 注入Seeders
		log.Printf("正在注入%d个Seeders", len(spider.Seeders))
		n := 0
		for _, url := range spider.Seeders {
			ok, err := r.Queue.Add(url, storage.Priority0)
			if err != nil {
				return errors.Wrapf(err, "启动爬虫失败，注入Seeder:%q失败", url)
			}
			if ok {
				n++
			}
		}
		log.Printf("成功注入%d个Seeders", n)
	} else if err != nil {
		return errors.Wrap(err, "启动爬虫失败，未能获取到'_IsInit'")
	}
	// 注入资源
	spider.Queue = r.Queue
	spider.Bucket = r.Bucket
	// 监听队列
	loopCh := make(chan int, r.parallels)
	for i := 0; i < r.parallels; i++ {
		go func(i int) {
			log.Printf("进入调度循环: %d", i)
			for {
				// 弹出Item
				item, err := r.Queue.Pop()
				if err == io.EOF {
					// TODO: 全部Goroutine都空闲时才可停止
					// 所有Goroutine要么同时运行，要么同时停止
					// 因为队列空时，运行中的Goroutine还会继续往队列添加新的URL
					log.Printf("队列已空")
					//break
					time.Sleep(time.Second) // TODO: 临时方法
					continue
				} else if err != nil {
					r.popErrCount++
					log.Printf("队列弹出失败: %s", err)
					c := time.Second * time.Duration(r.popErrCount)
					time.Sleep(c)
					log.Printf("休眠结束: %s", c.String())
					continue
				}
				r.popErrCount = 0
				// 交由Spider处理
				// TODO: 重试逻辑
				if err := spider.OnProcess(item.URL, r); err != nil {
					log.Printf("执行失败: %s, %s", item.URL, err)
				}
				if _, err := r.Queue.Finish(item.URL); err != nil {
					log.Printf("从队列移除%q失败: %s", item.URL, err)
				}
				// 速率控制 TODO: 精细地控制interval
				time.Sleep(r.Interval)
			}
			loopCh <- i
		}(i)
		time.Sleep(time.Second) // 慢慢启动
	}
	log.Printf("全部Groutines已启动")
	for k := 0; k < r.parallels; k++ {
		i := <-loopCh
		log.Printf("%d号Grourine已结束", i)
	}
	log.Printf("全部Groutine执行完毕")
	return nil
}

func MustNewReactor(queue storage.Queue, bucket storage.Bucket, parallels int, opt *ReactorOpt) *Reactor {
	r, err := NewReactor(queue, bucket, parallels, opt)
	if err != nil {
		panic(err)
	}
	return r
}

func NewReactor(queue storage.Queue, bucket storage.Bucket, parallels int, opt *ReactorOpt) (*Reactor, error) {
	// 默认参数
	reactor := Reactor{
		Queue:         queue,
		Bucket:        bucket,
		Transport:     transport.DirectTransport(),
		Interval:      0,
		DownloadRetry: 3,
		parallels:     parallels,
	}
	// 可选参数
	if opt.transport != nil {
		reactor.Transport = opt.transport
	}
	if opt.interval != nil {
		reactor.Interval = *opt.interval
	}
	if opt.downloadRetry != nil {
		reactor.DownloadRetry = *opt.downloadRetry
	}
	// 调试模式, 清空资源
	if opt.debug != nil && *opt.debug {
		log.Println("进入调试模式")
		log.Println("正在清空queue")
		if err := queue.Truncate(); err != nil {
			return nil, errors.Wrapf(err, "清空queue失败")
		}
		log.Println("正在清空bucket")
		if err := bucket.Truncate(); err != nil {
			return nil, errors.Wrapf(err, "清空bucket失败")
		}
	}
	return &reactor, nil
}
