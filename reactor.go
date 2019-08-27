package digger

import (
	"crypto/tls"
	"fmt"
	"github.com/ReneKroon/ttlcache"
	mapset "github.com/deckarep/golang-set"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"github.com/spencer404/go-digger/proxy"
	"github.com/spencer404/go-digger/storage"
	"io"
	"log"
	"reflect"
	"sync"
	"time"
)

// TODO: 彩色日志，区分时间、Reactor、Spider、Error、Warning
// 反应堆可选参数
type ReactorOpt struct {
	providers      []proxy.Provider
	interval       *time.Duration
	downloadRetry  *int
	debug          *bool
	proxyParallels *int
}

func NewReactorOpt() *ReactorOpt {
	return &ReactorOpt{}
}

func (r *ReactorOpt) ProxyProviders(p ...proxy.Provider) *ReactorOpt {
	r.providers = p
	return r
}

func (r *ReactorOpt) ProxyParallels(i int) *ReactorOpt {
	r.proxyParallels = &i
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
	Queue          storage.Queue
	Bucket         storage.Bucket
	Interval       time.Duration
	Retry          int
	popErrCount    int // 队列连续弹出失败计数器
	parallels      int // Goroutine数量
	proxyParallels int // 代理并发量
	providers      []proxy.Provider
	poolCh         chan *Proxy
	putBackList    []*Proxy
	poolFilter     mapset.Set
	blackList      *ttlcache.Cache
	freezeList     freezeList
	freezeLock     sync.Mutex
	putBackLock    sync.Mutex
	poolLock       sync.Mutex
}

func (r *Reactor) MustRun(spider *Spider) {
	if err := r.Run(spider); err != nil {
		panic(err)
	}
}

func (r *Reactor) makeProxy(item *storage.QueueItem) (*Proxy, error) {
	if r.providers == nil {
		return nil, nil
	}
	log.Printf("正在申请代理")
	r.poolLock.Lock()
	defer r.poolLock.Unlock()
	m := len(r.poolCh) + 1
	for i := 0; i < m; i++ {
		select {
		case p := <-r.poolCh:
			log.Printf("已获得代理: %s", p.URL)
			// 若在黑名单中，则丢弃
			if _, exist := r.blackList.Get(p.URL); exist {
				key := fmt.Sprintf("%s:%d", p.URL, p.Index)
				r.poolFilter.Remove(key)
				log.Printf("获得的代理已被拉黑: %s", p.URL)
				continue
			}
			// 若TTL过期，则丢弃
			if !p.ExpiredTime.IsZero() && p.ExpiredTime.Before(time.Now()) {
				key := fmt.Sprintf("%s:%d", p.URL, p.Index)
				r.poolFilter.Remove(key)
				log.Printf("获得的代理已过期: %s", p.URL)
				continue
			}
			// 若在冻结列表中，也给冻结起来
			r.freezeLock.Lock()
			if _, exist := r.freezeList[p.URL]; exist {
				r.freezeList[p.URL].ps = append(r.freezeList[p.URL].ps, p)
				r.freezeLock.Unlock()
				log.Printf("获得的代理将被冻结: %s", p.URL)
				continue
			}
			r.freezeLock.Unlock()
			// 代理正常
			return p, nil
		case <-time.After(time.Second * 3):
			return nil, errors.Errorf("获取代理超时")
		}
	}
	return nil, errors.Errorf("代理池忙")
}

func (r *Reactor) startProxyPool() {
	r.poolCh = make(chan *Proxy, r.parallels*2)
	r.putBackList = make([]*Proxy, 0)
	r.poolFilter = mapset.NewSet()
	r.blackList = ttlcache.NewCache()
	r.freezeList = make(freezeList, 0)

	// 获取新代理，providers -> poolCh
	go func() {
		cases := make([]reflect.SelectCase, len(r.providers))
		for i, provider := range r.providers {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(provider)}
		}
		for {
			// 暂存区优先级高，先从这获取代理填充poolCh
			var p *Proxy
			for {
				r.putBackLock.Lock()
				if len(r.putBackList) == 0 {
					r.putBackLock.Unlock()
					break
				}
				p, r.putBackList = r.putBackList[0], r.putBackList[1:]
				r.putBackLock.Unlock()
				r.poolCh <- p
				log.Printf("暂存区代理已入队: %s", p.URL)
			}
			// 暂存区满后，获取新代理填充poolCh
			chosen, value, ok := reflect.Select(cases)
			// 当有proxy退出时，ok==false, chosen为cases下标
			if !ok {
				if len(cases) == 1 {
					cases = make([]reflect.SelectCase, 0)
					log.Printf("ProxyPool中已无Provider，结束运行")
					break
				}
				cases = append(cases[:chosen], cases[chosen+1:]...)
				log.Printf("ProxyPool中有Provider退出")
			}
			urlItem := value.Interface().(proxy.Item)
			url := urlItem.URL
			log.Printf("从Provider处获得新代理: %s", url)
			if _, exist := r.blackList.Get(url); exist {
				log.Printf("新代理在黑名单中, 未能入队: %s", url)
				continue
			}
			for i := 0; i < r.proxyParallels; i++ {
				key := fmt.Sprintf("%s:%d", url, i)
				if urlItem.EnableFilter && r.poolFilter.Contains(key) {
					log.Printf("新代理重复, 未能入队: %s", key)
					continue
				}
				r.poolFilter.Add(key)
				r.poolCh <- &Proxy{
					URL:         url,
					Index:       i,
					CreateTime:  time.Now(),
					ExpiredTime: time.Time{},
				}
				log.Printf("新代理已入队: %s:%d", url, i)
			}

		}
	}()

	// 扫描冻结列表，freezeList -> putBackList
	go func() {
		for ; ; time.Sleep(time.Second) {
			r.freezeLock.Lock()
			for url, item := range r.freezeList {
				if item.t.Before(time.Now()) {
					for _, p := range item.ps {
						r.putBackLock.Lock()
						r.putBackList = append(r.putBackList, p)
						r.putBackLock.Unlock()
						log.Printf("代理已解冻: %s", p.URL)
					}
					delete(r.freezeList, url)
				}
			}
			r.freezeLock.Unlock()
		}
	}()
}

// 启动爬虫
// TODO: 启动时将进行态URL置为等待态
// TODO: 使用支持日志等级的日志模块
// TODO: DownloadRetry变为Retry，根据OnProcess返回值决定是否Retry
func (r *Reactor) Run(spider *Spider) error {
	// 初始化爬虫
	// Spider字段检查与设置默认值
	if spider.OnInit == nil {
		spider.OnInit = func(reactor *Reactor) error {
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
		if err := spider.OnInit(r); err != nil {
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
	r.startProxyPool()
	// 监听队列
	loopCh := make(chan int, r.parallels)
	loopBreak := make(map[int]bool, r.parallels)
	for i := 0; i < r.parallels; i++ {
		loopBreak[i] = false
		go func(i int) {
			log.Printf("%d号Grourine已启动", i)
		QueueLoop:
			for {
				// 弹出Item
				item, err := r.Queue.Pop()
				if err == io.EOF {
					log.Printf("%d号Grourine报告队列已空", i)
					// 所有Goroutine要么同时运行，要么同时停止
					// 因为队列空时，运行中的Goroutine还会继续往队列添加新的URL
					time.Sleep(time.Second * time.Duration(r.parallels/2))
					loopBreak[i] = true
					for _, v := range loopBreak {
						if !v {
							continue QueueLoop
						}
					}
					break
				} else if err != nil {
					r.popErrCount++
					log.Printf("队列弹出失败: %s", err)
					c := time.Second * time.Duration(r.popErrCount)
					time.Sleep(c)
					log.Printf("休眠结束: %s", c.String())
					continue
				}
				loopBreak[i] = false
				r.popErrCount = 0
				// 客户端
				p, err := r.makeProxy(&item)
				if err != nil {
					log.Printf("获取代理失败: %s", err)
					continue
				}
				c := resty.New()
				if p != nil {
					c.SetProxy(p.URL)
					c.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true})
				}
				ph := &ProxyHelper{}
				// 交由Spider处理
				// TODO: 重试逻辑
				log.Printf("正在执行: %s", item.URL)
				if err := spider.OnProcess(item.URL, c, ph, r); err != nil {
					log.Printf("执行失败: %s, %s", item.URL, err)
					if ph.flag == FlagUnset {
						ph.flag = FlagDelete
					}
				}
				if _, err := r.Queue.Finish(item.URL); err != nil {
					log.Printf("从队列移除%q失败: %s", item.URL, err)
				}
				// 处理代理
				switch ph.flag {
				case FlagPutBack, FlagUnset:
					r.putBackLock.Lock()
					r.putBackList = append(r.putBackList, p)
					r.putBackLock.Unlock()
				case FlagFreeze:
					r.freezeLock.Lock()
					if _, exist := r.freezeList[p.URL]; exist {
						r.freezeList[p.URL].ps = append(r.freezeList[p.URL].ps, p)
						r.freezeList[p.URL].t = time.Now().Add(ph.flagD)
					} else {
						r.freezeList[p.URL] = &freezeItem{t: time.Now().Add(ph.flagD), ps: []*Proxy{p}}
					}
					r.freezeLock.Unlock()
				case FlagForbidden:
					r.blackList.SetWithTTL(p.URL, struct{}{}, ph.flagD)
					key := fmt.Sprintf("%s:%d", p.URL, p.Index)
					r.poolFilter.Remove(key)
				case FlagDelete:
					key := fmt.Sprintf("%s:%d", p.URL, p.Index)
					r.poolFilter.Remove(key)
				default:
					log.Printf("未支持的Flag: %d", ph.flag)
					continue
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
		Queue:          queue,
		Bucket:         bucket,
		Interval:       0,
		Retry:          3,
		providers:      nil,
		parallels:      parallels,
		proxyParallels: 1,
	}
	// 可选参数
	if opt.providers != nil {
		reactor.providers = opt.providers
	}
	if opt.interval != nil {
		reactor.Interval = *opt.interval
	}
	if opt.downloadRetry != nil {
		reactor.Retry = *opt.downloadRetry
	}
	if opt.proxyParallels != nil {
		reactor.proxyParallels = *opt.proxyParallels
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
