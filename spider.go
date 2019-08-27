package digger

import "github.com/go-resty/resty/v2"

type Spider struct {
	Seeders   []string                                                                           // 初始URL
	OnInit    func(reactor *Reactor) error                                                       // 首次运行时调用
	OnProcess func(url string, client *resty.Client, proxy *ProxyHelper, reactor *Reactor) error // 从队列获取到URL时调用
}
