package proxy

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"log"
	"regexp"
	"strings"
	"time"
)

func splitAddress(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return r == '\n' || r == '\r' || r == ' '
	})
}

// 通用代理
// 兼容返回格式为"ip:port"，并以空格、换行符分隔的API接口
func NewSimpleAPIProvider(url string, protocol string, interval time.Duration, enableFilter bool) Provider {
	c := resty.New().R()
	ch := make(chan Item, 0)
	reIPPort := regexp.MustCompile(
		`^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]):[0-9]+$`,
	)
	go func() {
		for ; ; time.Sleep(interval) {
			log.Printf("正在请求代理接口")
			resp, err := c.Get(url)
			if err != nil {
				log.Printf("请求代理接口失败: %s", err)
				continue
			}
			if resp.StatusCode() != 200 {
				log.Printf("代理接口返回异常状态码: %d, %s", resp.StatusCode(), resp.String())
				continue
			}
			addresses := splitAddress(resp.String())
			if len(addresses) == 0 {
				continue
			}
			for i, address := range addresses {
				address = strings.TrimSpace(address)
				if reIPPort.MatchString(addresses[0]) {
					s := protocol + "://" + address
					ch <- Item{s, enableFilter}
					log.Printf("从接口获得代理: %s", s)
				} else if i == 0 {
					log.Printf("代理接口返回异常数据: %s", resp.String())
					break
				} else {
					log.Printf("代理格式错误: %s", address)
				}
			}
		}
	}()
	return ch
}

// 蘑菇API代理
// count: 单次提取数量（个）
// interval: API调用频率
func NewMoguAPIProvider(url string, interval time.Duration) Provider {
	return NewSimpleAPIProvider(url, "http", interval, true)
}

// 蘑菇隧道代理
func NewMoguHTTPTunnelProvider(username, password string) Provider {
	ch := make(chan Item, 1)
	const server = "secondtransfer.moguproxy.com:9001"
	go func() {
		for {
			ch <- Item{fmt.Sprintf("http://%s:%s@%s", username, password, server), false}
		}
	}()
	return ch
}
