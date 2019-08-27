package digger

import "time"

type flag int

const (
	FlagUnset = iota
	FlagPutBack
	FlagForbidden
	FlagDelete
	FlagFreeze
)

// 代理
type Proxy struct {
	URL         string
	Index       int       // 序号，Proxy会被添加parallels次到poolCh中，用于区分
	CreateTime  time.Time // 创建时间
	ExpiredTime time.Time // 过期时间
}

type ProxyHelper struct {
	flag  flag
	flagD time.Duration // Flag的参数
}

// 禁用当前代理一段时间，到期后解禁
func (p *ProxyHelper) Forbidden(d time.Duration) {
	p.flag = FlagForbidden
	p.flagD = d
}

// 删除当前代理
func (p *ProxyHelper) Delete() {
	p.flag = FlagDelete
}

// 冻结当前代理一段时间，到期自动放回代理池（未过期、未在黑名单中）
func (p *ProxyHelper) Freeze(d time.Duration) {
	p.flag = FlagFreeze
	p.flagD = d
}

// 冻结列表
type freezeItem struct {
	t  time.Time // 解冻时间
	ps []*Proxy  // 被冻结的代理
}

type freezeList map[string]*freezeItem
