package digger

import (
	"github.com/spencer404/go-digger/storage"
)

type Spider struct {
	Seeders   []string       // 初始URL
	Queue     storage.Queue  // 队列
	Bucket    storage.Bucket //
	OnInit    func() error
	OnProcess func(url string, reactor *Reactor) error
}
