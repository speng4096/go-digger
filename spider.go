package digger

import (
	"github.com/spencer404/go-digger/storage"
)

type Spider struct {
	Seeders   []string
	Queue     storage.Queue
	Bucket    storage.Bucket
	OnInit    func() error
	OnProcess func(url string, reactor *Reactor) error
}