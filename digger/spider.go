package digger

import (
	"github.com/spencer404/go-digger/digger/storage"
	"net/http"
)

type Spider struct {
	Seeders   []string
	Queue     storage.Queue
	Bucket    storage.Bucket
	OnInit    func() error
	OnProcess func(url string, transport *http.Transport) error
}
