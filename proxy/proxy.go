package proxy

type Item struct {
	URL          string
	EnableFilter bool
}

type Provider <-chan Item
