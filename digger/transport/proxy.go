package transport

import (
	"net/http"
	"net/url"
)

type Maker func() *http.Transport

func DirectTransport() Maker {
	return func() *http.Transport {
		return &http.Transport{}
	}
}

func StaticTransport(proxyURL string) Maker {
	return func() *http.Transport {
		return &http.Transport{Proxy: func(_ *http.Request) (*url.URL, error) {
			return url.Parse(proxyURL)
		}}
	}
}
