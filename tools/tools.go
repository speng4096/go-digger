package tools

import (
	"github.com/antonholmquist/jason"
	"github.com/pkg/errors"
	"strings"
)

// 取出字符串中间部分
func Centre(s, left, right string) string {
	a := strings.Index(s, left)
	b := strings.LastIndex(s, right)
	if a >= b {
		return ""
	}
	return s[a : b+1]
}

// 取出jsonp中的json部分
func ParseJSONp(body string) (*jason.Object, error) {
	s := Centre(body, "{", "}")
	obj, err := jason.NewObjectFromReader(strings.NewReader(s))
	if err != nil {
		return nil, errors.Wrapf(err, "提取JSON失败\n--Body: %s", body)
	}
	return obj, nil
}
