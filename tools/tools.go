package tools

import "strings"

// 取出字符串中间部分
func Centre(s, left, right string) string {
	a := strings.Index(s, left)
	b := strings.LastIndex(s, right)
	if a >= b {
		return ""
	}
	return s[a : b+1]
}
