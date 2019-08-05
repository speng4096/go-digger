package internal

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

func TableExist(db *sqlx.DB, tableName string) (bool, error) {
	tables := make([]string, 0)
	if err := db.Select(&tables, fmt.Sprintf("SHOW TABLES LIKE '%s'", tableName)); err != nil {
		return false, errors.Wrap(err, "查询表名失败")
	}
	return len(tables) != 0, nil
}

func Escape(source string) string {
	var j = 0
	if len(source) == 0 {
		return ""
	}
	tempStr := source[:]
	desc := make([]byte, len(tempStr)*2)
	for i := 0; i < len(tempStr); i++ {
		flag := false
		var escape byte
		switch tempStr[i] {
		case '\r':
			flag = true
			escape = '\r'
			break
		case '\n':
			flag = true
			escape = '\n'
			break
		case '\\':
			flag = true
			escape = '\\'
			break
		case '\'':
			flag = true
			escape = '\''
			break
		case '"':
			flag = true
			escape = '"'
			break
		case '\032':
			flag = true
			escape = 'Z'
			break
		default:
		}
		if flag {
			desc[j] = '\\'
			desc[j+1] = escape
			j = j + 2
		} else {
			desc[j] = tempStr[i]
			j = j + 1
		}
	}
	return string(desc[0:j])
}

func SQLf(sql string, args ...string) string {
	s := make([]interface{}, len(args))
	for i, a := range args {
		s[i] = Escape(a)
	}
	return fmt.Sprintf(sql, s...)
}
