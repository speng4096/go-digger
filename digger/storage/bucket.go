package storage

import (
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/spencer404/go-digger/digger/internal"
	"time"
)

const createBucketSQL = "CREATE TABLE IF NOT EXISTS `%s` (" +
	"`k`        VARCHAR(500) NOT NULL," +
	"`v`        VARCHAR(500) NOT NULL," +
	"`created`  TIMESTAMP    NOT NULL DEFAULT now()," +
	"`updated`  TIMESTAMP    NOT NULL DEFAULT now() ON UPDATE now()," +
	"PRIMARY KEY (`k`))" +
	"ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4"

type BucketItem struct {
	Key     string    `db:"k"`
	Value   string    `db:"v"`
	Created time.Time `db:"created"`
	Updated time.Time `db:"updated"`
	Count   int       `db:"count"` // 用于SQL查询统计数量
}

type MyBucket struct {
	db        *sqlx.DB
	tableName string
}

func (m *MyBucket) Set(key string, value string) error {
	sql := internal.SQLf("INSERT INTO %s (k, v) VALUE (?, ?) ON DUPLICATE KEY UPDATE v=?", m.tableName)
	if _, err := m.db.Exec(sql, key, value, value); err != nil {
		return errors.Wrapf(err, "写入记录%q失败", key)
	}
	return nil
}

func (m *MyBucket) Get(key string) (value string, err error) {
	items := make([]BucketItem, 0)
	sql := internal.SQLf("SELECT * FROM %s WHERE k=? LIMIT 1", m.tableName)
	if err := m.db.Select(&items, sql, key); err != nil {
		return "", errors.Wrapf(err, "读取记录%q失败", key)
	}
	if len(items) == 0 {
		return "", ErrNotExist
	}
	return items[0].Value, nil
}

func (m *MyBucket) Delete(key string) error {
	sql := internal.SQLf("DELETE FROM %s WHERE k=? LIMIT 1", m.tableName)
	res, err := m.db.Exec(sql, key)
	if err != nil {
		return errors.Wrapf(err, "删除记录%q失败", key)
	}
	if n, err := res.RowsAffected(); err != nil {
		return errors.Wrapf(err, "删除记录%q后获取影响行数失败", key)
	} else if n == 0 {
		return ErrNotExist
	}
	return nil
}

func (m *MyBucket) Keys() ([]string, error) {
	items := make([]BucketItem, 0)
	sql := internal.SQLf("SELECT k FROM %s", m.tableName)
	if err := m.db.Select(&items, sql); err != nil {
		return nil, errors.Wrapf(err, "查询Keys失败")
	}
	keys := make([]string, len(items))
	for i, item := range items {
		keys[i] = item.Key
	}
	return keys, nil
}

func (m *MyBucket) Truncate() error {
	sql := internal.SQLf("TRUNCATE TABLE %s", m.tableName)
	if _, err := m.db.Exec(sql); err != nil {
		return errors.Wrapf(err, "清空%q表失败", m.tableName)
	}
	return nil
}

func MustNewMyBucket(dsn string, tableName string) *MyBucket {
	bucket, err := NewMyBucket(dsn, tableName)
	if err != nil {
		panic(err)
	}
	return bucket
}

func NewMyBucket(dsn string, tableName string) (*MyBucket, error) {
	// 连接MySQL
	dsn = dsn + "?parseTime=true"
	db, err := sqlx.Connect("mysql", dsn) // Connect会执行一次Ping
	if err != nil {
		return nil, errors.Wrap(err, "创建MyBucket失败")
	}
	// 创建Table
	if _, err = db.Exec(internal.SQLf(createBucketSQL, tableName)); err != nil {
		return nil, errors.Wrap(err, "创建MyBucket失败")
	}
	return &MyBucket{db: db, tableName: tableName}, nil
}
