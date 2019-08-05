package storage

import (
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/spencer404/go-digger/digger/internal"
	"io"
	"strings"
	"time"
)

const createQueueSQL = "CREATE TABLE IF NOT EXISTS `%s` (" +
	"`id`       BIGINT       NOT NULL AUTO_INCREMENT," +
	"`url`      VARCHAR(500) NOT NULL," +
	"`state`    TINYINT      NOT NULL COMMENT '0: 等待中; 1: 进行中;'," +
	"`priority` TINYINT      NOT NULL COMMENT '优先级'," +
	"`created`  TIMESTAMP    NOT NULL DEFAULT now()," +
	"`updated`  TIMESTAMP    NOT NULL DEFAULT now() ON UPDATE now()," +
	"PRIMARY KEY (`id`)," +
	"INDEX `index_priority` (`priority` ASC)," +
	"INDEX `index_state` (`state` ASC)," +
	"CONSTRAINT unique_url UNIQUE (`url`))" +
	"ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4"

type MyQueue struct {
	db        *sqlx.DB
	tableName string
	timeout   time.Duration
	filter    Filter
}

func (m *MyQueue) AddDirect(url string, priority Priority) (bool, error) {
	sql := internal.SQLf(`INSERT INTO %s (url, state, priority) VALUE (?, ?, ?)`, m.tableName)
	_, err := m.db.Exec(sql, url, StateWaiting, priority)
	if err == nil {
		return true, nil
	} else if err.(*mysql.MySQLError).Number == 1062 {
		return false, nil // 队列中重复
	} else {
		return false, errors.Wrap(err, "添加URL失败")
	}
}

func (m *MyQueue) Add(url string, priority Priority) (bool, error) {
	exist, err := m.filter.Lookup(url)
	if err != nil {
		return false, errors.Wrapf(err, "添加URL时在Filter中查询失败")
	}
	if exist {
		return false, nil
	}
	return m.AddDirect(url, priority)
}

func (m *MyQueue) Pop() (item QueueItem, err error) {
	tx, err := m.db.Beginx()
	defer tx.Rollback()
	if err != nil {
		return QueueItem{}, errors.Wrap(err, "事务未能开始")
	}
	// 查询出最优先行
	items := make([]QueueItem, 0)
	sql := internal.SQLf("SELECT * FROM %s WHERE state=? ORDER BY priority, id LIMIT 1 FOR UPDATE", m.tableName)
	if err := tx.Select(&items, sql, StateWaiting); err != nil {
		return QueueItem{}, errors.Wrap(err, "查询URL失败")
	}
	if len(items) == 0 {
		return QueueItem{}, io.EOF
	}
	item = items[0]
	// 更新状态
	sql = internal.SQLf("UPDATE %s SET state=? WHERE id=? LIMIT 1", m.tableName)
	if _, err := tx.Exec(sql, StateProcessing, item.ID); err != nil {
		return QueueItem{}, errors.Wrap(err, "更新URL状态失败")
	}
	if err := tx.Commit(); err != nil {
		return QueueItem{}, errors.Wrap(err, "提交事务失败")
	}
	return item, nil
}

func (m *MyQueue) Length(state State) (map[Priority]int, error) {
	dest := make([]QueueItem, 0)
	sql := internal.SQLf(`SELECT count(1) AS count, priority FROM %s WHERE state=? GROUP BY priority`, m.tableName)
	err := m.db.Select(&dest, sql, state)
	if err != nil {
		return nil, errors.Wrap(err, "查询长度信息失败")
	}
	res := make(map[Priority]int)
	for _, url := range dest {
		res[url.Priority] = url.Count
	}
	return res, nil
}

func (m *MyQueue) Truncate() error {
	sql := internal.SQLf("TRUNCATE TABLE %s", m.tableName)
	if _, err := m.db.Exec(sql); err != nil {
		return errors.Wrapf(err, "清空%q表失败", m.tableName)
	}
	if err := m.filter.Truncate(); err != nil {
		return errors.Wrapf(err, "清空过滤器失败")
	}
	return nil
}

// 清理超时URL的Goroutine
func (m *MyQueue) Collect() ([]QueueItem, error) {
	tx, err := m.db.Beginx()
	defer tx.Rollback()
	if err != nil {
		return nil, errors.Wrap(err, "事务未能开始")
	}
	// 寻找过期数据
	items := make([]QueueItem, 0)
	sql := internal.SQLf(
		"SELECT * FROM %s WHERE state=? AND timestampdiff(MICROSECOND, updated, now()) > ? FOR UPDATE", m.tableName)
	interval := m.timeout.Nanoseconds() / time.Microsecond.Nanoseconds()
	if err := tx.Select(&items, sql, StateProcessing, interval); err != nil {
		return nil, errors.Wrap(err, "查询过期数据失败")
	}
	// 分批删除过期数据
	const pageSize = 512
	for i := 0; i < (len(items)+pageSize-1)/pageSize; i++ {
		args := make([]interface{}, 0)
		marks := make([]string, 0)
		min := i * pageSize
		max := (i + 1) * pageSize
		if max > len(items) {
			max = len(items)
		}
		for _, item := range items[min:max] {
			args = append(args, item.ID)
			marks = append(marks, "?")
		}
		sql := internal.SQLf("DELETE FROM %s WHERE id IN (%s)", m.tableName, strings.Join(marks, ","))
		if _, err := tx.Exec(sql, args...); err != nil {
			return nil, errors.Wrap(err, "删除过期数据失败")
		}
	}
	// 完成
	if err := tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "提交事务失败")
	}

	return items, nil
}

func (m *MyQueue) Finish(url string) (bool, error) {
	// 添加到过滤器
	if err := m.filter.Insert(url); err != nil {
		return false, errors.Wrap(err, "添加到Filter失败")
	}
	// 从队列中删除
	sql := internal.SQLf("DELETE FROM %s WHERE url=?", m.tableName)
	res, err := m.db.Exec(sql, url)
	if err != nil {
		return false, errors.Wrap(err, "更新数据失败")
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, errors.Wrap(err, "获取影响行数失败")
	}
	return n == 1, nil
}

func (m *MyQueue) Lookup(url string) (State, error) {
	items := make([]QueueItem, 0)
	sql := internal.SQLf("SELECT state FROM %s WHERE url=? LIMIT 1", m.tableName)
	if err := m.db.Select(&items, sql, url); err != nil {
		return StateNotExist, errors.Wrap(err, "查询url状态失败")
	}
	if len(items) == 0 {
		return StateNotExist, nil
	}
	return items[0].State, nil
}

func MustNewMyQueue(dsn string, tableName string, filter Filter, timeout time.Duration) Queue {
	q, err := NewMyQueue(dsn, tableName, filter, timeout)
	if err != nil {
		panic(err)
	}
	return q
}

// TODO: 结构化DSN
func NewMyQueue(dsn string, tableName string, filter Filter, timeout time.Duration) (Queue, error) {
	// 连接MySQL
	dsn = dsn + "?parseTime=true"
	db, err := sqlx.Connect("mysql", dsn) // Connect会执行一次Ping
	if err != nil {
		return nil, errors.Wrap(err, "创建MyQueue失败")
	}
	// 创建Table
	if _, err = db.Exec(internal.SQLf(createQueueSQL, tableName)); err != nil {
		return nil, errors.Wrap(err, "创建MyQueue失败")
	}
	return &MyQueue{db: db, tableName: tableName, filter: filter, timeout: timeout}, nil
}
