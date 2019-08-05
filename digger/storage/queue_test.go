package storage

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"
)

const queueTimeout = time.Second * 5

var q Queue

func mustTruncateQueue(t *testing.T, q Queue) {
	if err := q.Truncate(); !assert.Nil(t, err) {
		panic("清理数据库失败")
	}
}

func testQueueCollect(t *testing.T, q Queue, timeout time.Duration) {
	mustTruncateQueue(t, q)
	ok, err := q.Add("p0i0", Priority0)
	assert.Nil(t, err)
	assert.True(t, ok)
	// 不应有过期的
	time.Sleep(timeout / 2)
	items, err := q.Collect()
	assert.Nil(t, err)
	assert.Equal(t, []QueueItem{}, items)
	// 再加一个
	ok, err = q.Add("p1i0", Priority1)
	assert.True(t, ok)
	assert.Nil(t, err)
	// 不应有过期的
	time.Sleep(timeout)
	items, err = q.Collect()
	assert.Nil(t, err)
	assert.Equal(t, []QueueItem{}, items)
	// 弹出p0i0
	_, err = q.Pop()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(items))
	// 不应有过期的
	items, err = q.Collect()
	assert.Nil(t, err)
	assert.Equal(t, []QueueItem{}, items)
	// 过期
	time.Sleep(timeout + time.Second)
	items, err = q.Collect()
	assert.Nil(t, err)
	assert.EqualValues(t, 1, len(items))
	assert.EqualValues(t, 1, items[0].ID)
	assert.EqualValues(t, "p0i0", items[0].URL)
	assert.EqualValues(t, StateProcessing, items[0].State)
	assert.EqualValues(t, Priority0, items[0].Priority)
}

func testQueueLength(t *testing.T, q Queue) {
	mustTruncateQueue(t, q)
	const n = 3
	for p := Priority0; p < Priority4; p++ {
		for k := 0; k < n; k++ {
			ok, err := q.Add(fmt.Sprintf("p%di%d", p, k), p)
			assert.Nil(t, err)
			assert.True(t, ok)
			waiting, err := q.Length(StateWaiting)
			assert.Nil(t, err)
			processing, err := q.Length(StateProcessing)
			assert.Nil(t, err)
			for pp := Priority0; pp < Priority4; pp++ {
				if pp < p {
					assert.Equal(t, n, waiting[pp])
				} else if pp == p {
					assert.Equal(t, k+1, waiting[pp])
				} else {
					assert.Equal(t, 0, waiting[pp])
				}
			}
			assert.Equal(t, 0, processing[p])
		}
	}
}

func testQueueAdd(t *testing.T, q Queue) {
	mustTruncateQueue(t, q)
	for p := Priority0; p < Priority4; p++ {
		for k := 0; k < 3; k++ {
			ok, err := q.Add(fmt.Sprintf("p%di%d", p, k), p)
			assert.Nil(t, err)
			assert.True(t, ok)
		}
	}
}

func testQueuePop(t *testing.T, q Queue) {
	mustTruncateQueue(t, q)
	// 随机顺序添加测试数据
	items := make([]QueueItem, 0)
	for p := Priority0; p < Priority4; p++ {
		for k := 0; k < 3; k++ {
			items = append(items, QueueItem{Priority: p, URL: fmt.Sprintf("p%di%d", p, k)})
		}
	}
	rand.Seed(time.Now().UTC().UnixNano())
	rand.Shuffle(len(items), func(i, j int) {
		items[i], items[j] = items[j], items[i]
	})
	for _, item := range items {
		ok, err := q.Add(item.URL, item.Priority)
		assert.Nil(t, err)
		assert.True(t, ok)
	}
	// 验证是否顺序弹出
	for p := Priority0; p < Priority4; p++ {
		lastID := int64(0)
		for k := 0; k < 3; k++ {
			item, err := q.Pop()
			assert.Nil(t, nil, err)
			assert.Equal(t, p, item.Priority)
			assert.Greater(t, item.ID, lastID) // 优先级相同时，按ID升序
			lastID = item.ID
		}
	}
	// 验证空队列弹出时返回io.EOF错误
	_, err := q.Pop()
	assert.Equal(t, io.EOF, err)
}

func testQueueLookupAndFinish(t *testing.T, q Queue) {
	mustTruncateQueue(t, q)
	// 测试url不存在
	state, err := q.Lookup("p0i0")
	assert.Nil(t, err)
	assert.Equal(t, state, StateNotExist)
	// 添加测试url
	ok, err := q.Add("p0i0", Priority0)
	assert.Nil(t, err)
	assert.True(t, ok)
	// 测试url处于等待状态
	state, err = q.Lookup("p0i0")
	assert.Nil(t, err)
	assert.Equal(t, state, StateWaiting)
	// 完成测试url
	ok, err = q.Finish("p0i0")
	assert.Nil(t, err)
	assert.True(t, ok)
	state, err = q.Lookup("p0i0")
	assert.Nil(t, err)
	assert.Equal(t, state, StateNotExist)
	// 完成不存在的url
	ok, err = q.Finish("p1i0")
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestMySQLQueue_Add(t *testing.T) {
	testQueueAdd(t, q)
}

func TestMySQLQueue_Pop(t *testing.T) {
	testQueuePop(t, q)
}

func TestMySQLQueue_Length(t *testing.T) {
	testQueueLength(t, q)
}

func TestMySQLQueue_Collect(t *testing.T) {
	testQueueCollect(t, q, queueTimeout)
}

func TestMySQLQueue_Lookup_Finish(t *testing.T) {
	testQueueLookupAndFinish(t, q)
}

func init() {
	tmpFile := path.Join(os.TempDir(), fmt.Sprintf("cuckoo-%d.mmap", time.Now().UnixNano()))
	filter, err := NewCuckooFilter(tmpFile, capacity)
	if err != nil {
		panic(err)
	}
	q, err = NewMyQueue(testDsn, "queue", filter, queueTimeout)
	if err != nil {
		panic(err)
	}
}
