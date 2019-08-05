package storage

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
	"time"
)

const n = 4 // 填充25%数据到CuckooFilter中
const capacity = 1 << 22

var cuckoo *CuckooFilter

func mustTruncateFilter(filter Filter) {
	if err := filter.Truncate(); err != nil {
		panic(err)
	}
}

func testFilterInsert(t *testing.T, filter Filter, capacity int) {
	mustTruncateFilter(filter)
	var nNil = 0
	for i := 0; i < capacity/n; i++ {
		if err := filter.Insert(fmt.Sprintf("%d", i)); err == nil {
			nNil++
		}
	}
	k := float64(nNil) / float64(capacity/n)
	t.Logf("插入成功率: %.6f", k)
	assert.Greater(t, k, 0.995) // 填充50%时，正确率需99%
}

func testFilterLookup(t *testing.T, filter Filter, capacity int) {
	mustTruncateFilter(filter)
	// 未插入时，检索不到测试数据
	for i := 0; i < capacity/n; i++ {
		ok, err := filter.Lookup(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.False(t, ok)
	}
	// 插入测试数据，记录插入成功的数据
	okUrls := make([]string, 0)
	errUrls := make([]string, 0)
	for i := 0; i < capacity/n; i++ {
		url := fmt.Sprintf("%d", i)
		if err := filter.Insert(url); err != nil {
			errUrls = append(errUrls, url)
		} else {
			okUrls = append(okUrls, url)
		}
	}
	// 插入后，能检索到插入成功的数据
	// 若失败，可能是ErrFull导致的，fp多次换位仍未找到空位导致丢失
	for _, url := range okUrls {
		ok, err := filter.Lookup(url)
		assert.Nil(t, err)
		assert.True(t, ok)
	}
	// 插入后，检索到未插入的数据的概率很低
	e := 0
	a := 0
	for i := capacity / n; i < capacity*2; i++ {
		a++
		ok, err := filter.Lookup(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		if !ok {
			e++
		}
	}
	t.Logf("检索未插入数据的失败率: %.6f", float64(e)/float64(a))
	assert.Greater(t, float64(e)/float64(a), 0.99)
}

func testFilterDeleteAndCount(t *testing.T, filter Filter, capacity int) {
	mustTruncateFilter(filter)
	count, err := filter.Count()
	assert.Nil(t, err)
	assert.EqualValues(t, count, 0)
	// 插入测试数据
	okUrls := make([]string, 0)
	errUrls := make([]string, 0)
	for i := 0; i < capacity/n; i++ {
		url := fmt.Sprintf("%d", i)
		if err := filter.Insert(url); err != nil {
			errUrls = append(errUrls, url)
		} else {
			okUrls = append(okUrls, url)
		}
		count, err = filter.Count()
		assert.Nil(t, err)
		assert.EqualValues(t, count, len(okUrls))
	}
	// 能删除插入成功的数据
	for i, url := range okUrls {
		assert.Nil(t, filter.Delete(url))
		count, err = filter.Count()
		assert.Nil(t, err)
		assert.EqualValues(t, count, len(okUrls)-i-1)
	}
	// 删除不存在数据时报错
	a := 0
	e := 0
	for i := capacity / n; i < capacity*10; i++ {
		a++
		if err := filter.Delete(fmt.Sprintf("%d", i)); err != nil {
			e++
		}
	}
	t.Logf("删除未插入数据的失败率: %.8f", float64(e)/float64(a))
	assert.Greater(t, float64(e)/float64(a), 0.99)
}

func TestCuckooFilter_Insert(t *testing.T) {
	testFilterInsert(t, cuckoo, capacity)
}

func TestCuckooFilter_Lookup(t *testing.T) {
	testFilterLookup(t, cuckoo, capacity)
}

func TestCuckooFilter_Delete_Count(t *testing.T) {
	testFilterDeleteAndCount(t, cuckoo, capacity)
}

func init() {
	var err error
	tmpFile := path.Join(os.TempDir(), fmt.Sprintf("cuckoo-%d.mmap", time.Now().UnixNano()))
	cuckoo, err = NewCuckooFilter(tmpFile, capacity)
	if err != nil {
		panic(err)
	}
}
