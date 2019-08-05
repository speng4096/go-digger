package storage

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

var bucket Bucket

func mustTruncateBucket(bucket Bucket) {
	if err := bucket.Truncate(); err != nil {
		panic(err)
	}
}

func testBucketSetGet(t *testing.T, bucket Bucket) {
	mustTruncateBucket(bucket)
	// Get不存在的Key时，返回ErrNotExist
	for i := 0; i < 1000; i++ {
		_, err := bucket.Get(fmt.Sprintf("Key-%d", i))
		assert.Equal(t, ErrNotExist, err)
	}
	// 写入Key-Value数据
	for i := 0; i < 1000; i++ {
		err := bucket.Set(fmt.Sprintf("Key-%d", i), fmt.Sprintf("Value-%d-1", i))
		assert.Nil(t, err)
	}
	// 验证Get
	for i := 0; i < 1000; i++ {
		value, err := bucket.Get(fmt.Sprintf("Key-%d", i))
		assert.Nil(t, err)
		assert.Equal(t, value, fmt.Sprintf("Value-%d-1", i))
	}
	// 覆盖Value
	for i := 0; i < 1000; i++ {
		err := bucket.Set(fmt.Sprintf("Key-%d", i), fmt.Sprintf("Value-%d-2", i))
		assert.Nil(t, err)
	}
	// 验证Get
	for i := 0; i < 1000; i++ {
		value, err := bucket.Get(fmt.Sprintf("Key-%d", i))
		assert.Nil(t, err)
		assert.Equal(t, value, fmt.Sprintf("Value-%d-2", i))
	}
}

func testBucketDelete(t *testing.T, bucket Bucket) {
	mustTruncateBucket(bucket)
	// 删除不存在的Key，返回ErrNotExist
	for i := 0; i < 1000; i++ {
		err := bucket.Delete(fmt.Sprintf("Key-%d", i))
		assert.Equal(t, err, ErrNotExist)
	}
	// 写入Key-Value数据
	for i := 0; i < 1000; i++ {
		err := bucket.Set(fmt.Sprintf("Key-%d", i), fmt.Sprintf("Value-%d-1", i))
		assert.Nil(t, err)
	}
	// 删除
	for i := 0; i < 1000; i++ {
		err := bucket.Delete(fmt.Sprintf("Key-%d", i))
		assert.Nil(t, err)
	}
	// 删除不存在的Key，返回ErrNotExist
	for i := 0; i < 1000; i++ {
		err := bucket.Delete(fmt.Sprintf("Key-%d", i))
		assert.Equal(t, err, ErrNotExist)
	}
}

func testBucketKeys(t *testing.T, bucket Bucket) {
	mustTruncateBucket(bucket)
	// 空Bucket
	keys, err := bucket.Keys()
	assert.Nil(t, err)
	assert.Equal(t, keys, []string{})
	// 写入Key-Value数据并验证
	expectKeys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("Key-%d", i)
		err := bucket.Set(key, fmt.Sprintf("Value-%d-1", i))
		assert.Nil(t, err)
		expectKeys[i] = key
	}
	keys, err = bucket.Keys()
	assert.Nil(t, err)
	sort.Strings(keys)
	sort.Strings(expectKeys)
	assert.Equal(t, keys, expectKeys)
}

func TestMyBucket_Set_Get(t *testing.T) {
	testBucketSetGet(t, bucket)
}

func TestMyBucket_Delete(t *testing.T) {
	testBucketDelete(t, bucket)
}

func TestMyBucket_Keys(t *testing.T) {
	testBucketKeys(t, bucket)
}

func init() {
	var err error
	bucket, err = NewMyBucket(testDsn, "bucket")
	if err != nil {
		panic(err)
	}
}
