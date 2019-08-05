package storage

import (
	"github.com/pkg/errors"
	"github.com/spencer404/go-cuckoofilter"
)

type CuckooFilter struct {
	f *cuckoofilter.CuckooFilter
}

func (c *CuckooFilter) Insert(url string) error {
	return c.f.InsertUnique([]byte(url))
}

func (c *CuckooFilter) Delete(url string) error {
	return c.f.Delete([]byte(url))
}

func (c *CuckooFilter) Lookup(url string) (bool, error) {
	return c.f.Lookup([]byte(url))
}

func (c *CuckooFilter) Count() (uint, error) {
	return c.f.Count(), nil
}

func (c *CuckooFilter) Truncate() error {
	return c.f.Truncate()
}

func MustNewCuckooFilter(filename string, capacity uint) *CuckooFilter {
	cuckoo, err := NewCuckooFilter(filename, capacity)
	if err != nil {
		panic(err)
	}
	return cuckoo
}

func NewCuckooFilter(filename string, capacity uint) (*CuckooFilter, error) {
	table, err := cuckoofilter.NewMMAPTable(filename, capacity)
	if err != nil {
		return nil, errors.Wrap(err, "新建MMAPTable失败")
	}
	return &CuckooFilter{cuckoofilter.NewCuckooFilter(table)}, nil
}
