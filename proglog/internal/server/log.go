package server

import (
	"fmt"
	"sync"
)

var ErrOffsetNotFound = fmt.Errorf("offset not found")

type Log struct {
	mu       sync.Mutex
	recoreds []Record
}

func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.recoreds)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.recoreds[offset], nil
}

func NewLog() *Log {
	return &Log{}
}

func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.recoreds))
	c.recoreds = append(c.recoreds, record)
	return record.Offset, nil
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}
