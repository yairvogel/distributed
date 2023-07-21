package counter

import (
	"fmt"
	"sync"
)

type Counter struct {
	nodeId int
	counts []uint
	mu     sync.RWMutex
}

func Init(nodes int, nodeId int) *Counter {
	return &Counter{counts: make([]uint, nodes), nodeId: nodeId}
}

func (c *Counter) Incr(d uint) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counts[c.nodeId] += d
}

func (c *Counter) Read() uint {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return sum(c.counts)
}

func sum(s []uint) uint {
	var res uint = 0
	for _, v := range s {
		res += v
	}

	return res
}

func (c *Counter) Merge(other []uint) error {
	if len(other) != len(c.counts) {
		return fmt.Errorf("merging incompatible length counters. ours: %v, theirs: %v", len(c.counts), len(other))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for i, v := range other {
		if v > c.counts[i] {
			c.counts[i] = v
		}
	}

	return nil
}

func (c *Counter) State() []uint {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dst := make([]uint, len(c.counts))
	copy(dst, c.counts)
	return dst
}
