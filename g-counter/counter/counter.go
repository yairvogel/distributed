package counter

import (
	"fmt"
	"sync"
)

// an eventually consitent Pn-Counter CRDT
type Counter struct {
	nodeId int
	// counts holds increments on index 2*n and decrements on 2*n+1
	counts []uint
	mu     sync.RWMutex
}

func Init(nodes int, nodeId int) *Counter {
	return &Counter{
		counts: make([]uint, nodes*2),
		nodeId: nodeId,
	}
}

func (c *Counter) Add(d int) {
	if d > 0 {
		c.incr(uint(d))
	}

	if d < 0 {
		c.decr(uint(-1 * d))
	}
}

func (c *Counter) incr(d uint) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counts[c.nodeId*2] += d
}

func (c *Counter) decr(d uint) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counts[(c.nodeId*2)+1] += d
}

func (c *Counter) Read() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return alternatingSum(c.counts)
}

func alternatingSum(s []uint) int {
	var (
		res  int = 0
		sign int = 1
	)
	for _, v := range s {
		res += int(v) * sign
		sign *= -1
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
