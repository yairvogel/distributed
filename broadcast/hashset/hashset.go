package hashset

import (
	"sync"
)

type unit struct{}

// thread-safe hashset, implemented over map and locking
type Hashset[I comparable] struct {
	innerMap map[I]unit
	mutex    sync.Mutex
}

func New[I comparable]() Hashset[I] {
	return Hashset[I]{innerMap: make(map[I]unit)}
}

func (h *Hashset[I]) Add(item I) {
	h.mutex.Lock()
	h.innerMap[item] = unit{}
	h.mutex.Unlock()
}

func (h *Hashset[I]) Keys() []I {
	keys := make([]I, len(h.innerMap))

	h.mutex.Lock()

	i := 0
	for k := range h.innerMap {
		keys[i] = k
		i++
	}

	h.mutex.Unlock()
	return keys
}

func (h *Hashset[I]) TryAdd(item I) bool {
	h.mutex.Lock()

	defer h.mutex.Unlock()
	_, hasKey := h.innerMap[item]
	if hasKey {
		return false
	}

	h.innerMap[item] = unit{}
	return true
}
