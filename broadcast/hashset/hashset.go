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

func New[I comparable]() *Hashset[I] {
	return &Hashset[I]{innerMap: make(map[I]unit)}
}

func (h *Hashset[I]) Add(item I) {
	h.mutex.Lock()
	h.innerMap[item] = unit{}
	h.mutex.Unlock()
}

func (h *Hashset[I]) Items() []I {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	keys := make([]I, len(h.innerMap))
	i := 0
	for k := range h.innerMap {
		keys[i] = k
		i++
	}

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

// in-place union
func (h *Hashset[I]) Union(other []I) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for _, i := range other {
		h.innerMap[i] = unit{}
	}
}

// immutable difference: returns a copy
func (h *Hashset[I]) Difference(other []I) []I {
	output := make([]I, 0)

	h.mutex.Lock()
	defer h.mutex.Unlock()

	for i, _ := range h.innerMap {
		hasKey := contains(other, i)
		if !hasKey {
			output = append(output, i)
		}
	}

	return output
}

func contains[I comparable](s []I, i I) bool {
	for _, v := range s {
		if v == i {
			return true
		}
	}
	return false
}
