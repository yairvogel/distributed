package hashset

type unit struct{}

type Hashset[I comparable] map[I]unit

func (h Hashset[I]) Add(item I) {
	h[item] = unit{}
}

func (h Hashset[I]) Keys() []I {
	keys := make([]I, len(h))

	i := 0
	for k := range h {
		keys[i] = k
		i++
	}

	return keys
}

func (h Hashset[I]) TryAdd(item I) bool {
	_, hasKey := h[item]
	if hasKey {
		return false
	}

	h[item] = unit{}
	return true
}
