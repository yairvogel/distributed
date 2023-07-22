package state

type txn = []Mop

type key = int
type vals = []int
type State struct {
	values map[key]vals
}

func NewState() *State {
	return &State{make(map[key]vals)}
}

func (s *State) Transact(t txn) txn {
	var t2 txn = make(txn, len(t))
	for i, m := range t {
		switch m.op {
		case "r":
			m = Mop{op: m.op, key: m.key, val: m.val, vals: m.vals}
			m.vals = s.getOrDefault(m.key)
		case "append":
			vals := append(s.getOrDefault(m.key), m.val)
			s.values[m.key] = vals
		}

		t2[i] = m
	}

	return t2
}

func (s State) getOrDefault(k key) vals {
	if val, hasKey := s.values[k]; hasKey {
		return val
	}

	return vals{}
}
